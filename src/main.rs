//! The `dbsync` binary: argument parsing and dispatch.
//!
//! Everything substantial lives in the library modules; this file only decides
//! which one to hand control to.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use dbsync::Config;
use dbsync::auth::{self, TokenStore};

#[derive(Parser)]
#[command(name = "dbsync", version, about = "Realtime Dropbox sync daemon")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, default_value = "config.toml", global = true)]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the sync daemon in the foreground (the default).
    Run,
    /// Link or inspect the Dropbox account.
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
    /// Load the configuration and report what it resolved to, then exit.
    Check,
    /// List entries that could not be synced and are missing locally.
    Failures {
        /// Show only the ones a retry could still fix.
        #[arg(long, conflicts_with = "permanent")]
        retryable: bool,
        /// Show only the ones that will never succeed without intervention.
        #[arg(long)]
        permanent: bool,
    },
    /// Ask the daemon to try one or more paths again.
    ///
    /// The request is queued rather than performed here: the daemon owns the
    /// state file, so a second process transferring behind its back would have
    /// its work overwritten. It is picked up on the daemon's next pass, and
    /// waits on disk if the daemon is not running.
    Retry {
        /// Remote paths, as `dbsync failures` prints them.
        #[arg(required = true)]
        paths: Vec<String>,
        /// Re-send the local file instead of re-fetching the remote one.
        ///
        /// Say so explicitly: re-fetching a path whose upload failed would pull
        /// the remote copy over the local edit that never got sent.
        #[arg(long)]
        upload: bool,
    },
}

#[derive(Subcommand)]
enum AuthAction {
    /// Run the OAuth2 PKCE flow and store a refresh token.
    Login {
        /// Paste the code by hand instead of catching a browser redirect.
        ///
        /// Use this over SSH: the browser is on another machine and cannot
        /// reach a loopback listener here.
        #[arg(long)]
        paste_code: bool,
    },
    /// Show which account is currently linked.
    Status,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dbsync=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Run) {
        Command::Check => {
            let config = Config::load(&cli.config)?;
            println!("config:      {}", cli.config.display());
            println!("local_root:  {}", config.local_root.display());
            println!(
                "remote_root: {}",
                if config.remote_root.is_empty() {
                    "<app root>"
                } else {
                    &config.remote_root
                }
            );
            println!("longpoll:    {}s", config.longpoll.timeout_secs);
            println!("debounce:    {}ms", config.watcher.debounce_ms);
            Ok(())
        }
        Command::Failures {
            retryable,
            permanent,
        } => {
            let db = dbsync::state::StateDb::default_location()?;
            let state = db.load()?;
            print_failures(&state, retryable, permanent);
            Ok(())
        }
        Command::Retry { paths, upload } => {
            let db = dbsync::state::StateDb::default_location()?;
            let queue = dbsync::state::RetryQueue::beside(db.path());
            let direction = if upload {
                dbsync::state::Direction::Upload
            } else {
                dbsync::state::Direction::Download
            };
            for path in &paths {
                queue.push(&dbsync::state::RetryRequest {
                    display_path: path.clone(),
                    direction,
                })?;
                println!("queued {} retry: {path}", direction.label());
            }
            println!(
                "\n{} request{} queued at {}; the daemon picks them up on its next pass",
                paths.len(),
                if paths.len() == 1 { "" } else { "s" },
                queue.path().display()
            );
            Ok(())
        }
        Command::Run => {
            let config = Config::load(&cli.config)?;
            dbsync::daemon::run(&config).await?;
            Ok(())
        }
        Command::Auth { action } => {
            let config = Config::load(&cli.config)?;
            let store = TokenStore::default_location()?;
            match action {
                AuthAction::Login { paste_code } => {
                    let credentials = if paste_code {
                        auth::login_with_pasted_code(&config.app_key, &store).await?
                    } else {
                        auth::login(&config.app_key, &store).await?
                    };
                    println!(
                        "\nLinked{}. Credentials saved to {}.",
                        credentials
                            .account_id
                            .map(|id| format!(" account {id}"))
                            .unwrap_or_default(),
                        store.path().display()
                    );
                    Ok(())
                }
                AuthAction::Status => match store.load() {
                    Ok(credentials) => {
                        println!("linked:  yes");
                        println!(
                            "account: {}",
                            credentials.account_id.as_deref().unwrap_or("?")
                        );
                        println!("store:   {}", store.path().display());
                        Ok(())
                    }
                    Err(dbsync::Error::NotAuthenticated) => {
                        println!("linked:  no — run `dbsync auth login`");
                        println!("store:   {}", store.path().display());
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                },
            }
        }
    }
}

/// Print the recorded failures, newest trouble first within each kind.
///
/// Permanent entries are listed first and labelled: they are the ones that will
/// still be missing tomorrow unless someone acts, so burying them under a list
/// of things the next pull will fix by itself would defeat the point.
fn print_failures(state: &dbsync::state::SyncState, retryable: bool, permanent: bool) {
    use dbsync::state::FailureKind;

    let wanted = |kind: FailureKind| match (retryable, permanent) {
        (false, false) => true,
        (true, _) => kind == FailureKind::Transient,
        (_, true) => kind == FailureKind::Permanent,
    };

    let mut shown = 0;
    for kind in [FailureKind::Permanent, FailureKind::Transient] {
        if !wanted(kind) {
            continue;
        }
        for failure in state.failures().filter(|f| f.kind == kind) {
            let label = match kind {
                FailureKind::Permanent => "permanent",
                FailureKind::Transient => "retryable",
            };
            println!(
                "{label}  {}  attempts={}  {}\n           {}",
                failure.direction.label(),
                failure.attempts,
                failure.display_path,
                failure.error
            );
            shown += 1;
        }
    }

    if shown == 0 {
        println!("no recorded failures — everything the state knows about is on disk");
    } else {
        println!(
            "\n{shown} entr{} not in sync",
            if shown == 1 { "y" } else { "ies" }
        );
    }
}
