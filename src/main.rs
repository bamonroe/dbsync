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
}

#[derive(Subcommand)]
enum AuthAction {
    /// Run the OAuth2 PKCE flow and store a refresh token.
    Login,
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
        Command::Run => {
            let config = Config::load(&cli.config)?;
            dbsync::daemon::run(&config).await?;
            Ok(())
        }
        Command::Auth { action } => {
            let config = Config::load(&cli.config)?;
            let store = TokenStore::default_location()?;
            match action {
                AuthAction::Login => {
                    let credentials = auth::login(&config.app_key, &store).await?;
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
