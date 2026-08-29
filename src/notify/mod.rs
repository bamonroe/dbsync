//! Remote change notification: the long-poll loop.
//!
//! This is the module the project is named for. It parks on Dropbox's
//! `/2/files/list_folder/longpoll` endpoint, which blocks until something
//! changes under the cursor's folder, and emits a signal when it does.
//!
//! Two things make this endpoint unlike the rest of the API, and they are why
//! it gets its own module and its own HTTP client:
//!
//! - It lives on `notify.dropboxapi.com`, not `api.dropboxapi.com`.
//! - It takes **no `Authorization` header** — the cursor is the capability.
//!
//! A `changes: true` response carries no file data. The reconciler must follow
//! up with `/2/files/list_folder/continue` to learn what actually changed.

mod longpoll;

pub use longpoll::{LongpollClient, LongpollOutcome};
