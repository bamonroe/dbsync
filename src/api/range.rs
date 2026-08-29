//! The byte range a content download asks for, and the check that the server
//! actually honoured it.
//!
//! Two shapes share one type on purpose. An **open-ended** range is a resume:
//! "everything from here to the end", and a server that ignores it and sends
//! the whole file is merely wasteful — the caller notices the 200 and starts
//! the partial over. A **bounded** range is one chunk of a parallel fetch, and
//! there the same 200 is corruption waiting to happen: the whole file would be
//! written into the slot reserved for one chunk. So a bounded range is
//! verified against the reply and an unhonoured one is an error, while an
//! open-ended one stays lenient.

use crate::error::{Error, Result};

/// A range of bytes to ask a content endpoint for.
///
/// `end` is *inclusive*, matching HTTP rather than Rust: `bytes=0-1023` is the
/// first 1024 bytes. `None` means "to the end of the file".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ByteRange {
    start: u64,
    end: Option<u64>,
}

impl ByteRange {
    /// Everything from `start` onwards — the resume shape.
    pub(super) fn from(start: u64) -> Self {
        Self { start, end: None }
    }

    /// Exactly `len` bytes beginning at `start` — one chunk of a parallel fetch.
    ///
    /// A zero-length chunk has no valid HTTP spelling, so it is not one:
    /// callers planning chunks must not emit empty ranges.
    pub(super) fn bounded(start: u64, len: u64) -> Self {
        debug_assert!(len > 0, "a byte range must ask for at least one byte");
        Self {
            start,
            end: Some(start + len.max(1) - 1),
        }
    }

    /// Is this range the whole file from byte zero, needing no header at all?
    pub(super) fn is_whole_file(self) -> bool {
        self.start == 0 && self.end.is_none()
    }

    /// The `Range` header value this asks for.
    pub(super) fn header_value(self) -> String {
        match self.end {
            Some(end) => format!("bytes={}-{end}", self.start),
            None => format!("bytes={}-", self.start),
        }
    }

    /// Confirm the reply is the range that was asked for.
    ///
    /// Only bounded ranges are held to this: see the module note.
    pub(super) fn verify(self, response: &reqwest::Response) -> Result<()> {
        let Some(end) = self.end else {
            return Ok(());
        };
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(self.unhonoured(format!(
                "answered {} rather than 206",
                response.status().as_u16()
            )));
        }
        let header = response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| self.unhonoured("no Content-Range in a 206".into()))?;
        match content_range(header) {
            Some((first, last)) if first == self.start && last == end => Ok(()),
            _ => Err(self.unhonoured(format!("Content-Range was {header:?}"))),
        }
    }

    fn unhonoured(self, detail: String) -> Error {
        Error::Api {
            status: 206,
            message: format!("asked for {} but {detail}", self.header_value()),
        }
    }
}

/// The first and last byte positions in a `Content-Range: bytes 0-1023/4096`.
///
/// The total after the `/` is deliberately ignored: it is the size of the
/// revision being served, which the caller already knows, and an unsatisfied
/// `*` form has no positions to compare anyway.
fn content_range(header: &str) -> Option<(u64, u64)> {
    let positions = header.trim().strip_prefix("bytes ")?;
    let positions = positions.split('/').next()?;
    let (first, last) = positions.split_once('-')?;
    Some((first.trim().parse().ok()?, last.trim().parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resume_asks_open_endedly() {
        assert_eq!(ByteRange::from(500).header_value(), "bytes=500-");
    }

    /// The end is inclusive, so a 1024-byte chunk ends at 1023 — an off-by-one
    /// here fetches an extra byte into every chunk and shifts the whole file.
    #[test]
    fn a_chunk_asks_for_an_inclusive_end() {
        assert_eq!(ByteRange::bounded(0, 1024).header_value(), "bytes=0-1023");
        assert_eq!(
            ByteRange::bounded(1024, 512).header_value(),
            "bytes=1024-1535"
        );
    }

    #[test]
    fn only_a_full_open_ended_range_is_the_whole_file() {
        assert!(ByteRange::from(0).is_whole_file());
        assert!(!ByteRange::from(1).is_whole_file());
        assert!(!ByteRange::bounded(0, 10).is_whole_file());
    }

    fn reply(status: u16, content_range: Option<&str>) -> reqwest::Response {
        let mut builder = http::Response::builder().status(status);
        if let Some(value) = content_range {
            builder = builder.header(reqwest::header::CONTENT_RANGE, value);
        }
        reqwest::Response::from(builder.body(String::new()).unwrap())
    }

    #[test]
    fn a_matching_206_satisfies_a_chunk() {
        assert!(
            ByteRange::bounded(0, 1024)
                .verify(&reply(206, Some("bytes 0-1023/4096")))
                .is_ok()
        );
    }

    /// The whole point of the check: a server that ignores the range answers
    /// 200 with the entire file, which would land on top of one chunk's slot.
    #[test]
    fn a_200_does_not_satisfy_a_chunk() {
        assert!(
            ByteRange::bounded(1024, 1024)
                .verify(&reply(200, None))
                .is_err()
        );
    }

    /// A 206 for the wrong bytes is the same corruption wearing the right
    /// status code.
    #[test]
    fn a_206_for_other_bytes_does_not_satisfy_a_chunk() {
        assert!(
            ByteRange::bounded(1024, 1024)
                .verify(&reply(206, Some("bytes 0-1023/4096")))
                .is_err()
        );
    }

    #[test]
    fn a_206_without_a_content_range_does_not_satisfy_a_chunk() {
        assert!(ByteRange::bounded(0, 10).verify(&reply(206, None)).is_err());
    }

    /// An open-ended resume stays lenient: download_to handles the 200 by
    /// truncating its partial, which is correct and not worth failing over.
    #[test]
    fn an_open_ended_range_accepts_a_whole_file_reply() {
        assert!(ByteRange::from(500).verify(&reply(200, None)).is_ok());
    }

    #[test]
    fn a_content_range_parses_into_its_positions() {
        assert_eq!(content_range("bytes 0-1023/4096"), Some((0, 1023)));
        assert_eq!(content_range("bytes 10-20/*"), Some((10, 20)));
        assert_eq!(content_range("bytes */4096"), None);
        assert_eq!(content_range("0-1023/4096"), None);
    }
}
