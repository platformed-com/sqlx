//! [sqlcommenter](https://google.github.io/sqlcommenter/) trailing-comment
//! generation for queries.
//!
//! When the `sqlcommenter` feature is enabled, backend executors call
//! [`maybe_append_comment`] to append a
//! `/*key1='value1',key2='value2'*/` comment to outgoing SQL so server-side
//! observability tools (Cloud SQL Insights, AlloyDB Insights, pg_stat_statements
//! consumers that preserve comments) can correlate query stats with the
//! upstream OTel trace.
//!
//! Implementation: we delegate the *what* to inject to OTel's globally
//! configured [`TextMapPropagator`](opentelemetry::propagation::TextMapPropagator).
//! Most apps install the W3C TraceContext propagator (yielding `traceparent`
//! and `tracestate`), which is exactly what the sqlcommenter spec calls for —
//! but if you've configured B3, Jaeger, a composite, or a custom propagator,
//! we hand off to that and use whatever keys/values it produces. Each
//! key/value pair is URL-encoded per RFC 3986 unreserved set, matching the
//! reference Python implementation.
//!
//! The trace context is read from `opentelemetry::Context::current()` while
//! the supplied tracing span is entered. That works because
//! `tracing-opentelemetry`'s `Layer` keeps the OTel current context in sync
//! with the tracing span stack on enter/exit. If no OTel layer is installed,
//! no propagator is registered, or the current span has no valid trace
//! context, this returns `None` and the query goes out unmodified.

use std::borrow::Cow;

/// Builds the trailing sqlcommenter comment for a query whose
/// `QueryLogger` span is `span`, or `None` if there's no trace context to
/// embed.
///
/// The comment is a no-op for the database engine (an SQL comment) but is
/// extracted by trace-aware observability tools.
#[cfg(feature = "sqlcommenter")]
pub fn comment_for_span(span: &tracing::Span) -> Option<String> {
    use opentelemetry::propagation::Injector;
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

    // RFC 3986 unreserved set: alphanumeric plus `-`, `_`, `.`, `~`. Matches the
    // sqlcommenter reference (Python) implementation, which uses urllib's
    // `quote` defaults. Anything else — including `*` (would close the comment
    // when adjacent to `/`), `'` (would break the quoted value), and `,`/`=`
    // (the separators in our output) — gets percent-encoded.
    const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');

    /// Streams propagator output directly into the sqlcommenter wire format.
    /// The opening `"/*"` is written on the first `set` call (so an empty
    /// buffer unambiguously means "propagator emitted nothing"); subsequent
    /// calls prepend `","`. The caller appends `"*/"` to close the comment.
    #[derive(Default)]
    struct SqlcommenterCarrier {
        out: String,
    }

    impl Injector for SqlcommenterCarrier {
        fn set(&mut self, key: &str, value: String) {
            if self.out.is_empty() {
                // Pre-size to a conservative typical W3C TraceContext payload
                // (`traceparent` ~70 chars plus `tracestate` and wrapping).
                self.out.reserve(96);
                self.out.push_str("/*");
            } else {
                self.out.push(',');
            }
            self.out.extend(utf8_percent_encode(key, ENCODE_SET));
            self.out.push_str("='");
            self.out.extend(utf8_percent_encode(&value, ENCODE_SET));
            self.out.push('\'');
        }
    }

    // Enter the QueryLogger span briefly so `Context::current()` resolves to
    // its OTel context (tracing-opentelemetry's `Layer` syncs current-OTel
    // with current-tracing on enter/exit). The guard is dropped synchronously
    // before this function returns — never held across an await.
    let mut carrier = span.in_scope(|| {
        let cx = opentelemetry::Context::current();
        let mut carrier = SqlcommenterCarrier::default();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut carrier);
        });
        carrier
    });

    if carrier.out.is_empty() {
        // Propagator emitted nothing (no registered propagator, or no valid
        // trace context in the current OTel context).
        return None;
    }

    carrier.out.push_str("*/");
    Some(carrier.out)
}

/// Feature-disabled stub: always returns `None` so [`maybe_append_comment`]
/// is a zero-cost no-op when the user hasn't opted into sqlcommenter.
#[cfg(not(feature = "sqlcommenter"))]
pub fn comment_for_span(_span: &tracing::Span) -> Option<String> {
    None
}

/// Appends [`comment_for_span`]'s output to `sql` if a trace context exists.
/// Returns the original `sql` borrowed otherwise so the no-context (and
/// feature-disabled) fast path allocates nothing.
pub fn maybe_append_comment<'a>(sql: &'a str, span: &tracing::Span) -> Cow<'a, str> {
    match comment_for_span(span) {
        Some(comment) => Cow::Owned(format!("{sql} {comment}")),
        None => Cow::Borrowed(sql),
    }
}
