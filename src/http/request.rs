use http::{Method, Request, Version, header};

use super::Body;
use crate::ConnectRequest;

/// Builds the request: origin-form URL, headers, and the body stream.
pub(super) fn build_request<R>(
    request: &ConnectRequest,
    reader: R,
) -> Result<Request<Body<R>>, http::Error> {
    request_builder(request)?.body(Body::new(reader))
}

/// The method, the origin-form URL and the headers of a request.
pub(super) fn request_builder(
    request: &ConnectRequest,
) -> Result<http::request::Builder, http::Error> {
    let method = Method::from_bytes(request.http_method().unwrap_or("GET").as_bytes())?;

    let mut builder = Request::builder()
        .method(method)
        .uri(request.http_path().as_ref())
        .version(Version::HTTP_11);

    for (name, value) in request.http_headers() {
        builder = builder.header(name, value);
    }

    // The edge also reports the host on its own.
    let has_host = builder
        .headers_ref()
        .is_some_and(|headers| headers.contains_key(header::HOST));

    if let (false, Some(host)) = (has_host, request.http_host()) {
        builder = builder.header(header::HOST, host);
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamKind;

    fn connect_request(dest: &str, metadata: &[(&str, &str)]) -> ConnectRequest {
        ConnectRequest {
            dest: dest.to_owned(),
            kind: StreamKind::Http,
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn the_request_has_method_url_and_headers() {
        let edge = connect_request(
            "https://example.com/a/b?c=d",
            &[
                ("HttpMethod", "POST"),
                ("HttpHost", "example.com"),
                ("HttpHeader:Accept", "text/html"),
                ("HttpHeader:Set-Cookie", "a=1"),
                ("HttpHeader:Set-Cookie", "b=2"),
            ],
        );

        let request = build_request(&edge, tokio::io::empty()).unwrap();

        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.uri().path_and_query().unwrap(), "/a/b?c=d");
        assert_eq!(request.headers()["accept"], "text/html");
        assert_eq!(request.headers()["host"], "example.com");
        assert_eq!(request.headers().get_all("set-cookie").iter().count(), 2);
    }

    #[test]
    fn a_host_header_is_not_added_twice() {
        let edge = connect_request(
            "https://example.com/",
            &[
                ("HttpHost", "example.com"),
                ("HttpHeader:Host", "example.com"),
            ],
        );

        let request = build_request(&edge, tokio::io::empty()).unwrap();

        assert_eq!(request.headers().get_all("host").iter().count(), 1);
    }

    #[test]
    fn a_bad_method_or_header_is_an_error() {
        let bad_method = connect_request("https://h/", &[("HttpMethod", "NOT A METHOD")]);
        let bad_header = connect_request("https://h/", &[("HttpHeader:Bad Name", "x")]);

        assert!(build_request(&bad_method, tokio::io::empty()).is_err());
        assert!(build_request(&bad_header, tokio::io::empty()).is_err());
    }
}
