//! Transport layer - stdio and WebSocket
//!
//! Provides transport implementations for the LSP server:
//! - stdio: For native editors (Neovim, VS Code, Zed)
//! - WebSocket: For browser-based Monaco editor
//!
//! Note: The WebSocket transport handles protocol translation between the
//! LSP wire protocol (with Content-Length headers) and raw JSON-RPC over WebSocket.

use std::future::Future as StdFuture;
use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tower_lsp::{LspService, Server};

use super::backend::LspBackend;

/// Run the LSP server over stdio.
///
/// This is used by native editors like Neovim, VS Code, and Zed.
pub async fn run_stdio() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(LspBackend::new);

    Server::new(stdin, stdout, socket).serve(service).await;
}

/// Run the LSP server over WebSocket.
///
/// This is used by browser-based Monaco editor.
pub async fn run_websocket(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    eprintln!("[LSP] WebSocket server listening on {}", addr);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        eprintln!("[LSP] New WebSocket connection from {}", peer_addr);

        tokio::spawn(async move {
            if let Err(e) = handle_websocket_connection(stream).await {
                eprintln!("[LSP] WebSocket connection error: {}", e);
            }
        });
    }
}

/// Handle a single WebSocket connection.
///
/// This bridges between raw JSON-RPC over WebSocket and the LSP wire protocol
/// that tower-lsp expects (with Content-Length headers).
async fn handle_websocket_connection(
    stream: TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (ws_write, ws_read) = ws_stream.split();

    // Create channels for bridging WebSocket to tower-lsp
    let (input_tx, input_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);

    // Create the LSP service
    let (service, socket) = LspService::new(LspBackend::new);

    // Create async read/write adapters
    let input = ChannelReader::new(input_rx);
    let output = ChannelWriter::new(output_tx);

    // Spawn the LSP server
    let server_handle = tokio::spawn(async move {
        Server::new(input, output, socket).serve(service).await;
    });

    // Forward WebSocket messages to LSP input (add Content-Length headers)
    let ws_to_lsp = {
        let input_tx = input_tx.clone();
        async move {
            let mut ws_read = ws_read;
            while let Some(msg) = ws_read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        // Add LSP Content-Length header for tower-lsp
                        let json_bytes = text.as_bytes();
                        let header = format!("Content-Length: {}\r\n\r\n", json_bytes.len());
                        let mut full_message = header.into_bytes();
                        full_message.extend_from_slice(json_bytes);

                        if input_tx.send(full_message).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Binary(data)) => {
                        // Add LSP Content-Length header for tower-lsp
                        let header = format!("Content-Length: {}\r\n\r\n", data.len());
                        let mut full_message = header.into_bytes();
                        full_message.extend_from_slice(&data);

                        if input_tx.send(full_message).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Err(e) => {
                        eprintln!("[LSP] WebSocket read error: {}", e);
                        break;
                    }
                    _ => {} // Ignore ping/pong
                }
            }
        }
    };

    // Forward LSP output to WebSocket (strip Content-Length headers)
    let lsp_to_ws = async move {
        let mut ws_write = ws_write;
        let mut buffer = Vec::new();

        while let Some(data) = output_rx.recv().await {
            buffer.extend_from_slice(&data);

            // Process complete messages from buffer
            while let Some(json_msg) = extract_lsp_message(&mut buffer) {
                let msg = Message::Text(json_msg.into());
                if ws_write.send(msg).await.is_err() {
                    return;
                }
            }
        }
    };

    // Run both directions concurrently
    tokio::select! {
        _ = ws_to_lsp => {}
        _ = lsp_to_ws => {}
    }

    // Clean up
    drop(input_tx);
    let _ = server_handle.await;

    Ok(())
}

/// Extract a complete LSP message from the buffer, stripping the Content-Length header.
///
/// Returns the JSON content if a complete message is available, None otherwise.
fn extract_lsp_message(buffer: &mut Vec<u8>) -> Option<String> {
    // Find the header/body separator
    let header_end = find_header_end(buffer)?;

    // Parse Content-Length from header
    let header = std::str::from_utf8(&buffer[..header_end]).ok()?;
    let content_length = parse_content_length(header)?;

    // Check if we have the full body
    let body_start = header_end + 4; // Skip \r\n\r\n
    let message_end = body_start + content_length;

    if buffer.len() < message_end {
        return None; // Incomplete message
    }

    // Extract the JSON body
    let json_body = std::str::from_utf8(&buffer[body_start..message_end])
        .ok()?
        .to_string();

    // Remove the processed message from buffer
    buffer.drain(..message_end);

    Some(json_body)
}

/// Find the position of \r\n\r\n which separates headers from body
fn find_header_end(buffer: &[u8]) -> Option<usize> {
    for i in 0..buffer.len().saturating_sub(3) {
        if &buffer[i..i + 4] == b"\r\n\r\n" {
            return Some(i);
        }
    }
    None
}

/// Parse Content-Length value from LSP headers
fn parse_content_length(header: &str) -> Option<usize> {
    for line in header.lines() {
        if let Some(value) = line.strip_prefix("Content-Length:") {
            return value.trim().parse().ok();
        }
    }
    None
}

/// Async reader that reads from a channel.
struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    buffer: Vec<u8>,
    position: usize,
}

impl ChannelReader {
    fn new(rx: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            buffer: Vec::new(),
            position: 0,
        }
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // If we have buffered data, return it
        if self.position < self.buffer.len() {
            let remaining = &self.buffer[self.position..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            self.position += to_copy;

            // Clear buffer if fully consumed
            if self.position >= self.buffer.len() {
                self.buffer.clear();
                self.position = 0;
            }

            return std::task::Poll::Ready(Ok(()));
        }

        // Try to receive more data
        match self.rx.poll_recv(cx) {
            std::task::Poll::Ready(Some(data)) => {
                let to_copy = data.len().min(buf.remaining());
                buf.put_slice(&data[..to_copy]);

                // Buffer any excess
                if to_copy < data.len() {
                    self.buffer = data;
                    self.position = to_copy;
                }

                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(None) => {
                // Channel closed - EOF
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Async writer that writes to a channel.
struct ChannelWriter {
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    /// Permit for the next write (acquired via poll_reserve)
    permit: Option<tokio::sync::mpsc::OwnedPermit<Vec<u8>>>,
}

impl ChannelWriter {
    fn new(tx: tokio::sync::mpsc::Sender<Vec<u8>>) -> Self {
        Self { tx, permit: None }
    }
}

impl AsyncWrite for ChannelWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        // If we don't have a permit, try to acquire one
        if self.permit.is_none() {
            let tx = self.tx.clone();
            match tx.try_reserve_owned() {
                Ok(permit) => {
                    self.permit = Some(permit);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    // Channel full - need to poll for capacity
                    let tx = self.tx.clone();
                    let mut reserve_fut = Box::pin(tx.reserve_owned());
                    match reserve_fut.as_mut().poll(cx) {
                        std::task::Poll::Ready(Ok(permit)) => {
                            self.permit = Some(permit);
                        }
                        std::task::Poll::Ready(Err(_)) => {
                            return std::task::Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "Channel closed",
                            )));
                        }
                        std::task::Poll::Pending => {
                            return std::task::Poll::Pending;
                        }
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Channel closed",
                    )));
                }
            }
        }

        // We have a permit, send the data
        if let Some(permit) = self.permit.take() {
            permit.send(buf.to_vec());
            std::task::Poll::Ready(Ok(buf.len()))
        } else {
            std::task::Poll::Pending
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_content_length() {
        assert_eq!(parse_content_length("Content-Length: 123"), Some(123));
        assert_eq!(
            parse_content_length("Content-Length: 456\r\nContent-Type: application/json"),
            Some(456)
        );
        assert_eq!(parse_content_length("Invalid"), None);
    }

    #[test]
    fn test_extract_lsp_message() {
        // Content-Length: 8 means we extract exactly 8 bytes: {"id":1}
        let mut buffer = b"Content-Length: 8\r\n\r\n{\"id\":1}extra".to_vec();
        let msg = extract_lsp_message(&mut buffer);
        assert_eq!(msg, Some("{\"id\":1}".to_string()));
        // "extra" remains in buffer for next message
        assert_eq!(buffer, b"extra".to_vec());
    }

    #[test]
    fn test_extract_incomplete_message() {
        let mut buffer = b"Content-Length: 100\r\n\r\n{\"id\":1}".to_vec();
        let msg = extract_lsp_message(&mut buffer);
        assert_eq!(msg, None); // Body too short
    }

    #[test]
    fn test_find_header_end() {
        assert_eq!(find_header_end(b"Content-Length: 13\r\n\r\n{}"), Some(18));
        assert_eq!(find_header_end(b"incomplete"), None);
    }

    #[tokio::test]
    async fn test_channel_writer_backpressure() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use std::time::Duration;

        // Create a channel with capacity 1
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        let mut writer = ChannelWriter::new(tx);

        // Fill the channel by doing a successful write
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let result = Pin::new(&mut writer).poll_write(&mut cx, b"first");
        assert!(matches!(result, Poll::Ready(Ok(5))));

        // Now the channel is full. Try to write again - it should return Pending
        let result = Pin::new(&mut writer).poll_write(&mut cx, b"second");
        assert!(
            matches!(result, Poll::Pending),
            "Should return Pending when channel is full"
        );

        // Spawn a task to drain the channel after a delay
        let drain_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = rx.recv().await; // Drain first message
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = rx.recv().await; // Drain second message
        });

        // Now use AsyncWriteExt to test that writes complete properly with backpressure
        use tokio::io::AsyncWriteExt;
        let write_result =
            tokio::time::timeout(Duration::from_millis(200), writer.write_all(b"second")).await;

        // This will timeout if poll_write doesn't register a waker
        assert!(
            write_result.is_ok(),
            "Write should complete after channel drains, not hang. Bug: poll_write returns Pending without registering waker"
        );
        drain_task.await.unwrap();
    }
}
