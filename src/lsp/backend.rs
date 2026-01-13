//! LSP Backend - implements tower_lsp::LanguageServer
//!
//! This module contains the main language server implementation that handles
//! LSP protocol messages for the Mantis Lua DSL.

use std::path::PathBuf;

use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use super::analysis::document::DocumentState;
use super::capabilities::definition;
use super::capabilities::diagnostics::{check_duplicates, DocumentInfo};
use super::capabilities::hover;
use super::capabilities::inlay;
use super::capabilities::references;
use super::capabilities::rename;
use super::capabilities::signature;
use super::capabilities::symbols;
use super::project::ProjectState;

/// The LSP backend state.
pub struct LspBackend {
    /// The LSP client for sending notifications/requests back to the editor.
    client: Client,
    /// Project-wide state tracking documents and entities.
    project: ProjectState,
}

impl LspBackend {
    /// Create a new LSP backend.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            project: ProjectState::new(PathBuf::from(".")),
        }
    }

    /// Create a new LSP backend with a specific project root.
    pub fn with_root(client: Client, root: PathBuf) -> Self {
        Self {
            client,
            project: ProjectState::new(root),
        }
    }

    /// Get a reference to the client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get a reference to the project state.
    pub fn project(&self) -> &ProjectState {
        &self.project
    }

    /// Get a document by URI.
    pub fn get_document(
        &self,
        uri: &Url,
    ) -> Option<dashmap::mapref::one::Ref<'_, Url, DocumentState>> {
        self.project.get_document(uri)
    }

    /// Publish diagnostics for all open documents.
    async fn publish_all_diagnostics(&self) {
        // Collect document info for analysis
        let docs: Vec<_> = self
            .project
            .documents
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().source.clone()))
            .collect();

        let doc_infos: Vec<DocumentInfo<'_>> = docs
            .iter()
            .map(|(uri, source)| DocumentInfo { uri, source })
            .collect();

        // Check for duplicates across all documents
        let diagnostics_result = check_duplicates(&doc_infos);

        // Publish diagnostics for each document
        for (uri, _) in &docs {
            let diagnostics = diagnostics_result.for_uri(uri);
            self.client
                .publish_diagnostics(uri.clone(), diagnostics, None)
                .await;
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for LspBackend {
    async fn initialize(&self, _params: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                // Full document sync - we get the entire document on each change
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                // Enable completion
                completion_provider: Some(CompletionOptions {
                    trigger_characters: Some(vec![
                        ":".to_string(),  // Method chains
                        "\"".to_string(), // String literals
                        ".".to_string(),  // Table access
                        "(".to_string(),  // Function arguments
                    ]),
                    resolve_provider: Some(false),
                    ..Default::default()
                }),
                // Hover support
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                // Document symbols for outline view
                document_symbol_provider: Some(OneOf::Left(true)),
                // Workspace symbols for cross-file search
                workspace_symbol_provider: Some(OneOf::Left(true)),
                // Go-to-definition for entity references
                definition_provider: Some(OneOf::Left(true)),
                // Find all references to an entity
                references_provider: Some(OneOf::Left(true)),
                // Signature help for function parameters
                signature_help_provider: Some(SignatureHelpOptions {
                    trigger_characters: Some(vec!["(".to_string(), ",".to_string()]),
                    retrigger_characters: None,
                    work_done_progress_options: Default::default(),
                }),
                // Inlay hints for entity types
                inlay_hint_provider: Some(OneOf::Left(true)),
                // Rename support for entities
                rename_provider: Some(OneOf::Right(RenameOptions {
                    prepare_provider: Some(true),
                    work_done_progress_options: Default::default(),
                })),
                ..Default::default()
            },
            server_info: Some(ServerInfo {
                name: "mantis-lsp".to_string(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
        })
    }

    async fn initialized(&self, _params: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "Mantis language server initialized")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let version = params.text_document.version;
        let text = params.text_document.text;

        self.project.update_document(uri.clone(), version, text);

        self.client
            .log_message(MessageType::LOG, format!("Opened: {}", uri))
            .await;

        // Publish diagnostics for all documents (duplicates are cross-file)
        self.publish_all_diagnostics().await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        let version = params.text_document.version;

        // With FULL sync, we get the entire document content
        if let Some(change) = params.content_changes.into_iter().next() {
            self.project
                .update_document(uri.clone(), version, change.text);
        }

        // Publish diagnostics for all documents (duplicates are cross-file)
        self.publish_all_diagnostics().await;
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;

        // Clear diagnostics for closed document
        self.client
            .publish_diagnostics(uri.clone(), vec![], None)
            .await;

        self.project.remove_document(&uri);

        self.client
            .log_message(MessageType::LOG, format!("Closed: {}", uri))
            .await;

        // Re-publish diagnostics (closing might resolve duplicates)
        self.publish_all_diagnostics().await;
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let uri = &params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        // Detect completion context
        let context = super::analysis::context::detect_context(&doc.tree, &doc.source, position);

        // Get completions based on context
        let items =
            super::capabilities::completions::get_completions(&context, &doc, &self.project);

        if items.is_empty() {
            Ok(None)
        } else {
            Ok(Some(CompletionResponse::Array(items)))
        }
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        Ok(hover::get_hover(&self.project, &doc, position))
    }

    async fn signature_help(&self, params: SignatureHelpParams) -> Result<Option<SignatureHelp>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        Ok(signature::get_signature_help(&doc, position))
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        let uri = &params.text_document.uri;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        Ok(Some(symbols::get_document_symbols(&doc)))
    }

    async fn symbol(
        &self,
        params: WorkspaceSymbolParams,
    ) -> Result<Option<Vec<SymbolInformation>>> {
        let symbols = symbols::get_workspace_symbols(&self.project, &params.query);
        Ok(Some(symbols))
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        Ok(definition::get_definition(&self.project, &doc, position))
    }

    async fn references(&self, params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        let uri = &params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;
        let include_declaration = params.context.include_declaration;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        let refs = references::find_references(&self.project, &doc, position, include_declaration);

        if refs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(refs))
        }
    }

    async fn inlay_hint(&self, params: InlayHintParams) -> Result<Option<Vec<InlayHint>>> {
        let uri = &params.text_document.uri;
        let range = params.range;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        let hints = inlay::get_inlay_hints(&doc, range);

        if hints.is_empty() {
            Ok(None)
        } else {
            Ok(Some(hints))
        }
    }

    async fn prepare_rename(
        &self,
        params: TextDocumentPositionParams,
    ) -> Result<Option<PrepareRenameResponse>> {
        let uri = &params.text_document.uri;
        let position = params.position;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        Ok(rename::prepare_rename(&doc, position))
    }

    async fn rename(&self, params: RenameParams) -> Result<Option<WorkspaceEdit>> {
        let uri = &params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;
        let new_name = &params.new_name;

        let Some(doc) = self.project.get_document(uri) else {
            return Ok(None);
        };

        Ok(rename::rename(&self.project, &doc, position, new_name))
    }
}

#[cfg(test)]
mod tests {
    // Note: Integration tests for the full LSP protocol would require
    // setting up a mock client, which is complex. Unit tests for the
    // individual components (context detection, completions) are in
    // their respective modules.

    #[test]
    fn test_backend_creation() {
        // This test just verifies the types compile correctly
        // Actual backend testing requires async runtime and mock client
    }
}
