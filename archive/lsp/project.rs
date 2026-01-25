//! Project-wide state management
//!
//! Tracks entities across multiple files in the workspace.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tower_lsp::lsp_types::Url;

use super::analysis::document::DocumentState;
use super::analysis::entities::{extract_entities, EntityKind, LocalEntity};
use crate::model::loader::load_model_from_str_lenient;
use crate::semantic::SemanticModel;

/// Debounce delay in milliseconds.
const DEBOUNCE_MS: u64 = 300;

/// Project-wide state tracking entities across files.
pub struct ProjectState {
    /// Root directory of the project.
    pub root: PathBuf,
    /// Open documents, keyed by URI.
    pub documents: DashMap<Url, DocumentState>,
    /// All entities across all documents.
    /// Maps entity name to (source URI, entity).
    pub entities: DashMap<String, (Url, LocalEntity)>,
    /// Version counter for debouncing. Each update increments this.
    update_version: AtomicU64,
    /// Channel to notify listeners of updates (after debounce).
    /// None when notifications are disabled (e.g., in tests).
    update_tx: Option<mpsc::Sender<()>>,
    /// Full semantic model (rebuilt on changes)
    semantic_model: Mutex<Option<SemanticModel>>,
    /// Flag indicating model needs rebuild
    model_dirty: AtomicBool,
}

impl ProjectState {
    /// Create a new project state with a channel for update notifications.
    /// Returns (ProjectState, receiver for debounced update notifications).
    pub fn with_channel(root: PathBuf) -> (Self, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel(16);
        (
            Self {
                root,
                documents: DashMap::new(),
                entities: DashMap::new(),
                update_version: AtomicU64::new(0),
                update_tx: Some(tx),
                semantic_model: Mutex::new(None),
                model_dirty: AtomicBool::new(true),
            },
            rx,
        )
    }

    /// Create a new project state (for testing or when notifications aren't needed).
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            documents: DashMap::new(),
            entities: DashMap::new(),
            update_version: AtomicU64::new(0),
            update_tx: None,
            semantic_model: Mutex::new(None),
            model_dirty: AtomicBool::new(true),
        }
    }

    /// Update a document and re-extract its entities.
    /// Triggers a debounced notification after DEBOUNCE_MS.
    pub fn update_document(&self, uri: Url, version: i32, source: String) {
        // Remove old entities from this document
        self.remove_entities_for_document(&uri);

        // Parse and store the document
        let doc = DocumentState::new(uri.clone(), version, source.clone());
        self.documents.insert(uri.clone(), doc);

        // Extract and store new entities
        let entities = extract_entities(&source);
        for entity in entities {
            self.entities
                .insert(entity.name.clone(), (uri.clone(), entity));
        }

        // Mark semantic model as needing rebuild
        self.model_dirty.store(true, Ordering::SeqCst);

        // Trigger debounced notification
        self.trigger_debounced_update();
    }

    /// Trigger a debounced update notification.
    /// The notification is sent after DEBOUNCE_MS.
    fn trigger_debounced_update(&self) {
        self.update_version.fetch_add(1, Ordering::SeqCst);

        // Only spawn if we have a notification channel
        if let Some(tx) = &self.update_tx {
            let tx = tx.clone();

            // Spawn a task to send notification after debounce delay.
            // Multiple rapid updates will queue multiple notifications,
            // but the receiver channel will coalesce them since we use try_send.
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(DEBOUNCE_MS)).await;
                let _ = tx.try_send(());
            });
        }
    }

    /// Get current update version (for debounce checking).
    pub fn current_version(&self) -> u64 {
        self.update_version.load(Ordering::SeqCst)
    }

    /// Remove a document and its entities.
    pub fn remove_document(&self, uri: &Url) {
        self.remove_entities_for_document(uri);
        self.documents.remove(uri);
    }

    /// Remove all entities associated with a document.
    fn remove_entities_for_document(&self, uri: &Url) {
        // Collect keys to remove (can't remove while iterating)
        let keys_to_remove: Vec<String> = self
            .entities
            .iter()
            .filter(|entry| &entry.value().0 == uri)
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.entities.remove(&key);
        }
    }

    /// Get a document by URI.
    pub fn get_document(
        &self,
        uri: &Url,
    ) -> Option<dashmap::mapref::one::Ref<'_, Url, DocumentState>> {
        self.documents.get(uri)
    }

    /// Find an entity by name.
    pub fn find_entity(&self, name: &str) -> Option<(Url, LocalEntity)> {
        self.entities.get(name).map(|r| r.value().clone())
    }

    /// Get all entities of a specific kind.
    pub fn entities_of_kind(&self, kind: EntityKind) -> Vec<(String, Url, LocalEntity)> {
        self.entities
            .iter()
            .filter(|entry| entry.value().1.kind == kind)
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.value().0.clone(),
                    entry.value().1.clone(),
                )
            })
            .collect()
    }

    /// Get all entity names (for completion suggestions).
    pub fn all_entity_names(&self) -> Vec<String> {
        self.entities.iter().map(|e| e.key().clone()).collect()
    }

    /// Get all source entities (for :source() completions).
    pub fn source_entities(&self) -> Vec<String> {
        self.entities_of_kind(EntityKind::Source)
            .into_iter()
            .map(|(name, _, _)| name)
            .collect()
    }

    /// Get all dimension entities (for relationship/includes completions).
    pub fn dimension_entities(&self) -> Vec<String> {
        self.entities_of_kind(EntityKind::Dimension)
            .into_iter()
            .map(|(name, _, _)| name)
            .collect()
    }

    /// Find duplicate entity definitions.
    /// Returns a list of (entity_name, list of defining URIs).
    pub fn find_duplicates(&self) -> Vec<(String, Vec<Url>)> {
        // Group entities by name across documents
        let mut by_name: std::collections::HashMap<String, Vec<Url>> =
            std::collections::HashMap::new();

        // We need to check all documents for entities with the same name
        for doc_ref in self.documents.iter() {
            let uri = doc_ref.key();
            let doc = doc_ref.value();
            let entities = extract_entities(&doc.source);

            for entity in entities {
                by_name
                    .entry(entity.name.clone())
                    .or_default()
                    .push(uri.clone());
            }
        }

        // Return only those with duplicates
        by_name
            .into_iter()
            .filter(|(_, uris)| uris.len() > 1)
            .collect()
    }

    /// Get the semantic model, rebuilding if dirty.
    /// Returns None if model fails to parse.
    pub fn semantic_model(&self) -> Option<std::sync::MutexGuard<'_, Option<SemanticModel>>> {
        // Atomically check and clear dirty flag to avoid TOCTOU race
        if self.model_dirty.swap(false, Ordering::SeqCst) {
            self.rebuild_semantic_model();
        }
        let guard = self.semantic_model.lock().unwrap();
        if guard.is_some() {
            Some(guard)
        } else {
            None
        }
    }

    fn rebuild_semantic_model(&self) {
        // Collect sources first to avoid holding DashMap locks during mutex acquisition
        let sources: Vec<String> = self
            .documents
            .iter()
            .map(|entry| entry.value().source.clone())
            .collect();

        if sources.is_empty() {
            // Clear stale model when no documents exist
            *self.semantic_model.lock().unwrap() = None;
            return;
        }

        let combined = sources.join("\n");

        // Parse with lenient mode (continues after errors)
        let result = load_model_from_str_lenient(&combined, "workspace.lua");

        // Build semantic model if we got a model
        let semantic = SemanticModel::new(result.model).ok();

        *self.semantic_model.lock().unwrap() = semantic;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_uri(name: &str) -> Url {
        Url::parse(&format!("file:///test/{}.lua", name)).unwrap()
    }

    #[test]
    fn test_project_tracks_entities() {
        let project = ProjectState::new(PathBuf::from("/test"));

        project.update_document(
            test_uri("a"),
            1,
            r#"source("orders"):from("raw.orders")"#.to_string(),
        );
        project.update_document(
            test_uri("b"),
            1,
            r#"source("products"):from("raw.products")"#.to_string(),
        );

        assert_eq!(project.entities.len(), 2);
        assert!(project.find_entity("orders").is_some());
        assert!(project.find_entity("products").is_some());
    }

    #[test]
    fn test_project_updates_entities_on_change() {
        let project = ProjectState::new(PathBuf::from("/test"));
        let uri = test_uri("model");

        // Initial content
        project.update_document(
            uri.clone(),
            1,
            r#"source("old_name"):from("t")"#.to_string(),
        );
        assert!(project.find_entity("old_name").is_some());

        // Update content
        project.update_document(
            uri.clone(),
            2,
            r#"source("new_name"):from("t")"#.to_string(),
        );
        assert!(project.find_entity("old_name").is_none());
        assert!(project.find_entity("new_name").is_some());
    }

    #[test]
    fn test_project_removes_entities_on_close() {
        let project = ProjectState::new(PathBuf::from("/test"));
        let uri = test_uri("model");

        project.update_document(uri.clone(), 1, r#"source("orders"):from("t")"#.to_string());
        assert!(project.find_entity("orders").is_some());

        project.remove_document(&uri);
        assert!(project.find_entity("orders").is_none());
    }

    #[test]
    fn test_find_duplicates() {
        let project = ProjectState::new(PathBuf::from("/test"));

        project.update_document(
            test_uri("a"),
            1,
            r#"source("orders"):from("raw.orders")"#.to_string(),
        );
        project.update_document(
            test_uri("b"),
            1,
            r#"source("orders"):from("other.orders")"#.to_string(),
        );

        let duplicates = project.find_duplicates();
        assert_eq!(duplicates.len(), 1);
        assert_eq!(duplicates[0].0, "orders");
        assert_eq!(duplicates[0].1.len(), 2);
    }

    #[test]
    fn test_entities_of_kind() {
        let project = ProjectState::new(PathBuf::from("/test"));

        project.update_document(
            test_uri("model"),
            1,
            r#"
source("orders"):from("raw.orders")
dimension("customers"):source("orders")
fact("sales"):source("orders")
"#
            .to_string(),
        );

        let sources = project.entities_of_kind(EntityKind::Source);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].0, "orders");

        let dimensions = project.entities_of_kind(EntityKind::Dimension);
        assert_eq!(dimensions.len(), 1);
        assert_eq!(dimensions[0].0, "customers");
    }

    #[test]
    fn test_source_entities_list() {
        let project = ProjectState::new(PathBuf::from("/test"));

        project.update_document(
            test_uri("model"),
            1,
            r#"
source("orders"):from("raw.orders")
source("products"):from("raw.products")
dimension("customers"):source("orders")
"#
            .to_string(),
        );

        let sources = project.source_entities();
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"orders".to_string()));
        assert!(sources.contains(&"products".to_string()));
    }
}
