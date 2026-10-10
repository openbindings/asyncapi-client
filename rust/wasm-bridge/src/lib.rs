//! Private ABI used by the handwritten TypeScript facade. Not a second engine.
use dynamic_asyncapi_client::{Diagnostic, Document, Json, Operation};
use wasm_bindgen::prelude::*;

fn encoded(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("metadata consists of JSON-compatible values")
}
fn error(error: Diagnostic) -> JsValue {
    JsValue::from_str(&encoded(&error))
}

#[wasm_bindgen]
pub struct DocumentHandle(Document);

#[wasm_bindgen]
impl DocumentHandle {
    #[wasm_bindgen(constructor)]
    pub fn new(source: &str, uri: Option<String>) -> Result<Self, JsValue> {
        Document::parse_with(source, uri.as_deref(), Default::default())
            .map(Self)
            .map_err(error)
    }
    pub fn with_resource(&self, uri: &str, source: &str) -> Result<Self, JsValue> {
        self.0.with_resource(uri, source).map(Self).map_err(error)
    }
    pub fn version(&self) -> String {
        self.0.version().to_owned()
    }
    pub fn root(&self) -> JsonHandle {
        JsonHandle(self.0.root())
    }
    pub fn operations(&self) -> OperationInventoryHandle {
        OperationInventoryHandle(self.0.operations())
    }
    pub fn operation_at(&self, pointer: &str) -> Result<OperationHandle, JsValue> {
        self.0
            .operation_at(pointer)
            .map(OperationHandle)
            .map_err(error)
    }
    pub fn operation_id(&self, id: &str) -> Result<OperationHandle, JsValue> {
        self.0.operation_id(id).map(OperationHandle).map_err(error)
    }
}

#[wasm_bindgen]
pub struct OperationInventoryHandle(std::vec::IntoIter<Result<Operation, Diagnostic>>);
#[wasm_bindgen]
impl OperationInventoryHandle {
    pub fn next_entry(&mut self) -> Result<Option<OperationHandle>, JsValue> {
        self.0
            .next()
            .transpose()
            .map(|op| op.map(OperationHandle))
            .map_err(error)
    }
}

#[wasm_bindgen]
pub struct OperationHandle(Operation);
#[wasm_bindgen]
impl OperationHandle {
    pub fn identity_json(&self) -> String {
        encoded(self.0.identity())
    }
    pub fn location_json(&self) -> String {
        encoded(&self.0.location())
    }
    pub fn describe_json(&self) -> Result<String, JsValue> {
        self.0.describe().map(|v| encoded(&v)).map_err(error)
    }
    pub fn authored(&self) -> JsonHandle {
        JsonHandle(self.0.authored())
    }
}

#[wasm_bindgen]
pub struct JsonHandle(Json);
#[wasm_bindgen]
impl JsonHandle {
    pub fn kind(&self) -> String {
        self.0.kind().into()
    }
    pub fn raw(&self) -> String {
        self.0.raw().into()
    }
    pub fn json(&self) -> String {
        self.0.to_json()
    }
    pub fn location_json(&self) -> String {
        encoded(&self.0.location())
    }
    pub fn number_text(&self) -> Option<String> {
        self.0.number_text().map(str::to_owned)
    }
    pub fn string(&self) -> Option<String> {
        self.0.as_str().map(str::to_owned)
    }
    pub fn boolean(&self) -> Option<bool> {
        self.0.as_bool()
    }
    pub fn get(&self, key: &str) -> Option<Self> {
        self.0.get(key).map(Self)
    }
    pub fn at(&self, index: usize) -> Option<Self> {
        self.0.at(index).map(Self)
    }
    pub fn pointer(&self, pointer: &str) -> Option<Self> {
        self.0.pointer(pointer).map(Self)
    }
}
