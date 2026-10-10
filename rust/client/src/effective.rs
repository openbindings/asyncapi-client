//! Merge Patch with authored origins. References inherited from an external
//! trait must still resolve relative to that trait, not the consuming operation.
use crate::{Code, Diagnostic, Document, Json};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug)]
pub(crate) struct Effective {
    pub source: Json,
    fields: Option<Arc<BTreeMap<String, Self>>>,
}

impl Effective {
    pub fn from(source: Json) -> Self {
        Self {
            source,
            fields: None,
        }
    }
    pub fn get(&self, key: &str) -> Option<Self> {
        match &self.fields {
            Some(fields) => fields.get(key).cloned(),
            None => self.source.get(key).map(Self::from),
        }
    }
    pub fn members(&self) -> Option<BTreeMap<String, Self>> {
        self.fields.as_ref().map(|x| (**x).clone()).or_else(|| {
            self.source.members().map(|items| {
                items
                    .into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect()
            })
        })
    }
    pub fn is_object(&self) -> bool {
        self.fields.is_some() || self.source.kind() == "object"
    }
    pub fn string(&self) -> Option<&str> {
        if self.fields.is_some() {
            None
        } else {
            self.source.as_str()
        }
    }
    pub fn optional_string(&self, key: &str) -> Result<Option<String>, Diagnostic> {
        self.get(key)
            .map(|node| {
                node.string().map(str::to_owned).ok_or_else(|| {
                    Diagnostic::new(Code::InvalidOperation, "declared field must be a string")
                        .at(node.source.location())
                })
            })
            .transpose()
    }
    pub fn resolve(&self, document: &Document) -> Result<Self, Diagnostic> {
        if let Some(reference) = self.get("$ref") {
            // Reference Object siblings have no effect. Use the actual $ref's
            // parent, since a merged container can have several source origins.
            let (parent, _) = reference.source.pointer.rsplit_once('/').unwrap();
            let object = reference.source.source.root().pointer(parent).unwrap();
            return document.resolve(object).map(Self::from);
        }
        Ok(self.clone())
    }
    fn merge(&mut self, patch: Json, remaining: &mut usize) -> Result<(), Diagnostic> {
        *remaining = remaining.checked_sub(1).ok_or_else(|| {
            Diagnostic::new(Code::Limit, "effective declaration merge limit exceeded")
                .at(patch.location())
        })?;
        if let Some(items) = patch.members() {
            let mut fields = self.members().unwrap_or_default();
            for (key, value) in items {
                if value.is_null() {
                    *remaining = remaining.checked_sub(1).ok_or_else(|| {
                        Diagnostic::new(Code::Limit, "effective declaration merge limit exceeded")
                            .at(value.location())
                    })?;
                    fields.remove(&key);
                } else if let Some(existing) = fields.get_mut(&key) {
                    existing.merge(value, remaining)?;
                } else {
                    let mut entry = Self {
                        source: value.clone(),
                        fields: Some(Arc::new(BTreeMap::new())),
                    };
                    entry.merge(value, remaining)?;
                    fields.insert(key, entry);
                }
            }
            self.source = patch;
            self.fields = Some(Arc::new(fields));
        } else {
            // Arrays are atomic under Merge Patch and remain owning source views.
            *self = Self::from(patch);
        }
        Ok(())
    }
}

pub(crate) fn with_traits(
    document: &Document,
    object: Json,
    forbidden: &[&str],
) -> Result<Effective, Diagnostic> {
    if object.kind() != "object" {
        return Err(
            Diagnostic::new(Code::InvalidOperation, "declaration must be an object")
                .at(object.location()),
        );
    }
    if object
        .get("traits")
        .is_none_or(|t| t.elements().is_some_and(|a| a.is_empty()))
    {
        return Ok(Effective::from(object));
    }
    let limits = document.limits();
    let mut remaining = limits.merge_nodes;
    let mut effective = Effective {
        source: object.clone(),
        fields: Some(Arc::new(BTreeMap::new())),
    };
    if let Some(traits) = object.get("traits") {
        let items = traits.elements().ok_or_else(|| {
            Diagnostic::new(Code::InvalidOperation, "traits must be an array").at(traits.location())
        })?;
        if items.len() > limits.trait_count {
            return Err(
                Diagnostic::new(Code::Limit, "trait count limit exceeded").at(traits.location())
            );
        }
        for item in items {
            let item = document.resolve(item)?;
            if item.kind() != "object" || forbidden.iter().any(|key| item.get(key).is_some()) {
                return Err(Diagnostic::new(
                    Code::InvalidOperation,
                    "invalid trait field or shape",
                )
                .at(item.location()));
            }
            effective.merge(item, &mut remaining)?;
        }
    }
    effective.merge(object, &mut remaining)?;
    Ok(effective)
}
