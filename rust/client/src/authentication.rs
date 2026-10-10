//! Document requirements and explicit alternatives; never credential values.
use crate::{Code, Diagnostic, Document, Edition, Json, Location, Requirement};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SecurityScope {
    Server,
    Operation,
}

/// Zero-based alternatives in the authored security arrays. A single alternative
/// is selected automatically; multiple alternatives require an explicit choice.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecuritySelection {
    pub server: Option<usize>,
    pub operation: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecuritySchemeDescription {
    pub scheme_type: String,
    /// Symbolic component name from a 2.x requirement, absent in 3.x.
    pub component_name: Option<String>,
    pub selection: Location,
    pub definition: Location,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityAlternative {
    pub index: usize,
    pub location: Location,
    /// Every scheme within a selected alternative is required.
    pub schemes: Vec<SecuritySchemeDescription>,
}

/// Alternatives within each array are OR; server and operation are AND.
/// This is requirement inspection, not complete security-scheme validation.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationDescription {
    pub server: Vec<SecurityAlternative>,
    pub operation: Vec<SecurityAlternative>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationPlan {
    pub server: Option<SecurityAlternative>,
    pub operation: Option<SecurityAlternative>,
}
impl AuthenticationPlan {
    pub fn schemes(&self) -> impl Iterator<Item = &SecuritySchemeDescription> {
        self.server
            .iter()
            .chain(self.operation.iter())
            .flat_map(|v| &v.schemes)
    }
    pub(crate) fn require_profile(&self, protocol: &str) -> Result<(), Diagnostic> {
        for scheme in self.schemes() {
            let supported = match scheme.scheme_type.as_str() {
                "userPassword" => matches!(protocol, "mqtt" | "mqtts"),
                "X509" => matches!(protocol, "mqtts" | "wss"),
                _ => false,
            };
            if !supported {
                return Err(Diagnostic::new(
                    Code::UnsupportedFeature,
                    "security scheme is not supported by the selected transport profile",
                )
                .at(scheme.selection.clone())
                .needs(Requirement::Authentication));
            }
        }
        Ok(())
    }
}

fn invalid(node: &Json, detail: &str) -> Diagnostic {
    Diagnostic::new(Code::InvalidOperation, detail).at(node.location())
}
fn bounded(node: &Json, len: usize, maximum: usize) -> Result<(), Diagnostic> {
    if len > maximum {
        return Err(Diagnostic::new(
            Code::Limit,
            "security declaration exceeds the profile limit",
        )
        .at(node.location()));
    }
    Ok(())
}
fn alternatives(node: Option<&Json>) -> Result<Vec<Json>, Diagnostic> {
    let Some(node) = node else {
        return Ok(vec![]);
    };
    let values = node
        .elements()
        .ok_or_else(|| invalid(node, "security must be an array"))?;
    bounded(node, values.len(), 64)?;
    Ok(values)
}
fn scopes(node: &Json, remaining: &mut usize) -> Result<Vec<String>, Diagnostic> {
    let values = node
        .elements()
        .ok_or_else(|| invalid(node, "security scopes must be an array"))?;
    bounded(node, values.len(), 256)?;
    values
        .into_iter()
        .map(|v| {
            let text = v
                .as_str()
                .ok_or_else(|| invalid(&v, "security scope must be a string"))?;
            *remaining = remaining.checked_sub(text.len()).ok_or_else(|| {
                Diagnostic::new(
                    Code::Limit,
                    "resolved security scope bytes exceed the source byte limit",
                )
                .at(v.location())
            })?;
            Ok(text.to_owned())
        })
        .collect()
}
fn scheme(
    document: &Document,
    selection: Json,
    component_name: Option<String>,
    definition: Json,
    required_scopes: Option<Json>,
    remaining: &mut usize,
) -> Result<SecuritySchemeDescription, Diagnostic> {
    let definition = document.resolve(definition)?;
    let kind = definition
        .get("type")
        .ok_or_else(|| invalid(&definition, "security scheme type is required"))?;
    let kind = kind
        .as_str()
        .ok_or_else(|| invalid(&kind, "security scheme type must be a string"))?;
    if !matches!(
        kind,
        "userPassword"
            | "apiKey"
            | "X509"
            | "symmetricEncryption"
            | "asymmetricEncryption"
            | "httpApiKey"
            | "http"
            | "oauth2"
            | "openIdConnect"
            | "plain"
            | "scramSha256"
            | "scramSha512"
            | "gssapi"
    ) {
        return Err(invalid(&definition, "unknown security scheme type"));
    }
    let required_scopes = required_scopes.or_else(|| {
        (document.edition() != Edition::V2_6)
            .then(|| definition.get("scopes"))
            .flatten()
    });
    let scopes = required_scopes
        .as_ref()
        .map(|node| scopes(node, remaining))
        .transpose()?
        .unwrap_or_default();
    if !scopes.is_empty() && !matches!(kind, "oauth2" | "openIdConnect") {
        return Err(invalid(
            required_scopes.as_ref().unwrap(),
            "this security scheme cannot require OAuth scopes",
        ));
    }
    Ok(SecuritySchemeDescription {
        scheme_type: kind.into(),
        component_name,
        selection: selection.location(),
        definition: definition.location(),
        scopes,
    })
}
fn alternative(
    document: &Document,
    node: Json,
    index: usize,
    remaining: &mut usize,
) -> Result<SecurityAlternative, Diagnostic> {
    let schemes = if document.edition() == Edition::V2_6 {
        let entries = node
            .members()
            .ok_or_else(|| invalid(&node, "security requirement must be an object"))?;
        bounded(&node, entries.len(), 16)?;
        let definitions = document
            .root()
            .get("components")
            .and_then(|v| v.get("securitySchemes"));
        entries
            .into_iter()
            .map(|(name, required_scopes)| {
                let definition =
                    definitions
                        .as_ref()
                        .and_then(|v| v.get(&name))
                        .ok_or_else(|| {
                            invalid(
                                &required_scopes,
                                "security requirement names an unknown component",
                            )
                        })?;
                scheme(
                    document,
                    required_scopes.clone(),
                    Some(name),
                    definition,
                    Some(required_scopes),
                    remaining,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![scheme(
            document,
            node.clone(),
            None,
            node.clone(),
            None,
            remaining,
        )?]
    };
    Ok(SecurityAlternative {
        index,
        location: node.location(),
        schemes,
    })
}
pub(crate) fn inspect(
    document: &Document,
    server: Option<&Json>,
    operation: Option<&Json>,
) -> Result<AuthenticationDescription, Diagnostic> {
    let mut remaining = document.limits().source_bytes;
    let mut read = |node| {
        alternatives(node)?
            .into_iter()
            .enumerate()
            .map(|(index, node)| alternative(document, node, index, &mut remaining))
            .collect::<Result<Vec<_>, Diagnostic>>()
    };
    Ok(AuthenticationDescription {
        server: read(server)?,
        operation: read(operation)?,
    })
}
fn select(
    document: &Document,
    node: Option<&Json>,
    choice: Option<usize>,
    scope: SecurityScope,
    remaining: &mut usize,
) -> Result<Option<SecurityAlternative>, Diagnostic> {
    let values = alternatives(node)?;
    if let Some(index) = choice {
        let selected = values.get(index).ok_or_else(|| {
            Diagnostic::new(
                Code::InvalidConfiguration,
                "selected security alternative is not available",
            )
        })?;
        return alternative(document, selected.clone(), index, remaining).map(Some);
    }
    match values.len() {
        0 => Ok(None),
        1 => alternative(document, values[0].clone(), 0, remaining).map(Some),
        count => Err(Diagnostic::new(
            Code::MissingConfiguration,
            "select a security alternative explicitly",
        )
        .at(node.unwrap().location())
        .needs(Requirement::AuthenticationChoice {
            scope,
            choices: (0..count).collect(),
        })),
    }
}
pub(crate) fn prepare(
    document: &Document,
    server: Option<&Json>,
    operation: Option<&Json>,
    choice: &SecuritySelection,
    protocol: &str,
) -> Result<AuthenticationPlan, Diagnostic> {
    let mut remaining = document.limits().source_bytes;
    let plan = AuthenticationPlan {
        server: select(
            document,
            server,
            choice.server,
            SecurityScope::Server,
            &mut remaining,
        )?,
        operation: select(
            document,
            operation,
            choice.operation,
            SecurityScope::Operation,
            &mut remaining,
        )?,
    };
    plan.require_profile(protocol)?;
    Ok(plan)
}
