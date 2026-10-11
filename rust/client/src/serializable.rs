//! Checked Serde construction. The JSON writer remains serde_json's; this
//! adapter guards representability and structural work before delegating.
use crate::{Code, Diagnostic, Json, Limits};
use serde::ser::{self, Serialize, Serializer};
use std::{cell::Cell, io};

#[derive(Clone, Copy)]
enum Failure {
    Limit,
    Representation,
}
struct State {
    limits: Limits,
    nodes: Cell<usize>,
    depth: Cell<usize>,
    failure: Cell<Option<Failure>>,
}
impl State {
    fn fail<E: ser::Error>(&self, failure: Failure) -> E {
        if self.failure.get().is_none() {
            self.failure.set(Some(failure));
        }
        E::custom("JSON construction failed")
    }
    fn check<E: ser::Error>(&self) -> Result<(), E> {
        match self.failure.get() {
            Some(f) => Err(self.fail(f)),
            None => Ok(()),
        }
    }
    fn capture<T, E: ser::Error>(&self, result: Result<T, E>) -> Result<T, E> {
        result.map_err(|_| self.fail(Failure::Representation))
    }
    fn node<E: ser::Error>(&self) -> Result<(), E> {
        self.check()?;
        if self.nodes.get() >= self.limits.nodes || self.depth.get() > self.limits.depth.min(96) {
            return Err(self.fail(Failure::Limit));
        }
        self.nodes.set(self.nodes.get() + 1);
        Ok(())
    }
    fn enter<E: ser::Error>(&self) -> Result<(), E> {
        self.node()?;
        self.depth.set(self.depth.get() + 1);
        Ok(())
    }
}
struct BoundedWriter<'a> {
    bytes: Vec<u8>,
    state: &'a State,
}
impl io::Write for BoundedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len()
            > self
                .state
                .limits
                .source_bytes
                .saturating_sub(self.bytes.len())
        {
            if self.state.failure.get().is_none() {
                self.state.failure.set(Some(Failure::Limit));
            }
            return Err(io::Error::other("JSON byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Checked<'a, T: ?Sized> {
    value: &'a T,
    state: &'a State,
}
impl<T: Serialize + ?Sized> Serialize for Checked<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.state.check()?;
        let result = self.value.serialize(Guard {
            inner: serializer,
            state: self.state,
        });
        let result = self.state.capture(result)?;
        self.state.check()?;
        Ok(result)
    }
}
struct Guard<'a, S> {
    inner: S,
    state: &'a State,
}
struct Compound<'a, C> {
    inner: Option<C>,
    state: &'a State,
    number: bool,
    fields: usize,
}
impl<C> Drop for Compound<'_, C> {
    fn drop(&mut self) {
        if !self.number {
            self.state.depth.set(self.state.depth.get() - 1);
        }
    }
}
macro_rules! primitives {
    ($($method:ident: $ty:ty),* $(,)?) => {$(
        fn $method(self, value: $ty) -> Result<Self::Ok, Self::Error> {
            self.state.node()?;
            self.state.capture(self.inner.$method(value))
        }
    )*};
}
impl<'a, S: Serializer> Serializer for Guard<'a, S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Compound<'a, S::SerializeSeq>;
    type SerializeTuple = Compound<'a, S::SerializeTuple>;
    type SerializeTupleStruct = Compound<'a, S::SerializeTupleStruct>;
    type SerializeTupleVariant = Compound<'a, S::SerializeTupleVariant>;
    type SerializeMap = Compound<'a, S::SerializeMap>;
    type SerializeStruct = Compound<'a, S::SerializeStruct>;
    type SerializeStructVariant = Compound<'a, S::SerializeStructVariant>;
    primitives!(serialize_bool:bool, serialize_i8:i8, serialize_i16:i16, serialize_i32:i32,
        serialize_i64:i64, serialize_i128:i128, serialize_u8:u8, serialize_u16:u16,
        serialize_u32:u32, serialize_u64:u64, serialize_u128:u128, serialize_char:char,
        serialize_str:&str);
    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(self.state.fail(Failure::Representation));
        }
        self.state.node()?;
        self.state.capture(self.inner.serialize_f32(value))
    }
    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(self.state.fail(Failure::Representation));
        }
        self.state.node()?;
        self.state.capture(self.inner.serialize_f64(value))
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        use ser::SerializeSeq;
        let mut seq = self.serialize_seq(Some(value.len()))?;
        for value in value {
            seq.serialize_element(value)?;
        }
        seq.end()
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        Checked {
            value,
            state: self.state,
        }
        .serialize(self.inner)
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.state.node()?;
        self.state.capture(self.inner.serialize_unit())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.state.node()?;
        self.state
            .capture(self.inner.serialize_unit_variant(name, index, variant))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.state.check()?;
        // RawValue can inject an arbitrarily nested value without checked child
        // callbacks. Use Json::parse for raw JSON; other private protocols refuse.
        if name.starts_with("$serde_json::private::") {
            return Err(self.state.fail(Failure::Representation));
        }
        self.state.capture(self.inner.serialize_newtype_struct(
            name,
            &Checked {
                value,
                state: self.state,
            },
        ))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.state.enter()?;
        let result = self.state.capture(self.inner.serialize_newtype_variant(
            name,
            index,
            variant,
            &Checked {
                value,
                state: self.state,
            },
        ));
        self.state.depth.set(self.state.depth.get() - 1);
        result
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.state.enter()?;
        Ok(Compound {
            inner: Some(self.state.capture(self.inner.serialize_seq(len))?),
            state: self.state,
            number: false,
            fields: 0,
        })
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.state.enter()?;
        Ok(Compound {
            inner: Some(self.state.capture(self.inner.serialize_tuple(len))?),
            state: self.state,
            number: false,
            fields: 0,
        })
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.state.enter()?;
        Ok(Compound {
            inner: Some(
                self.state
                    .capture(self.inner.serialize_tuple_struct(name, len))?,
            ),
            state: self.state,
            number: false,
            fields: 0,
        })
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        // Variant's outer object is guarded here; final admission counts both containers.
        self.state.enter()?;
        Ok(Compound {
            inner: Some(
                self.state.capture(
                    self.inner
                        .serialize_tuple_variant(name, index, variant, len),
                )?,
            ),
            state: self.state,
            number: false,
            fields: 0,
        })
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.state.enter()?;
        Ok(Compound {
            inner: Some(self.state.capture(self.inner.serialize_map(len))?),
            state: self.state,
            number: false,
            fields: 0,
        })
    }
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        // serde_json::Number's pinned arbitrary_precision protocol writes one
        // number token. Its field bypasses string-node accounting; final strict
        // admission validates the generated token (including custom spoofing).
        let number = name == "$serde_json::private::Number";
        if number {
            self.state.node()?;
        } else {
            if name.starts_with("$serde_json::private::") {
                return Err(self.state.fail(Failure::Representation));
            }
            self.state.enter()?;
        }
        Ok(Compound {
            inner: Some(self.state.capture(self.inner.serialize_struct(name, len))?),
            state: self.state,
            number,
            fields: 0,
        })
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.state.enter()?;
        Ok(Compound {
            inner: Some(
                self.state.capture(
                    self.inner
                        .serialize_struct_variant(name, index, variant, len),
                )?,
            ),
            state: self.state,
            number: false,
            fields: 0,
        })
    }
}
macro_rules! sequences {
    ($trait:ident, $method:ident) => {
        impl<C: ser::$trait> ser::$trait for Compound<'_, C> {
            type Ok = C::Ok;
            type Error = C::Error;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
                self.state.check()?;
                self.state
                    .capture(self.inner.as_mut().unwrap().$method(&Checked {
                        value,
                        state: self.state,
                    }))
            }
            fn end(mut self) -> Result<Self::Ok, Self::Error> {
                self.state.check()?;
                self.state.capture(self.inner.take().unwrap().end())
            }
        }
    };
}
sequences!(SerializeSeq, serialize_element);
sequences!(SerializeTuple, serialize_element);
sequences!(SerializeTupleStruct, serialize_field);
sequences!(SerializeTupleVariant, serialize_field);
impl<C: ser::SerializeMap> ser::SerializeMap for Compound<'_, C> {
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.state.check()?;
        self.state
            .capture(self.inner.as_mut().unwrap().serialize_key(key))
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.state.check()?;
        self.state
            .capture(self.inner.as_mut().unwrap().serialize_value(&Checked {
                value,
                state: self.state,
            }))
    }
    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.state.check()?;
        self.state.capture(self.inner.take().unwrap().end())
    }
}
macro_rules! structures {
    ($trait:ident) => {
        impl<C: ser::$trait> ser::$trait for Compound<'_, C> {
            type Ok = C::Ok;
            type Error = C::Error;
            fn serialize_field<T: Serialize + ?Sized>(
                &mut self,
                key: &'static str,
                value: &T,
            ) -> Result<(), Self::Error> {
                self.state.check()?;
                let inner = self.inner.as_mut().unwrap();
                if self.number {
                    if key != "$serde_json::private::Number" || self.fields != 0 {
                        return Err(self.state.fail(Failure::Representation));
                    }
                    let token = value
                        .serialize(NumberString { state: self.state })
                        .map_err(|_| self.state.fail(Failure::Representation))?;
                    self.fields += 1;
                    self.state.capture(inner.serialize_field(key, &token))
                } else {
                    self.state.capture(inner.serialize_field(
                        key,
                        &Checked {
                            value,
                            state: self.state,
                        },
                    ))
                }
            }
            fn end(mut self) -> Result<Self::Ok, Self::Error> {
                self.state.check()?;
                if self.number && self.fields != 1 {
                    return Err(self.state.fail(Failure::Representation));
                }
                self.state.capture(self.inner.take().unwrap().end())
            }
        }
    };
}
structures!(SerializeStruct);
structures!(SerializeStructVariant);

impl Json {
    /// Construct exact JSON from ordinary Serde values. Finite floats and all
    /// integer widths retain serde_json's representation. Nonfinite floats refuse
    /// (including nested values); no implicit null substitution. Standard Serde
    /// enum/byte-array/map-key conversions apply. Duplicate names and invalid
    /// generated JSON refuse. RawValue/unknown private protocols refuse; parse
    /// raw JSON explicitly instead. Literal private-marker map keys stay keys.
    ///
    /// Emitted bytes, JSON nodes and container depth are bounded. Caller-provided
    /// Serialize/Display code, transparent-wrapper recursion and work performed
    /// before a serializer callback cannot be bounded or preempted. Errors never
    /// retain custom error text. This constructs a value; it evaluates no schema.
    pub fn from_serializable<T: Serialize + ?Sized>(
        value: &T,
        limits: Limits,
    ) -> Result<Self, Diagnostic> {
        let state = State {
            limits,
            nodes: Cell::new(0),
            depth: Cell::new(0),
            failure: Cell::new(None),
        };
        let mut writer = BoundedWriter {
            bytes: Vec::new(),
            state: &state,
        };
        let result = Checked {
            value,
            state: &state,
        }
        .serialize(&mut serde_json::Serializer::new(&mut writer));
        if result.is_err() || state.failure.get().is_some() {
            return Err(match state.failure.get() {
                Some(Failure::Limit) => {
                    Diagnostic::new(Code::Limit, "JSON construction limit exceeded")
                }
                _ => Diagnostic::new(
                    Code::InvalidValue,
                    "value cannot be represented as supported JSON",
                ),
            });
        }
        let text = std::str::from_utf8(&writer.bytes).map_err(|_| {
            Diagnostic::new(Code::InvalidValue, "serializer produced invalid UTF-8")
        })?;
        Self::parse(text, limits)
    }
}

// The arbitrary_precision protocol is privileged only at the named Serde
// callback. Its content must be exactly one numeric token, never injected JSON.
struct NumberString<'a> {
    state: &'a State,
}
impl Serializer for NumberString<'_> {
    type Ok = String;
    type Error = serde_json::Error;
    type SerializeSeq = ser::Impossible<String, serde_json::Error>;
    type SerializeTuple = ser::Impossible<String, serde_json::Error>;
    type SerializeTupleStruct = ser::Impossible<String, serde_json::Error>;
    type SerializeTupleVariant = ser::Impossible<String, serde_json::Error>;
    type SerializeMap = ser::Impossible<String, serde_json::Error>;
    type SerializeStruct = ser::Impossible<String, serde_json::Error>;
    type SerializeStructVariant = ser::Impossible<String, serde_json::Error>;
    fn serialize_str(self, token: &str) -> Result<String, Self::Error> {
        if token.len() > self.state.limits.source_bytes {
            return Err(self.state.fail(Failure::Limit));
        }
        if token.parse::<serde_json::Number>().is_err() || token.trim() != token {
            return Err(self.state.fail(Failure::Representation));
        }
        Ok(token.to_owned())
    }
    fn serialize_bool(self, _: bool) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_i8(self, _: i8) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_i16(self, _: i16) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_i32(self, _: i32) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_i64(self, _: i64) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_i128(self, _: i128) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_u8(self, _: u8) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_u16(self, _: u16) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_u32(self, _: u32) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_u64(self, _: u64) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_u128(self, _: u128) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_f32(self, _: f32) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_f64(self, _: f64) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_char(self, _: char) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_none(self) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_unit(self) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<String, Self::Error> {
        Err(self.state.fail(Failure::Representation))
    }
}
