use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Not;

use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap as _;
use uuid::Uuid;

use crate::time::Date;
use crate::time::DateTime;
use crate::time::Time;

/// api metadata of one app, served by `GET /_sys/api`, used to generate client stubs and to check compatibility between versions.
#[derive(Debug, Serialize)]
pub struct ApiDefinition {
    pub app: String,
    pub services: Vec<ServiceDefinition>,
    pub types: Vec<TypeDefinition>,
}

#[derive(Debug, Serialize)]
pub struct ServiceDefinition {
    pub name: String,
    pub operations: Vec<OperationDefinition>,
}

#[derive(Debug, Serialize)]
pub struct OperationDefinition {
    pub name: String,
    pub method: String,
    pub path: String,
    /// None for operation without request param.
    pub request: Option<TypeRef>,
    /// None for `Result<(), Exception>`, which responds 204.
    pub response: Option<TypeRef>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TypeDefinition {
    Struct { name: String, fields: Vec<FieldDefinition> },
    Enum { name: String, values: Vec<String> },
}

impl TypeDefinition {
    pub fn name(&self) -> &str {
        match self {
            TypeDefinition::Struct { name, .. } | TypeDefinition::Enum { name, .. } => name,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct FieldDefinition {
    pub name: String,
    pub r#type: TypeRef,
    pub constraints: Constraints,
}

/// validation rules declared by `#[derive(Validate)]` attributes.
#[derive(Debug, Default, Serialize, PartialEq, Eq)]
pub struct Constraints {
    #[serde(skip_serializing_if = "Not::not")]
    pub not_blank: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<i64>,
}

/// serialized as `{"kind": ...}`, kind is either rust primitive type name or one of `option`/`list`/`map`/`ref`,
/// primitive names are a closed set defined by framework, so they never collide with the structural kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeRef {
    /// rust type name as is, e.g. `i32`, `String`, `Uuid`, `DateTime`, consumers know the service is implemented in rust.
    Primitive { name: String },
    Option { item: Box<TypeRef> },
    List { item: Box<TypeRef> },
    /// json object with string keys.
    Map { value: Box<TypeRef> },
    /// reference to a named type in `ApiDefinition::types`.
    Ref { name: String },
}

impl Serialize for TypeRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            TypeRef::Primitive { name } => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("kind", name)?;
                map.end()
            }
            TypeRef::Option { item } | TypeRef::List { item } => {
                let kind = if matches!(self, TypeRef::Option { .. }) { "option" } else { "list" };
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", kind)?;
                map.serialize_entry("item", item)?;
                map.end()
            }
            TypeRef::Map { value } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "map")?;
                map.serialize_entry("value", value)?;
                map.end()
            }
            TypeRef::Ref { name } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "ref")?;
                map.serialize_entry("name", name)?;
                map.end()
            }
        }
    }
}

/// describes a type used in api request/response, implemented by `#[derive(ApiType)]` for structs and enums,
/// and by framework for primitives and containers.
pub trait ApiType {
    fn type_ref(registry: &mut TypeRegistry) -> TypeRef;

    /// None means no body, e.g. unit type `()`.
    fn optional_type_ref(registry: &mut TypeRegistry) -> Option<TypeRef> {
        Some(Self::type_ref(registry))
    }
}

/// collects named type definitions while walking api types.
#[derive(Default)]
pub struct TypeRegistry {
    // key is simple type name, value is (full rust type name, definition), definition is None while it's being defined (recursive type)
    types: BTreeMap<String, (&'static str, Option<TypeDefinition>)>,
}

impl TypeRegistry {
    /// returns true if the type is new and must be defined by calling `define`,
    /// panics if another rust type with same simple name is already registered, simple names must be unique within app.
    pub fn reserve(&mut self, name: &str, rust_type: &'static str) -> bool {
        if let Some((existing, _)) = self.types.get(name) {
            assert!(*existing == rust_type, "duplicate api type name, name={name}, types=[{existing}, {rust_type}]");
            return false;
        }
        self.types.insert(name.to_owned(), (rust_type, None));
        true
    }

    pub fn define(&mut self, definition: TypeDefinition) {
        let name = definition.name();
        let entry = self.types.get_mut(name).unwrap_or_else(|| panic!("type must be reserved before define, name={name}"));
        entry.1 = Some(definition);
    }

    /// returns definitions sorted by name.
    pub fn into_types(self) -> Vec<TypeDefinition> {
        self.types
            .into_iter()
            .map(|(name, (_, definition))| definition.unwrap_or_else(|| panic!("type is reserved but not defined, name={name}")))
            .collect()
    }
}

macro_rules! primitive_api_type {
    ($($rust_type:ty),+ $(,)?) => {
        $(
            impl ApiType for $rust_type {
                #[inline]
                fn type_ref(_registry: &mut TypeRegistry) -> TypeRef {
                    TypeRef::Primitive { name: stringify!($rust_type).to_owned() }
                }
            }
        )+
    };
}

primitive_api_type! {
    String, bool,
    i8, i16, i32, i64, isize,
    u8, u16, u32, u64, usize,
    f32, f64,
    Uuid, Date, Time, DateTime,
}

impl ApiType for () {
    fn type_ref(_registry: &mut TypeRegistry) -> TypeRef {
        panic!("unit type can only be used as request or response")
    }

    #[inline]
    fn optional_type_ref(_registry: &mut TypeRegistry) -> Option<TypeRef> {
        None
    }
}

impl<T, E> ApiType for Result<T, E>
where
    T: ApiType,
{
    #[inline]
    fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
        T::type_ref(registry)
    }

    #[inline]
    fn optional_type_ref(registry: &mut TypeRegistry) -> Option<TypeRef> {
        T::optional_type_ref(registry)
    }
}

impl<T> ApiType for Option<T>
where
    T: ApiType,
{
    fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
        TypeRef::Option { item: Box::new(T::type_ref(registry)) }
    }
}

impl<T> ApiType for Vec<T>
where
    T: ApiType,
{
    fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
        TypeRef::List { item: Box::new(T::type_ref(registry)) }
    }
}

impl<T> ApiType for Box<T>
where
    T: ApiType,
{
    #[inline]
    fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
        T::type_ref(registry)
    }
}

impl<V, S> ApiType for HashMap<String, V, S>
where
    V: ApiType,
{
    fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
        TypeRef::Map { value: Box::new(V::type_ref(registry)) }
    }
}

impl<V> ApiType for BTreeMap<String, V>
where
    V: ApiType,
{
    fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
        TypeRef::Map { value: Box::new(V::type_ref(registry)) }
    }
}

#[cfg(test)]
mod tests {
    use std::any::type_name;

    use super::*;

    struct Child;

    impl ApiType for Child {
        fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
            if registry.reserve("Child", type_name::<Self>()) {
                let fields = vec![FieldDefinition {
                    name: "value".to_owned(),
                    r#type: <i32 as ApiType>::type_ref(registry),
                    constraints: Constraints { min: Some(1), ..Constraints::default() },
                }];
                registry.define(TypeDefinition::Struct { name: "Child".to_owned(), fields });
            }
            TypeRef::Ref { name: "Child".to_owned() }
        }
    }

    // recursive type, children: Vec<Node>, child: Option<Child>
    struct Node;

    impl ApiType for Node {
        fn type_ref(registry: &mut TypeRegistry) -> TypeRef {
            if registry.reserve("Node", type_name::<Self>()) {
                let fields = vec![
                    FieldDefinition {
                        name: "children".to_owned(),
                        r#type: <Vec<Node> as ApiType>::type_ref(registry),
                        constraints: Constraints::default(),
                    },
                    FieldDefinition {
                        name: "child".to_owned(),
                        r#type: <Option<Child> as ApiType>::type_ref(registry),
                        constraints: Constraints::default(),
                    },
                ];
                registry.define(TypeDefinition::Struct { name: "Node".to_owned(), fields });
            }
            TypeRef::Ref { name: "Node".to_owned() }
        }
    }

    #[test]
    fn register_types() {
        let mut registry = TypeRegistry::default();
        let type_ref = <Result<Node, String> as ApiType>::optional_type_ref(&mut registry);
        assert_eq!(type_ref, Some(TypeRef::Ref { name: "Node".to_owned() }));
        assert_eq!(<Result<(), String> as ApiType>::optional_type_ref(&mut registry), None);

        let types = registry.into_types();
        assert_eq!(types.len(), 2);
        assert_eq!(types[0].name(), "Child");
        assert_eq!(types[1].name(), "Node");
        let TypeDefinition::Struct { fields, .. } = &types[1] else { panic!("must be struct") };
        assert_eq!(fields[0].r#type, TypeRef::List { item: Box::new(TypeRef::Ref { name: "Node".to_owned() }) });
        assert_eq!(fields[1].r#type, TypeRef::Option { item: Box::new(TypeRef::Ref { name: "Child".to_owned() }) });
    }

    #[test]
    #[should_panic(expected = "duplicate api type name")]
    fn duplicate_type_name() {
        let mut registry = TypeRegistry::default();
        registry.reserve("Child", "a::Child");
        registry.reserve("Child", "b::Child");
    }

    #[test]
    fn serialize_type_ref() {
        let type_ref = TypeRef::Option { item: Box::new(TypeRef::Ref { name: "Child".to_owned() }) };
        assert_eq!(
            serde_json::to_string(&type_ref).unwrap(),
            r#"{"kind":"option","item":{"kind":"ref","name":"Child"}}"#
        );

        let mut registry = TypeRegistry::default();
        assert_eq!(<u64 as ApiType>::type_ref(&mut registry), TypeRef::Primitive { name: "u64".to_owned() });
        assert_eq!(<DateTime as ApiType>::type_ref(&mut registry), TypeRef::Primitive { name: "DateTime".to_owned() });
        assert_eq!(
            serde_json::to_string(&<Vec<Uuid> as ApiType>::type_ref(&mut registry)).unwrap(),
            r#"{"kind":"list","item":{"kind":"Uuid"}}"#
        );
        assert_eq!(
            serde_json::to_string(&<HashMap<String, i64> as ApiType>::type_ref(&mut registry)).unwrap(),
            r#"{"kind":"map","value":{"kind":"i64"}}"#
        );
    }
}
