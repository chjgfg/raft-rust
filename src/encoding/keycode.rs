//! Keycode 是一种保持字典序的二进制键编码，用于键值存储中的键。
//! 设计目标是简单而非高效（不使用 varint 等压缩方法）。
//! 顺序很重要：它支持对键空间局部扫描，例如扫描某张表，或按索引范围谓词
//! 如 `WHERE id < 100` 扫描。当键已按目标顺序排列时（如 Raft 日志）也可避免额外排序。
//! 编码不是自描述的：调用方必须提供具体类型来解码，且二进制键必须符合该类型结构。
//! Keycode 支持如下原始类型子集，编码规则为：
//! * [`bool`]：`false` 为 `0x00`，`true` 为 `0x01`。
//! * [`u64`]：大端二进制。
//! * [`i64`]：大端二进制，符号位翻转。
//! * [`f64`]：大端二进制，符号位翻转；负值时其余位也翻转。
//! * [`Vec<u8>`]：`0x00` 转义为 `0x00ff`，以 `0x0000` 结尾。
//! * [`String`]：与 [`Vec<u8>`] 相同。
//! * 序列：所含元素直接拼接，无其它结构。
//! * 枚举：变体索引为 [`u8`]，后接内容序列。
//! 规范的键表示是枚举。例如：
//! ```ignore
//! #[derive(Debug, Deserialize, Serialize)]
//! enum Key {
//!     Foo,
//!     Bar(String),
//!     Baz(bool, u64, #[serde(with = "serde_bytes")] Vec<u8>),
//! }
//! ```
//! 注意：`Vec<u8>` 这类字节串必须用 [`serde_bytes::ByteBuf`] 包装，
//! 或使用 `#[serde(with="serde_bytes")]` 属性。参见 <https://github.com/serde-rs/bytes>。

use std::ops::Bound;

use itertools::Either;
use serde::de::{
    Deserialize, DeserializeSeed, EnumAccess, IntoDeserializer as _, SeqAccess, VariantAccess,
    Visitor,
};
use serde::ser::{Impossible, Serialize, SerializeSeq, SerializeTuple, SerializeTupleVariant};

use crate::errdata;
use crate::error::{Error, Result};

/// 将键序列化为二进制 Keycode 表示。
/// 常见情况下，编码后的键仅在一次存储引擎调用中借用后即丢弃。
/// 也可以传入可复用字节向量编码并返回引用，以减少分配，
/// 但为简单起见不这么做。

pub fn serialize<T: Serialize>(key: &T) -> Vec<u8> {
    let mut serializer = Serializer { output: Vec::new() };
    // 失败则 panic：说明数据结构本身有问题。
    key.serialize(&mut serializer).expect("key must be serializable");
    serializer.output
}

/// 从二进制 Keycode 表示反序列化键。
pub fn deserialize<'a, T: Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let mut deserializer = Deserializer::from_bytes(input);
    let t = T::deserialize(&mut deserializer)?;
    if !deserializer.input.is_empty() {
        return errdata!(
            "unexpected trailing bytes {:x?} at end of key {input:x?}",
            deserializer.input,
        );
    }
    Ok(t)
}

/// 为键前缀生成键范围，用于前缀扫描等。
/// 排他上界通过对最后一个字节加 1 得到。若末尾字节为 0xff（加 1 会溢出），
/// 则找到最后一个非 0xff 字节加 1 并截断其后。若全部为 0xff，则扫描到范围末尾，
/// 因为其后不可能再有其它前缀。


pub fn prefix_range(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = Bound::Included(prefix.to_vec());
    let end = match prefix.iter().rposition(|&b| b != 0xff) {
        Some(i) => Bound::Excluded(
            prefix.iter().take(i).copied().chain(std::iter::once(prefix[i] + 1)).collect(),
        ),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// 将键序列化为二进制字节向量。
struct Serializer {
    output: Vec<u8>,
}

impl serde::ser::Serializer for &mut Serializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleVariant = Self;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Impossible<(), Error>;
    type SerializeStructVariant = Impossible<(), Error>;

    /// bool：true 用 1，false 用 0。
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.output.push(if v { 1 } else { 0 });
        Ok(())
    }

    fn serialize_i8(self, _: i8) -> Result<()> {
        unimplemented!()
    }

    fn serialize_i16(self, _: i16) -> Result<()> {
        unimplemented!()
    }

    fn serialize_i32(self, _: i32) -> Result<()> {
        unimplemented!()
    }

    /// i64 使用大端补码，但翻转最左符号位，使负数排在正数之前。
    /// 其余位的相对顺序本身正确：最大负整数 -1 编码为 01111111...11111111，
    /// 排在所有其它负数之后、正数之前。


    fn serialize_i64(self, v: i64) -> Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // 翻转符号位
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_u8(self, _: u8) -> Result<()> {
        unimplemented!()
    }

    fn serialize_u16(self, _: u16) -> Result<()> {
        unimplemented!()
    }

    fn serialize_u32(self, _: u32) -> Result<()> {
        unimplemented!()
    }

    /// u64 直接使用大端编码。
    fn serialize_u64(self, v: u64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, _: f32) -> Result<()> {
        unimplemented!()
    }

    /// f64 以大端 IEEE 754 编码，但翻转符号位使正数排在负数之后；
    /// 对负数还翻转其余所有位，使其从小到大排列。NaN 排在最后。


    fn serialize_f64(self, v: f64) -> Result<()> {
        let mut bytes = v.to_be_bytes();
        match v.is_sign_negative() {
            false => bytes[0] ^= 1 << 7, // 正数：翻转符号位
            true => bytes.iter_mut().for_each(|b| *b = !*b), // 负数：翻转所有位
        }
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_char(self, _: char) -> Result<()> {
        unimplemented!()
    }

    // 字符串按字节编码。
    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    // 字节切片以 0x0000 结尾，并将 0x00 转义为 0x00ff。
    // 这样可检测结尾，且对前缀重叠的切片，较短者排在较长者之前。

    // 不能使用长度前缀编码，因为它不能正确排序。
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        let bytes = v
            .iter()
            .flat_map(|&byte| match byte {
                0x00 => Either::Left([0x00, 0xff].into_iter()),
                byte => Either::Right([byte].into_iter()),
            })
            .chain([0x00, 0x00]);
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        unimplemented!()
    }

    /// 枚举变体按其索引序列化为单个字节。
    fn serialize_unit_variant(self, _: &'static str, index: u32, _: &'static str) -> Result<()> {
        self.output.push(index.try_into()?);
        Ok(())
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> Result<()> {
        unimplemented!()
    }

    /// newtype 变体按变体索引加内部类型序列化。
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()> {
        self.serialize_unit_variant(name, index, variant)?;
        value.serialize(self)
    }

    /// 序列序列化为各元素编码的拼接。
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    /// 元组序列化为各元素编码的拼接。
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    /// 元组变体按变体索引与
    /// 各元素编码的拼接进行序列化。
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        unimplemented!()
    }
}

/// 序列仅拼接各元素编码，无额外外部结构。
impl SerializeSeq for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

/// 元组与序列相同，仅拼接各元素编码。
impl SerializeTuple for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

/// 元组与序列相同，仅拼接各元素编码。
impl SerializeTupleVariant for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

/// 将字节切片反序列化为给定类型的键。格式不是自描述的，
/// 调用方必须提供具体类型以进行反序列化。

pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    /// 为字节切片创建反序列化器。
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    /// 截取并返回字节切片的下 len 个字节；不足则报错。
    /// there aren't enough bytes left.
    fn take_bytes(&mut self, len: usize) -> Result<&[u8]> {
        if self.input.len() < len {
            return errdata!("insufficient bytes, expected {len} bytes for {:x?}", self.input);
        }
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        Ok(bytes)
    }

    /// 解码并截取下一段已编码的字节切片。
    fn decode_next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut decoded = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,        // terminator
                    Some((_, 0xff)) => decoded.push(0x00), // escaped 0x00
                    _ => return errdata!("invalid escape sequence"),
                },
                Some((_, b)) => decoded.push(*b),
                None => return errdata!("unexpected end of input"),
            }
        };
        self.input = &self.input[taken..];
        Ok(decoded)
    }
}

/// 序列化格式细节参见 Serializer。
impl<'de> serde::de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        panic!("must provide type, Keycode is not self-describing")
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(match self.take_bytes(1)?[0] {
            0x00 => false,
            0x01 => true,
            b => return errdata!("invalid boolean value {b}"),
        })
    }

    fn deserialize_i8<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i16<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7; // 翻转符号位
        visitor.visit_i64(i64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u16<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(u64::from_be_bytes(self.take_bytes(8)?.try_into()?))
    }

    fn deserialize_f32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let mut bytes = self.take_bytes(8)?.to_vec();
        match bytes[0] >> 7 {
            0 => bytes.iter_mut().for_each(|b| *b = !*b), // negative, flip all bits
            1 => bytes[0] ^= 1 << 7,                      // positive, flip sign bit
            _ => panic!("bits can only be 0 or 1"),
        }
        visitor.visit_f64(f64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_char<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_string(String::from_utf8(bytes)?)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let bytes = self.decode_next_bytes()?;
        visitor.visit_byte_buf(bytes)
    }

    fn deserialize_option<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(self, _: &'static str, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_map<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        unimplemented!()
    }
}

/// 序列一直反序列化到字节切片耗尽为止。
impl<'de> SeqAccess<'de> for Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.input.is_empty() {
            return Ok(None);
        }
        seed.deserialize(self).map(Some)
    }
}

/// Enum variants are deserialized by their index.
impl<'de> EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let index = self.take_bytes(1)?[0] as u32;
        let value: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((value?, self))
    }
}

/// Enum variant contents are deserialized as sequences.
impl<'de> VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn struct_variant<V: Visitor<'de>>(self, _: &'static [&'static str], _: V) -> Result<V::Value> {
        unimplemented!()
    }
}

