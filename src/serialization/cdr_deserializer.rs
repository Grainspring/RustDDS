use std::marker::PhantomData;

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
use serde::de::{
  self, DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
  VariantAccess, Visitor,
};
use paste::paste;

use crate::{
  dds::traits::{
    key::Keyed,
    serde_adapters::{no_key, with_key},
  },
  messages::submessages::submessage_elements::serialized_payload::RepresentationIdentifier,
  serialization::error::{Error, Result},
};

/// This type adapts CdrDeserializer (which implements serde::Deserializer) to
/// work as a [`with_key::DeserializerAdapter`] and
/// [`no_key::DeserializerAdapter`].
///
/// CdrDeserializer cannot directly implement
/// the trait itself, because CdrDeserializer has the type parameter BO open,
/// and the adapter needs to be bi-endian.
pub struct CDRDeserializerAdapter<D> {
  phantom: PhantomData<D>,
  // no-one home
}

const REPR_IDS: [RepresentationIdentifier; 3] = [
  RepresentationIdentifier::CDR_BE,
  RepresentationIdentifier::CDR_LE,
  RepresentationIdentifier::PL_CDR_LE,
];

impl<D> no_key::DeserializerAdapter<D> for CDRDeserializerAdapter<D>
where
  D: DeserializeOwned,
{
  fn supported_encodings() -> &'static [RepresentationIdentifier] {
    &REPR_IDS
  }

  fn from_bytes(input_bytes: &[u8], encoding: RepresentationIdentifier) -> Result<D> {
    match encoding {
      RepresentationIdentifier::CDR_LE | RepresentationIdentifier::PL_CDR_LE => {
        deserialize_from_little_endian(input_bytes)
      }
      RepresentationIdentifier::CDR_BE => deserialize_from_big_endian(input_bytes),
      repr_id => Err(Error::Message(format!(
        "Unknown representaiton identifier {:?}.",
        repr_id
      ))),
    }
  }
}

impl<D> with_key::DeserializerAdapter<D> for CDRDeserializerAdapter<D>
where
  D: Keyed + DeserializeOwned,
  <D as Keyed>::K: DeserializeOwned, // Key should do this already?
{
  fn key_from_bytes(input_bytes: &[u8], encoding: RepresentationIdentifier) -> Result<D::K> {
    match encoding {
      RepresentationIdentifier::CDR_LE | RepresentationIdentifier::PL_CDR_LE => {
        deserialize_from_little_endian(input_bytes)
      }
      RepresentationIdentifier::CDR_BE => deserialize_from_big_endian(input_bytes),
      repr_id => Err(Error::Message(format!(
        "Unknown representaiton identifier {:?}.",
        repr_id
      ))),
    }
  }
}

/// a CDR deserializer implementation.
///
/// Input is from &[u8], since we expect to have the data in contiguous memory
/// buffers.
pub struct CdrDeserializer<'de, BO> {
  phantom: PhantomData<BO>, // This field exists only to provide use for BO. See PhantomData docs.
  input: &'de [u8],         /* We borrow the input data, therefore we carry lifetime 'de all
                             * around. */
  serialized_data_count: usize, // This is to keep track of CDR data alignment requirements.
}

impl<'de, BO> CdrDeserializer<'de, BO>
where
  BO: ByteOrder,
{
  pub fn new_little_endian(input: &[u8]) -> CdrDeserializer<LittleEndian> {
    CdrDeserializer::<LittleEndian>::new(input)
  }

  pub fn new_big_endian(input: &[u8]) -> CdrDeserializer<BigEndian> {
    CdrDeserializer::<BigEndian>::new(input)
  }

  pub fn new(input: &'de [u8]) -> CdrDeserializer<'de, BO> {
    CdrDeserializer::<BO> {
      phantom: PhantomData,
      input,
      serialized_data_count: 0,
    }
  }

  /// Read the first bytes in the input.
  fn next_bytes(&mut self, count: usize) -> Result<&[u8]> {
    if count <= self.input.len() {
      let (head, tail) = self.input.split_at(count);
      self.input = tail;
      self.serialized_data_count += count;
      Ok(head)
    } else {
      Err(Error::Eof)
    }
  }

  /// consume and discard bytes
  fn remove_bytes_from_input(&mut self, count: usize) -> Result<()> {
    let _pad = self.next_bytes(count)?;
    Ok(())
  }

  fn calculate_padding_count_from_written_bytes_and_remove(
    &mut self,
    type_octet_aligment: usize,
  ) -> Result<()> {
    let modulo = self.serialized_data_count % type_octet_aligment;
    if modulo == 0 {
      Ok(())
    } else {
      let padding = type_octet_aligment - modulo;
      self.remove_bytes_from_input(padding)
    }
  }
}

pub fn deserialize_from_little_endian<T>(s: &[u8]) -> Result<T>
where
  T: DeserializeOwned,
{
  let mut deserializer = CdrDeserializer::<LittleEndian>::new(s);
  T::deserialize(&mut deserializer)
}

pub fn deserialize_from_big_endian<T>(s: &[u8]) -> Result<T>
where
  T: DeserializeOwned,
{
  let mut deserializer = CdrDeserializer::<BigEndian>::new(s);
  T::deserialize(&mut deserializer)
}

/// macro for writing primitive number deserializers. Rust does not allow
/// declaring a macro inside impl block, so it is here.
macro_rules! deserialize_multibyte_number {
  ($num_type:ident) => {
    paste! {
      fn [<deserialize_ $num_type>]<V>(self, visitor: V) -> Result<V::Value>
      where
        V: Visitor<'de>,
      {
        const SIZE :usize = std::mem::size_of::<$num_type>();
        assert!(SIZE > 1, "multibyte means size must be > 1");
        self.calculate_padding_count_from_written_bytes_and_remove(SIZE)?;
        visitor.[<visit_ $num_type>](
          self.next_bytes(SIZE)?.[<read_ $num_type>]::<BO>().unwrap() )
      }
    }
  };
}

impl<'de, 'a, BO> de::Deserializer<'de> for &'a mut CdrDeserializer<'de, BO>
where
  BO: ByteOrder,
{
  type Error = Error;

  /// CDR serialization is not a self-describing data format, so we cannot
  /// implement this.
  fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    Err(Error::Message(
      "cdr_desrializer: Cannot deserialize \"any\" type. ".to_string(),
    ))
  }

  //15.3.1.5 Boolean
  //  Boolean values are encoded as single octets, where TRUE is the value 1, and
  // FALSE as 0.
  fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    match self.next_bytes(1)?.first().unwrap() {
      0 => visitor.visit_bool(false),
      1 => visitor.visit_bool(true),
      x => Err(Error::BadBoolean(*x)),
    }
  }

  deserialize_multibyte_number!(i16);
  deserialize_multibyte_number!(i32);
  deserialize_multibyte_number!(i64);

  deserialize_multibyte_number!(u16);
  deserialize_multibyte_number!(u32);
  deserialize_multibyte_number!(u64);

  deserialize_multibyte_number!(f32);
  deserialize_multibyte_number!(f64);

  // Single-byte numbers have a bit simpler logic: No alignment, no endianness.
  fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_i8(self.next_bytes(1)?.read_i8().unwrap())
  }

  fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_u8(self.next_bytes(1)?.read_u8().unwrap())
  }

  /// Since this is Rust, a char is 32-bit Unicode codepoint.
  fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.calculate_padding_count_from_written_bytes_and_remove(4)?;
    let codepoint = self.next_bytes(4)?.read_u32::<BO>().unwrap();
    // TODO: Temporary workaround until std::char::from_u32() makes it into stable
    // matched value should be char::from_u32( codepoint )
    match Some(codepoint as u8 as char) {
      Some(c) => visitor.visit_char(c),
      None => Err(Error::BadChar(codepoint)),
    }
  }

  fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    // read string length
    self.calculate_padding_count_from_written_bytes_and_remove(4)?;
    let bytes_len = self.next_bytes(4)?.read_u32::<BO>().unwrap() as usize;

    let bytes = self.next_bytes(bytes_len)?; // length includes null terminator

    let bytes_without_null = &bytes[0..bytes.len() - 1];

    match std::str::from_utf8(bytes_without_null) {
      Ok(s) => visitor.visit_str(s),
      Err(utf8_err) => Err(Error::BadString(utf8_err)),
    }
  }

  fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_str(visitor)
  }

  // Byte strings

  fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_seq(visitor)
  }

  fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_seq(visitor)
  }

  fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.calculate_padding_count_from_written_bytes_and_remove(4)?;
    let enum_tag = self.next_bytes(4)?.read_u32::<BO>().unwrap();
    match enum_tag {
      0 => visitor.visit_none(),
      1 => visitor.visit_some(self),
      wtf => Err(Error::BadOption(wtf)),
    }
  }

  fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    // Unit data is not put on wire, to match behavior with cdr_serializer
    visitor.visit_unit()
  }

  fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_unit(visitor) // This means a named type, which has no
                                   // data.
  }

  fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_newtype_struct(self)
  }

  ///Sequences are encoded as an unsigned long value, followed by the elements
  /// of the
  //sequence. The initial unsigned long contains the number of elements in the
  // sequence. The elements of the sequence are encoded as specified for their
  // type.
  fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.calculate_padding_count_from_written_bytes_and_remove(4)?;
    let element_count = self.next_bytes(4)?.read_u32::<BO>().unwrap() as usize;
    visitor.visit_seq(SequenceHelper::new(self, element_count))
  }

  // if sequence is fixed length array then number of elements is not included
  fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_seq(SequenceHelper::new(self, len))
  }

  fn deserialize_tuple_struct<V>(
    self,
    _name: &'static str,
    len: usize,
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_seq(SequenceHelper::new(self, len))
  }

  fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.calculate_padding_count_from_written_bytes_and_remove(4)?;
    let element_count = self.next_bytes(4)?.read_u32::<BO>().unwrap() as usize;
    visitor.visit_map(SequenceHelper::new(self, element_count))
  }

  fn deserialize_struct<V>(
    self,
    _name: &'static str,
    fields: &'static [&'static str],
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_seq(SequenceHelper::new(self, fields.len()))
  }

  ///Enum values are encoded as unsigned longs. (u32)
  /// The numeric values associated with enum identifiers are determined by the
  /// order in which the identifiers appear in the enum declaration. The first
  /// enum identifier has the numeric value zero (0). Successive enum
  /// identifiers take ascending numeric values, in order of declaration from
  /// left to right.
  fn deserialize_enum<V>(
    self,
    _name: &'static str,
    _variants: &'static [&'static str],
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.calculate_padding_count_from_written_bytes_and_remove(4)?;
    visitor.visit_enum(EnumerationHelper::<BO>::new(self))
  }

  /// An identifier in Serde is the type that identifies a field of a struct or
  /// the variant of an enum. In JSON, struct fields and enum variants are
  /// represented as strings. In other formats they may be represented as
  /// numeric indices.
  fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_u32(visitor)
  }

  fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_any(visitor)
  }
}

// ----------------------------------------------------------

struct EnumerationHelper<'a, 'de: 'a, BO> {
  de: &'a mut CdrDeserializer<'de, BO>,
}

impl<'a, 'de, BO> EnumerationHelper<'a, 'de, BO>
where
  BO: ByteOrder,
{
  fn new(de: &'a mut CdrDeserializer<'de, BO>) -> Self {
    EnumerationHelper::<BO> { de }
  }
}

impl<'de, 'a, BO> EnumAccess<'de> for EnumerationHelper<'a, 'de, BO>
where
  BO: ByteOrder,
{
  type Error = Error;
  type Variant = Self;

  fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
  where
    V: DeserializeSeed<'de>,
  {
    // preceeding deserialize_enum aligned to 4
    let enum_tag = self.de.next_bytes(4)?.read_u32::<BO>().unwrap();
    let val: Result<_> = seed.deserialize(enum_tag.into_deserializer());
    Ok((val?, self))
  }
}

// ----------------------------------------------------------

impl<'de, 'a, BO> VariantAccess<'de> for EnumerationHelper<'a, 'de, BO>
where
  BO: ByteOrder,
{
  type Error = Error;

  fn unit_variant(self) -> Result<()> {
    Ok(())
  }

  fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
  where
    T: DeserializeSeed<'de>,
  {
    seed.deserialize(self.de)
  }

  fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    de::Deserializer::deserialize_tuple(self.de, len, visitor)
  }

  fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    de::Deserializer::deserialize_tuple(self.de, fields.len(), visitor)
  }
}

// ----------------------------------------------------------

struct SequenceHelper<'a, 'de: 'a, BO> {
  de: &'a mut CdrDeserializer<'de, BO>,
  element_counter: usize,
  expected_count: usize,
}

impl<'a, 'de, BO> SequenceHelper<'a, 'de, BO> {
  fn new(de: &'a mut CdrDeserializer<'de, BO>, expected_count: usize) -> Self {
    SequenceHelper {
      de,
      element_counter: 0,
      expected_count,
    }
  }
}

// `SeqAccess` is provided to the `Visitor` to give it the ability to iterate
// through elements of the sequence.
impl<'a, 'de, BO> SeqAccess<'de> for SequenceHelper<'a, 'de, BO>
where
  BO: ByteOrder,
{
  type Error = Error;

  fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
  where
    T: DeserializeSeed<'de>,
  {
    if self.element_counter == self.expected_count {
      Ok(None)
    } else {
      self.element_counter += 1;
      seed.deserialize(&mut *self.de).map(Some)
    }
  }
}

// `MapAccess` is provided to the `Visitor` to give it the ability to iterate
// through entries of the map.
impl<'de, 'a, BO> MapAccess<'de> for SequenceHelper<'a, 'de, BO>
where
  BO: ByteOrder,
{
  type Error = Error;

  fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
  where
    K: DeserializeSeed<'de>,
  {
    if self.element_counter == self.expected_count {
      Ok(None)
    } else {
      self.element_counter += 1;
      seed.deserialize(&mut *self.de).map(Some)
    }
  }

  fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
  where
    V: DeserializeSeed<'de>,
  {
    // Deserialize a map value.
    seed.deserialize(&mut *self.de)
  }
}

#[cfg(test)]
mod tests {
  #![allow(clippy::needless_pass_by_value)]
  use byteorder::{BigEndian, LittleEndian};
  use tracing::info;
  use serde::{Deserialize, Serialize};
  use test_case::test_case;

  use crate::serialization::{
    cdr_deserializer::{deserialize_from_big_endian, deserialize_from_little_endian},
    cdr_serializer::to_bytes,
  };

  #[test]
  fn cdr_deserialization_struct() {
    //IDL
    /*
    struct OmaTyyppi
    {
     octet first;
    octet second;
    long third;
    unsigned long long fourth;
    boolean fifth;
    float sixth;
    boolean seventh;
    sequence<long> eigth;
    sequence<octet> ninth;
    sequence<short> tenth;
    sequence<long long> eleventh;
    unsigned short twelwe [3];
    string thirteen;
    };
    */

    /*

      ser_var.first(1);
    ser_var.second(-3);
    ser_var.third(-5000);
    ser_var.fourth(1234);
    ser_var.fifth(true);
    ser_var.sixth(-6.6);
    ser_var.seventh(true);
    ser_var.eigth({1,2});
    ser_var.ninth({1});
    ser_var.tenth({5,-4,3,-2,1});
    ser_var.eleventh({});
    ser_var.twelwe({3,2,1});
    ser_var.thirteen("abc");

      */
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct OmaTyyppi {
      first_value: u8,
      second_value: i8,
      third_value: i32,
      fourth_value: u64,
      fifth: bool,
      sixth: f32,
      seventh: bool,
      eigth: Vec<i32>,
      ninth: Vec<u8>,
      tenth: Vec<i16>,
      eleventh: Vec<i64>,
      twelwe: [u16; 3],
      thirteen: String,
    }

    let mikki_hiiri = OmaTyyppi {
      first_value: 1,
      second_value: -3,
      third_value: -5000,
      fourth_value: 1234u64,
      fifth: true,
      sixth: -6.6f32,
      seventh: true,
      eigth: vec![1, 2],
      ninth: vec![1],
      tenth: vec![5, -4, 3, -2, 1],
      eleventh: vec![],
      twelwe: [3, 2, 1],
      thirteen: "abc".to_string(),
    };

    let expected_serialized_result: Vec<u8> = vec![
      0x01, 0xfd, 0x00, 0x00, 0x78, 0xec, 0xff, 0xff, 0xd2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x33, 0x33, 0xd3, 0xc0, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00,
      0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
      0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x05, 0x00, 0xfc, 0xff, 0x03, 0x00, 0xfe, 0xff,
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x02, 0x00, 0x01, 0x00, 0x00,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63, 0x00,
    ];

    let sarjallistettu = to_bytes::<OmaTyyppi, LittleEndian>(&mikki_hiiri).unwrap();

    for x in 0..expected_serialized_result.len() {
      if expected_serialized_result[x] != sarjallistettu[x] {
        info!("index: {}", x);
      }
    }
    assert_eq!(sarjallistettu, expected_serialized_result);
    info!("serialization successfull!");

    let rakennettu: OmaTyyppi =
      deserialize_from_little_endian(&expected_serialized_result).unwrap();
    assert_eq!(rakennettu, mikki_hiiri);
    info!("deserialized: {:?}", rakennettu);
  }

  #[test]

  fn cdr_deserialization_user_defined_data() {
    // look this example https://www.omg.org/spec/DDSI-RTPS/2.3/PDF
    //10.7 Example for User-defined Topic Data
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct ShapeType {
      color: String,
      x: i32,
      y: i32,
      size: i32,
    }

    let message = ShapeType {
      color: "BLUE".to_string(),
      x: 34,
      y: 100,
      size: 24,
    };

    let expected_serialized_result: Vec<u8> = vec![
      0x05, 0x00, 0x00, 0x00, 0x42, 0x4c, 0x55, 0x45, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00,
      0x00, 0x64, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00,
    ];

    let serialized = to_bytes::<ShapeType, LittleEndian>(&message).unwrap();
    assert_eq!(serialized, expected_serialized_result);
    let deserialized_message: ShapeType = deserialize_from_little_endian(&serialized).unwrap();
    assert_eq!(deserialized_message, message);
  }

  #[test]

  fn cdr_deserialization_serialization_topic_name() {
    // look this example https://www.omg.org/spec/DDSI-RTPS/2.3/PDF
    //10.6 Example for Built-in Endpoint Data
    // this is just CRD topic name strings

    // TODO what about padding??
    let received_cdr_string: Vec<u8> = vec![
      0x07, 0x00, 0x00, 0x00, 0x053, 0x71, 0x75, 0x61, 0x72, 0x65, 0x00, /* 0x00, */
    ];

    let deserialized_message: String =
      deserialize_from_little_endian(&received_cdr_string).unwrap();
    info!("{:?}", deserialized_message);
    assert_eq!("Square", deserialized_message);

    let received_cdr_string2: Vec<u8> = vec![
      0x0A, 0x00, 0x00, 0x00, 0x53, 0x68, 0x61, 0x70, 0x65, 0x54, 0x79, 0x70, 0x65,
      0x00, /* 0x00, 0x00, */
    ];

    let deserialized_message2: String =
      deserialize_from_little_endian(&received_cdr_string2).unwrap();
    info!("{:?}", deserialized_message2);
    assert_eq!("ShapeType", deserialized_message2);
  }

  #[test]
  fn cdr_deserialization_example_struct() {
    // look this example https://www.omg.org/spec/DDSI-RTPS/2.2/PDF
    // 10.2.2 Example

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct Example {
      a: u32,
      b: [u8; 4],
    }

    let o = Example {
      a: 1,
      b: [b'a', b'b', b'c', b'd'],
    };

    let serialized_le: Vec<u8> = vec![0x01, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63, 0x64];

    let serialized_be: Vec<u8> = vec![0x00, 0x00, 0x00, 0x01, 0x61, 0x62, 0x63, 0x64];

    let deserialized_le: Example = deserialize_from_little_endian(&serialized_le).unwrap();
    let deserialized_be: Example = deserialize_from_big_endian(&serialized_be).unwrap();
    let serialized_o_le = to_bytes::<Example, LittleEndian>(&o).unwrap();
    let serialized_o_be = to_bytes::<Example, BigEndian>(&o).unwrap();

    assert_eq!(
      serialized_o_le,
      vec![0x01, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63, 0x64,]
    );

    assert_eq!(
      serialized_o_be,
      vec![0x00, 0x00, 0x00, 0x01, 0x61, 0x62, 0x63, 0x64,]
    );

    info!("serialization success");

    assert_eq!(deserialized_le, o);
    assert_eq!(deserialized_be, o);
    info!("deserialition success");
  }

  #[test]

  fn cdr_deserialization_serialization_payload_shapes() {
    // This test uses wireshark captured shapes demo part of serialized message as
    // received_message.
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct ShapeType {
      color: String,
      x: i32,
      y: i32,
      size: i32,
    }
    // this message is DataMessages serialized data withoutt encapsulation kind and
    // encapsulation options
    let received_message: Vec<u8> = vec![
      0x04, 0x00, 0x00, 0x00, 0x52, 0x45, 0x44, 0x00, 0x61, 0x00, 0x00, 0x00, 0x1b, 0x00, 0x00,
      0x00, 0x1e, 0x00, 0x00, 0x00,
    ];
    let received_message2: Vec<u8> = vec![
      0x04, 0x00, 0x00, 0x00, 0x52, 0x45, 0x44, 0x00, 0x61, 0x00, 0x00, 0x00, 0x1b, 0x00, 0x00,
      0x00, 0x1e, 0x00, 0x00, 0x00,
    ];

    let deserialized_message: ShapeType =
      deserialize_from_little_endian(&received_message).unwrap();
    info!("{:?}", deserialized_message);

    let serialized_message = to_bytes::<ShapeType, LittleEndian>(&deserialized_message).unwrap();

    assert_eq!(serialized_message, received_message2);
    //assert_eq!(deserialized_message,received_message)
  }

  #[test]

  fn cdr_deserialization_custom_data_message_from_ros_and_wireshark() {
    // IDL of messsage
    //float64 x
    //float64 y
    //float64 heading
    //float64 v_x
    //float64 v_y
    //float64 kappa
    //string test

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct MessageType {
      x: f64,
      y: f64,
      heading: f64,
      v_x: f64,
      v_y: f64,
      kappa: f64,
      test: String,
    }

    let received_message_le: Vec<u8> = vec![
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1f, 0x85, 0xeb, 0x51, 0xb8,
      0x1e, 0xd5, 0x3f, 0x0a, 0x00, 0x00, 0x00, 0x54, 0x6f, 0x69, 0x6d, 0x69, 0x69, 0x6b, 0x6f,
      0x3f, 0x00, //0x00, 0x00,
    ];

    let value: MessageType = deserialize_from_little_endian(&received_message_le).unwrap();
    info!("{:?}", value);
    assert_eq!(value.test, "Toimiiko?");
  }

  #[derive(Serialize, Deserialize, Debug, PartialEq)]
  struct InterestingMessage {
    unbounded_string: String,
    x: i32,
    y: i32,
    shapesize: i32,
    liuku: f32,
    tuplaliuku: f64,
    kolme_lyhytta: [u16; 3],
    nelja_lyhytta: [i16; 4],
    totuusarvoja: Vec<bool>,
    kolm_tavua: Vec<u8>,
  }

  #[derive(Serialize, Deserialize, Debug, PartialEq)]
  enum BigEnum {
    Interesting(InterestingMessage),
    Boring,
    Something { x: f32, y: f32 },
  }

  #[test]
  fn cdr_deserialization_custom_type() {
    // IDL Definition of message:
    /*struct InterestingMessage
    {
    string unbounded_string;
      long x;
      long y;
      long shapesize;
      float liuku;
      double tuplaliuku;
      unsigned short kolme_lyhytta [3];
      short nelja_lyhytta [4];
      sequence<boolean> totuusarvoja;
      sequence<octet,3> kolm_tavua;
    };
    */

    // values put to serilization message with eprosima fastbuffers
    /*
      ser_var.unbounded_string("tassa on aika pitka teksti");
      ser_var.x(1);
      ser_var.x(2);
      ser_var.y(-3);
      ser_var.shapesize(-4);
      ser_var.liuku(5.5);
      ser_var.tuplaliuku(-6.6);
      std::array<uint16_t, 3> foo  = {1,2,3};
      ser_var.kolme_lyhytta(foo);
      std::array<int16_t, 4>  faa = {1,-2,-3,4};
      ser_var.nelja_lyhytta(faa);
      ser_var.totuusarvoja({true,false,true});
      ser_var.kolm_tavua({23,0,2});
    */

    let value = InterestingMessage {
      unbounded_string: "Tassa on aika pitka teksti".to_string(),
      x: 2,
      y: -3,
      shapesize: -4,
      liuku: 5.5,
      tuplaliuku: -6.6,
      kolme_lyhytta: [1, 2, 3],
      nelja_lyhytta: [1, -2, -3, 4],
      totuusarvoja: vec![true, false, true],
      kolm_tavua: [23, 0, 2].to_vec(),
    };

    const DATA: &[u8] = &[
      0x1b, 0x00, 0x00, 0x00, 0x54, 0x61, 0x73, 0x73, 0x61, 0x20, 0x6f, 0x6e, 0x20, 0x61, 0x69,
      0x6b, 0x61, 0x20, 0x70, 0x69, 0x74, 0x6b, 0x61, 0x20, 0x74, 0x65, 0x6b, 0x73, 0x74, 0x69,
      0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xfd, 0xff, 0xff, 0xff, 0xfc, 0xff, 0xff, 0xff, 0x00,
      0x00, 0xb0, 0x40, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0xc0, 0x01, 0x00, 0x02, 0x00,
      0x03, 0x00, 0x01, 0x00, 0xfe, 0xff, 0xfd, 0xff, 0x04, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
      0x00, 0x01, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x17, 0x00, 0x02,
    ];

    let serialization_result_le = to_bytes::<InterestingMessage, LittleEndian>(&value).unwrap();

    assert_eq!(serialization_result_le, DATA);
    info!("serialization success!");
    let deserialization_result: InterestingMessage = deserialize_from_little_endian(DATA).unwrap();

    info!("{:?}", deserialization_result);
  }

  #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
  enum SomeTupleEnum {
    A(i32),
    B(i32),
    C(i32),
  }

  #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
  enum MixedEnum {
    A(i32),
    B { value: i32 },
    C(i32, i32),
  }

  #[test_case(35_u8 ; "u8")]
  #[test_case(35_u16 ; "u16")]
  #[test_case(352323_u32 ; "u32")]
  #[test_case(352323232_u64 ; "u64")]
  #[test_case(-3_i8 ; "i8")]
  #[test_case(-3_i16 ; "i16")]
  #[test_case(-323232_i32 ; "i32")]
  #[test_case(-3232323434_i64 ; "i64")]
  #[test_case(true)]
  #[test_case(false)]
  #[test_case(2.35_f32 ; "f32")]
  #[test_case(278.35_f64 ; "f64")]
  #[test_case('a' ; "char")]
  #[test_case("BLUE".to_string() ; "string")]
  #[test_case(vec![1_i32, -2_i32, 3_i32] ; "Vec<i32>")]
  #[test_case(InterestingMessage {
      unbounded_string: "Tässä on aika pitkä teksti".to_string(),
      x: 2,
      y: -3,
      shapesize: -4,
      liuku: 5.5,
      tuplaliuku: -6.6,
      kolme_lyhytta: [1, 2, 3],
      nelja_lyhytta: [1, -2, -3, 4],
      totuusarvoja: vec![true, false, true],
      kolm_tavua: [23, 0, 2].to_vec(),
    } ; "InterestingMessage")]
  #[test_case( BigEnum::Boring ; "BigEnum::Boring")]
  #[test_case( BigEnum::Interesting(InterestingMessage {
      unbounded_string: "Tässä on aika pitkä teksti".to_string(),
      x: 2,
      y: -3,
      shapesize: -4,
      liuku: 5.5,
      tuplaliuku: -6.6,
      kolme_lyhytta: [1, 2, 3],
      nelja_lyhytta: [1, -2, -3, 4],
      totuusarvoja: vec![true, false, true],
      kolm_tavua: [23, 0, 2].to_vec(),
    }) ; "BigEnum::Interesting")]
  #[test_case( BigEnum::Something{ x:123.0, y:-0.1 } ; "BigEnum::Something")]
  #[test_case( SomeTupleEnum::A(123) ; "SomeTupleEnum::A")]
  #[test_case( SomeTupleEnum::B(1234) ; "SomeTupleEnum::B")]
  #[test_case( SomeTupleEnum::C(-1) ; "SomeTupleEnum::C")]
  #[test_case( MixedEnum::A(123) ; "MixedEnum::A")]
  #[test_case( MixedEnum::B{ value:1234 } ; "MixedEnum::B")]
  #[test_case( MixedEnum::C(42,43) ; "MixedEnum::C")]
  fn cdr_serde_round_trip<T>(input: T)
  where
    T: PartialEq + std::fmt::Debug + Serialize + for<'a> Deserialize<'a>,
  {
    let serialized = to_bytes::<_, LittleEndian>(&input).unwrap();
    println!("Serialized data: {:x?}", &serialized);
    let deserialized = deserialize_from_little_endian(&serialized).unwrap();
    assert_eq!(input, deserialized);
  }

  /*
  #[test]
  fn cdr_deserialization_bytes(){
    let mut buf = B::with_capacity(1024);
    buf.put(&b"hello world"[..]);
    buf.put_u16(1234);

    let ubuf = buf.into(u8);
    let mut serialized = to_little_endian_binary(&ubuf).unwrap();
    let deserialized : Vec<u8> = deserialize_from_little_endian(&mut serialized).unwrap();

  }
  */
}
