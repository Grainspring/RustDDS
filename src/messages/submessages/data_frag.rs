use std::io;

use tracing::debug;
use speedy::{Context, Error, Readable, Writable, Writer};
use enumflags2::BitFlags;
use bytes::Bytes;

use crate::{
  messages::submessages::{submessage_elements::parameter_list::ParameterList, submessages::*},
  structure::{
    guid::EntityId,
    sequence_number::{FragmentNumber, SequenceNumber},
  },
};

/// The DataFrag Submessage extends the Data Submessage by enabling the
/// serializedData to be fragmented and sent as multiple DataFrag Submessages.
/// The fragments contained in the DataFrag Submessages are then re-assembled by
/// the RTPS Reader.
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(test, derive(Default))]
pub struct DataFrag {
  /// Identifies the RTPS Reader entity that is being informed of the change
  /// to the data-object.
  pub reader_id: EntityId,

  /// Identifies the RTPS Writer entity that made the change to the
  /// data-object.
  pub writer_id: EntityId,

  /// Uniquely identifies the change and the relative order for all changes
  /// made by the RTPS Writer identified by the writerGuid.
  /// Each change gets a consecutive sequence number.
  /// Each RTPS Writer maintains is own sequence number.
  pub writer_sn: SequenceNumber,

  /// Indicates the starting fragment for the series of fragments in
  /// serialized_data. Fragment numbering starts with number 1.
  pub fragment_starting_num: FragmentNumber,

  /// The number of consecutive fragments contained in this Submessage,
  /// starting at fragment_starting_num.
  pub fragments_in_submessage: u16,

  /// The total size in bytes of the original data before fragmentation.
  pub data_size: u32,

  /// The size of an individual fragment in bytes. The maximum fragment size
  /// equals 64K.
  pub fragment_size: u16,

  /// Contains QoS that may affect the interpretation of the message.
  /// Present only if the InlineQosFlag is set in the header.
  pub inline_qos: Option<ParameterList>,

  /// Encapsulation of a consecutive series of fragments, starting at
  /// fragment_starting_num for a total of fragments_in_submessage.
  /// Represents part of the new value of the data-object
  /// after the change. Present only if either the DataFlag or the KeyFlag are
  /// set in the header. Present only if DataFlag is set in the header.
  ///
  /// Note: RTPS spec says the serialized_payload is of type SerializedPayload,
  /// but that is technically incorrect. It is a fragment of
  /// SerializedPayload. The headers at the beginning of SerializedPayload
  /// appear only at the first fragment. The fragmentation mechanism here
  /// should treat serialized_payload as an opaque stream of bytes.
  pub serialized_payload: Bytes,
}

impl DataFrag {
  // Serialized length of DataFrag submessage without submessage header.
  // This is compatible with the definition of the definition of
  // "octetsToNextHeader" field in RTPS spec v2.5 Section "9.4.5.1 Submessage
  // Header".
  pub fn len_serialized(&self) -> usize {
    2 + // extraFlags (unused in RTPS v2.5)
    2 + // octetsToInlineSos
    4 + // readerId
    4 + // writerId
    8 + // writerSN
    4 + // fragmentStartingNum
    2 + // fragmentsInSubmessage
    2 + // fragmentSize
    4 + // sampleSize
    self.inline_qos.as_ref().map(|q| q.len_serialized() ).unwrap_or(0) + // QoS ParamterList
    self.serialized_payload.len()
  }

  pub fn deserialize(buffer: &Bytes, flags: BitFlags<DATAFRAG_Flags>) -> io::Result<Self> {
    let mut cursor = io::Cursor::new(&buffer);
    let endianness = endianness_flag(flags.bits());
    let map_speedy_err = |p: Error| io::Error::new(io::ErrorKind::Other, p);

    let _extra_flags =
      u16::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor).map_err(map_speedy_err)?;
    let octets_to_inline_qos =
      u16::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor).map_err(map_speedy_err)?;
    let reader_id = EntityId::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor)
      .map_err(map_speedy_err)?;
    let writer_id = EntityId::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor)
      .map_err(map_speedy_err)?;
    let writer_sn = SequenceNumber::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor)
      .map_err(map_speedy_err)?;
    let fragment_starting_num =
      FragmentNumber::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor)
        .map_err(map_speedy_err)?;
    let fragments_in_submessage =
      u16::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor).map_err(map_speedy_err)?;
    let fragment_size =
      u16::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor).map_err(map_speedy_err)?;
    let data_size =
      u32::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor).map_err(map_speedy_err)?;

    let expect_qos = flags.contains(DATAFRAG_Flags::InlineQos);
    //let expect_key = flags.contains(DATAFRAG_Flags::Key);

    // Skip any possible fields we do not know about.
    let rtps_v23_header_size: u16 = 7 * 4;
    let extra_octets = octets_to_inline_qos - rtps_v23_header_size;
    if octets_to_inline_qos < rtps_v23_header_size {
      return Err(io::Error::new(
        io::ErrorKind::Other,
        "DataFrag has too low octetsToInlineQos",
      ));
    }
    cursor.set_position(cursor.position() + u64::from(extra_octets));

    let inline_qos = if expect_qos {
      Some(
        ParameterList::read_from_stream_unbuffered_with_ctx(endianness, &mut cursor)
          .map_err(map_speedy_err)?,
      )
    } else {
      None
    };

    // Payload should be always present, be it data or key fragments.
    let serialized_payload = buffer.clone().split_off(cursor.position() as usize);

    Ok(Self {
      reader_id,
      writer_id,
      writer_sn,
      fragment_starting_num,
      fragments_in_submessage,
      data_size,
      fragment_size,
      inline_qos,
      serialized_payload,
    })
  }
}

impl<C: Context> Writable<C> for DataFrag {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    writer.write_u16(0)?;
    if self.inline_qos.is_some() && !self.inline_qos.as_ref().unwrap().parameters.is_empty() {
      debug!("self.inline_qos {:?}", self.inline_qos);
      todo!()
    } else if self.inline_qos.is_some() && self.inline_qos.as_ref().unwrap().parameters.is_empty()
      || self.inline_qos.is_none()
    {
      writer.write_u16(24)?;
    }
    writer.write_value(&self.reader_id)?;
    writer.write_value(&self.writer_id)?;
    writer.write_value(&self.writer_sn)?;
    writer.write_value(&self.fragment_starting_num)?;
    writer.write_value(&self.fragments_in_submessage)?;
    writer.write_value(&self.fragment_size)?;
    writer.write_value(&self.data_size)?;
    if self.inline_qos.is_some() && !self.inline_qos.as_ref().unwrap().parameters.is_empty() {
      writer.write_value(&self.inline_qos)?;
    }
    writer.write_bytes(&self.serialized_payload)?;
    Ok(())
  }
}
