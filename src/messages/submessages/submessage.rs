use crate::messages::submessages::ack_nack::AckNack;
use crate::messages::submessages::data::Data;
use crate::messages::submessages::data_frag::DataFrag;
use crate::messages::submessages::gap::Gap;
use crate::messages::submessages::heartbeat::Heartbeat;
use crate::messages::submessages::heartbeat_frag::HeartbeatFrag;
use crate::messages::submessages::info_destination::InfoDestination;
use crate::messages::submessages::info_reply::InfoReply;
use crate::messages::submessages::info_source::InfoSource;
use crate::messages::submessages::info_timestamp::InfoTimestamp;
use crate::messages::submessages::nack_frag::NackFrag;
use crate::messages::submessages::submessage_flag::*;

use speedy::{Writable, Writer, Context};
use enumflags2::BitFlags;

//TODO: These messagesa restructured a bit oddly. Why is flags separate from the submessage proper?

#[derive(Debug, PartialEq)]
pub enum EntitySubmessage {
  AckNack(AckNack, BitFlags<Submessage_ACKNACK_Flags>),
  Data(Data, BitFlags<Submessage_DATA_Flags>),
  DataFrag(DataFrag, BitFlags<Submessage_DATAFRAG_Flags>),
  Gap(Gap, BitFlags<Submessage_GAP_Flags>),
  Heartbeat(Heartbeat, BitFlags<Submessage_HEARTBEAT_Flags>),
  HeartbeatFrag(HeartbeatFrag, BitFlags<Submessage_HEARTBEATFRAG_Flags>),
  NackFrag(NackFrag, BitFlags<Submessage_NACKFRAG_Flags>),
}

// we must write this manually, because
// 1) we cannot implement Writable for *Flags defined using enumflags2, as they are foreign types (coherence rules)
// 2) Writer should not use any enum variant tag in this type, as we have SubmessageHeader already.
impl<C:Context> Writable<C> for EntitySubmessage {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    match self {
      EntitySubmessage::AckNack(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      EntitySubmessage::Data(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      EntitySubmessage::DataFrag(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      EntitySubmessage::Gap(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      EntitySubmessage::Heartbeat(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      EntitySubmessage::HeartbeatFrag(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      EntitySubmessage::NackFrag(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }        
    }
  }
}

impl EntitySubmessage {
  pub fn get_data_submessage(&self) -> Option<&Data> {
    match self {
      EntitySubmessage::Data(data, _) => Some(data),
      _ => None,
    }
  }
  /*
  pub fn get_submessage_flag(&self) -> Option<&SubmessageFlag> {
    match self {
      EntitySubmessage::AckNack(_, flag) => Some(flag),
      EntitySubmessage::Data(_, flag) => Some(flag),
      EntitySubmessage::DataFrag(_, flag) => Some(flag),
      EntitySubmessage::Heartbeat(_, flag) => Some(flag),
      _ => None,
    }
  } */
}

#[derive(Debug, PartialEq)]
pub enum InterpreterSubmessage {
  InfoSource(InfoSource, BitFlags<Submessage_INFOSOURCE_Flags>),
  InfoDestination(InfoDestination, BitFlags<Submessage_INFODESTINATION_Flags>),
  InfoReply(InfoReply, BitFlags<Submessage_INFOREPLY_Flags>),
  InfoTimestamp(InfoTimestamp, BitFlags<Submessage_INFOTIMESTAMP_Flags>),
  //Pad(Pad), // Pad message does not need to be processed above serialization layer
}

// See notes on impl Writer for EntitySubmessage
impl<C:Context> Writable<C> for InterpreterSubmessage {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    match self {
      InterpreterSubmessage::InfoSource(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      InterpreterSubmessage::InfoDestination(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      InterpreterSubmessage::InfoReply(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
      InterpreterSubmessage::InfoTimestamp(s,f) => { writer.write_value(s)?; writer.write_u8( f.bits() ) }
    }
  }
}
