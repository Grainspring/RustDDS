use std::sync::Arc;

use crate::structure::guid::GUID;
use crate::structure::instance_handle::InstanceHandle;
use crate::structure::sequence_number::SequenceNumber;
use crate::dds::ddsdata::DDSData;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Clone)]
pub enum ChangeKind {
  ALIVE,
  NOT_ALIVE_DISPOSED,
  NOT_ALIVE_UNREGISTERED,
}

#[derive(Debug, Clone)]
pub struct CacheChange {
  pub kind: ChangeKind,
  pub writer_guid: GUID,
  pub instance_handle: InstanceHandle,
  pub sequence_number: SequenceNumber,
  pub data_value: Option<Arc<DDSData>>,
  //pub inline_qos: ParameterList,
}

impl PartialEq for CacheChange {
  fn eq(&self, other: &Self) -> bool {
    let dataeq = match &self.data_value {
      Some(d1) => match &other.data_value {
        Some(d2) => **d1 == **d2,
        None => false,
      },
      None => match other.data_value {
        Some(_) => false,
        None => true,
      },
    };

    self.kind == other.kind
      && self.writer_guid == other.writer_guid
      && self.instance_handle == other.instance_handle
      && self.sequence_number == other.sequence_number
      && dataeq
  }
}

impl CacheChange {
  pub fn new(
    writer_guid: GUID,
    sequence_number: SequenceNumber,
    data_value: Option<Arc<DDSData>>,
  ) -> CacheChange {
    CacheChange {
      kind: ChangeKind::ALIVE,
      writer_guid,
      instance_handle: InstanceHandle::default(),
      sequence_number,
      data_value,
      //inline_qos: ParameterList::new(),
    }
  }
}

impl Default for CacheChange {
  fn default() -> Self {
    CacheChange::new(GUID::default(), SequenceNumber::default(), None)
  }
}
