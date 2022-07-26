use std::{
  collections::{BTreeMap, BTreeSet},
  fmt, iter,
  rc::Rc,
  sync::{Arc, RwLock},
  time::Duration as StdDuration,
};

use mio::Token;
use mio_extras::{channel as mio_channel, timer::Timer};
use tracing::{debug, error, info, trace, warn};
use enumflags2::BitFlags;
use speedy::{Endianness, Writable};

use crate::{
  dds::{
    ddsdata::DDSData,
    message_receiver::MessageReceiverState,
    qos::{policy, HasQoSPolicy, QosPolicies},
    rtps_writer_proxy::RtpsWriterProxy,
    statusevents::{CountWithChange, DataReaderStatus},
    with_key::datawriter::{WriteOptions, WriteOptionsBuilder},
  },
  messages::{
    header::Header,
    protocol_id::ProtocolId,
    protocol_version::ProtocolVersion,
    submessages::{submessage_elements::parameter_list::ParameterList, submessages::*},
    vendor_id::VendorId,
  },
  network::udp_sender::UDPSender,
  serialization::message::Message,
  structure::{
    cache_change::{CacheChange, ChangeKind},
    dds_cache::DDSCache,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    locator::Locator,
    sequence_number::{FragmentNumberSet, SequenceNumber, SequenceNumberSet},
    time::Timestamp,
  },
};
use super::{qos::InlineQos, with_key::datareader::ReaderCommand};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimedEvent {
  DeadlineMissedCheck,
}

// Some pieces necessary to contruct a reader.
// These can be sent between threads, whereas a Reader cannot.
pub(crate) struct ReaderIngredients {
  pub guid: GUID,
  pub notification_sender: mio_channel::SyncSender<()>,
  pub status_sender: mio_channel::SyncSender<DataReaderStatus>,
  pub topic_name: String,
  pub qos_policy: QosPolicies,
  pub data_reader_command_receiver: mio_channel::Receiver<ReaderCommand>,
}

impl ReaderIngredients {
  pub fn alt_entity_token(&self) -> Token {
    self.guid.entity_id.as_alt_token()
  }
}

impl fmt::Debug for ReaderIngredients {
  // Need manual implementation, because channels cannot be Dubug formatted.
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Reader")
      .field("my_guid", &self.guid)
      .field("topic_name", &self.topic_name)
      .field("qos_policy", &self.qos_policy)
      .finish()
  }
}

pub(crate) struct Reader {
  // Should the instant be sent?
  notification_sender: mio_channel::SyncSender<()>,
  status_sender: mio_channel::SyncSender<DataReaderStatus>,
  udp_sender: Rc<UDPSender>,

  is_stateful: bool, // is this StatefulReader or Statelessreader as per RTPS spec
  // Currently we support only stateful behaviour.
  // RTPS Spec: Section 8.4.11.2 Reliable StatelessReader Behavior
  // "This combination is not supported by the RTPS protocol."
  // So stateful must be true whenever we are Reliable.
  reliability: policy::Reliability,
  dds_cache: Arc<RwLock<DDSCache>>,

  #[cfg(test)]
  seqnum_instant_map: BTreeMap<SequenceNumber, Timestamp>,

  topic_name: String,
  qos_policy: QosPolicies,

  my_guid: GUID,

  heartbeat_response_delay: StdDuration,

  // TODO: Implement (use) this
  #[allow(dead_code)]
  heartbeat_supression_duration: StdDuration,

  received_hearbeat_count: i32,

  matched_writers: BTreeMap<GUID, RtpsWriterProxy>,
  writer_match_count_total: i32, // total count, never decreases

  requested_deadline_missed_count: i32,
  offered_incompatible_qos_count: i32,

  pub(crate) timed_event_timer: Timer<TimedEvent>,
  pub(crate) data_reader_command_receiver: mio_channel::Receiver<ReaderCommand>,
}

impl Reader {
  pub fn new(
    i: ReaderIngredients,
    dds_cache: Arc<RwLock<DDSCache>>,
    udp_sender: Rc<UDPSender>,
    timed_event_timer: Timer<TimedEvent>,
  ) -> Self {
    Self {
      notification_sender: i.notification_sender,
      status_sender: i.status_sender,
      udp_sender,
      is_stateful: true, // Do not change this before stateless functionality is implemented.

      reliability: i
        .qos_policy
        .reliability() // use qos specification
        .unwrap_or(policy::Reliability::BestEffort), // or default to BestEffort
      dds_cache,
      topic_name: i.topic_name,
      qos_policy: i.qos_policy,

      #[cfg(test)]
      seqnum_instant_map: BTreeMap::new(),
      my_guid: i.guid,

      heartbeat_response_delay: StdDuration::new(0, 500_000_000), // 0,5sec
      heartbeat_supression_duration: StdDuration::new(0, 0),
      received_hearbeat_count: 0,
      matched_writers: BTreeMap::new(),
      writer_match_count_total: 0,
      requested_deadline_missed_count: 0,
      offered_incompatible_qos_count: 0,
      timed_event_timer,
      data_reader_command_receiver: i.data_reader_command_receiver,
    }
  }
  // TODO: check if it's necessary to implement different handlers for discovery
  // and user messages

  /// To know when token represents a reader we should look entity attribute
  /// kind
  pub fn entity_token(&self) -> Token {
    self.guid().entity_id.as_token()
  }

  pub fn set_requested_deadline_check_timer(&mut self) {
    if let Some(deadline) = self.qos_policy.deadline {
      debug!(
        "GUID={:?} set_requested_deadline_check_timer: {:?}",
        self.my_guid,
        deadline.0.to_std()
      );
      self
        .timed_event_timer
        .set_timeout(deadline.0.to_std(), TimedEvent::DeadlineMissedCheck);
    } else {
      trace!(
        "GUID={:?} - no deaadline policy - do not set set_requested_deadline_check_timer",
        self.my_guid
      );
    }
  }

  #[tracing::instrument(level = "trace")]
  pub fn send_status_change(&self, change: DataReaderStatus) {
    match self.status_sender.try_send(change) {
      Ok(()) => (), // expected result
      Err(mio_channel::TrySendError::Full(_)) => {
        trace!("Reader cannot send new status changes, datareader is full.");
        // It is perfectly normal to fail due to full channel, because
        // no-one is required to be listening to these.
      }
      Err(mio_channel::TrySendError::Disconnected(_)) => {
        // If we get here, our DataReader has died. The Reader should now dispose
        // itself. Or possibly it has lost the receiver object, which is sort of
        // sloppy, but does not necessarily mean the end of the world.
        // TODO: Implement Reader disposal.
        info!("send_status_change - cannot send status, DataReader Disconnected.");
      }
      Err(mio_channel::TrySendError::Io(e)) => {
        error!("send_status_change - cannot send status: {:?}", e);
      }
    }
  }

  // The deadline that the DataReader was expecting through its QosPolicy
  // DEADLINE was not respected for a specific instance
  // if statusChange is returned it should be send to DataReader
  // this calculation should be repeated every self.qos_policy.deadline
  #[tracing::instrument(level = "trace")]
  fn calculate_if_requested_deadline_is_missed(&mut self) -> Vec<DataReaderStatus> {
    debug!("calculate_if_requested_deadline_is_missed");

    let deadline_duration = match self.qos_policy.deadline {
      None => return vec![],
      Some(policy::Deadline(deadline_duration)) => deadline_duration,
    };

    let mut changes: Vec<DataReaderStatus> = vec![];
    let now = Timestamp::now();
    for writer_proxy in self.matched_writers.values_mut() {
      if let Some(last_change) = writer_proxy.last_change_timestamp() {
        let since_last = now.duration_since(last_change);
        // if time singe last received message is greater than deadline increase status
        // and return notification.
        trace!(
          "Comparing deadlines: {:?} - {:?}",
          since_last,
          deadline_duration
        );
        if since_last > deadline_duration {
          debug!(
            "Deadline missed: {:?} - {:?}",
            since_last, deadline_duration
          );
          self.requested_deadline_missed_count += 1;
          changes.push(DataReaderStatus::RequestedDeadlineMissed {
            count: CountWithChange::start_from(self.requested_deadline_missed_count, 1),
          });
        }
      } else {
        // no messages received ever so deadline must be missed.
        // TODO: But what if the Reader or WriterProxy was just created?
        self.requested_deadline_missed_count += 1;
        changes.push(DataReaderStatus::RequestedDeadlineMissed {
          count: CountWithChange::start_from(self.requested_deadline_missed_count, 1),
        });
      }
    } // for
    changes
  } // fn

  #[tracing::instrument(level = "trace")]
  pub fn handle_timed_event(&mut self) {
    while let Some(e) = self.timed_event_timer.poll() {
      match e {
        TimedEvent::DeadlineMissedCheck => {
          self.handle_requested_deadline_event();
          self.set_requested_deadline_check_timer(); // re-prime timer
        }
      }
    }
  }

  #[tracing::instrument(level = "trace")]
  pub fn process_command(&mut self) {
    loop {
      use std::sync::mpsc::TryRecvError;
      match self.data_reader_command_receiver.try_recv() {
        Ok(ReaderCommand::ResetRequestedDeadlineStatus) => {
          warn!("RESET_REQUESTED_DEADLINE_STATUS not implemented!");
          //TODO: This should be implemented.
        }

        // Disconnected is normal when terminating
        Err(TryRecvError::Disconnected) => {
          trace!("DataReader disconnected");
          break;
        }
        Err(TryRecvError::Empty) => {
          warn!("There was no command. Spurious command event??");
          break;
        }
      }
    }
  }

  #[tracing::instrument(level = "trace")]
  fn handle_requested_deadline_event(&mut self) {
    debug!("handle_requested_deadline_event");
    for missed_deadline in self.calculate_if_requested_deadline_is_missed() {
      self.send_status_change(missed_deadline);
    }
  }

  // TODO Used for test/debugging purposes
  #[cfg(test)]
  pub fn history_cache_change_data(&self, sequence_number: SequenceNumber) -> Option<DDSData> {
    let dds_cache = self.dds_cache.read().unwrap();
    let cc = self
      .seqnum_instant_map
      .get(&sequence_number)
      .and_then(|i| dds_cache.topic_get_change(&self.topic_name, i));

    debug!("history cache !!!! {:?}", cc);

    cc.map(|cc| cc.data_value.clone())
  }

  // Used for test/debugging purposes
  #[cfg(test)]
  pub fn history_cache_change(&self, sequence_number: SequenceNumber) -> Option<CacheChange> {
    debug!("{:?}", sequence_number);
    let dds_cache = self.dds_cache.read().unwrap();
    let cc = self
      .seqnum_instant_map
      .get(&sequence_number)
      .and_then(|i| dds_cache.topic_get_change(&self.topic_name, i));
    debug!("history cache !!!! {:?}", cc);
    cc.cloned()
  }

  // TODO Used for test/debugging purposes
  #[cfg(test)]
  pub fn history_cache_sequence_start_and_end_numbers(&self) -> Vec<SequenceNumber> {
    let start = self.seqnum_instant_map.iter().min().unwrap().0;
    let end = self.seqnum_instant_map.iter().max().unwrap().0;
    return vec![*start, *end];
  }

  // updates or adds a new writer proxy, doesn't touch changes
  #[tracing::instrument(level = "trace")]
  pub fn update_writer_proxy(&mut self, proxy: RtpsWriterProxy, offered_qos: &QosPolicies) {
    debug!("update_writer_proxy topic={:?}", self.topic_name);
    match offered_qos.compliance_failure_wrt(&self.qos_policy) {
      None => {
        // success, update or insert
        let writer_id = proxy.remote_writer_guid;
        let count_change = self.matched_writer_update(proxy);
        if count_change > 0 {
          self.writer_match_count_total += count_change;
          self.send_status_change(DataReaderStatus::SubscriptionMatched {
            total: CountWithChange::new(self.writer_match_count_total, count_change),
            current: CountWithChange::new(self.matched_writers.len() as i32, count_change),
          });
          info!(
            "Matched new remote writer on topic={:?} writer= {:?}",
            self.topic_name, writer_id
          );
        }
      }
      Some(bad_policy_id) => {
        // no QoS match
        self.offered_incompatible_qos_count += 1;
        self.send_status_change(DataReaderStatus::RequestedIncompatibleQos {
          count: CountWithChange::new(self.offered_incompatible_qos_count, 1),
          last_policy_id: bad_policy_id,
          policies: Vec::new(), // TODO. implementation missing
        });
        warn!("update_writer_proxy - QoS mismatch {:?}", bad_policy_id);
        info!(
          "update_writer_proxy - QoS mismatch: topic={:?} requested={:?}  offered={:?}",
          self.topic_name, &self.qos_policy, offered_qos
        );
      }
    }
  }

  // return value counts how many new proxies were added
  #[tracing::instrument(level = "trace")]
  fn matched_writer_update(&mut self, proxy: RtpsWriterProxy) -> i32 {
    if let Some(op) = self.matched_writer_lookup(proxy.remote_writer_guid) {
      op.update_contents(proxy);
      0
    } else {
      self.matched_writers.insert(proxy.remote_writer_guid, proxy);
      1
    }
  }

  #[tracing::instrument(level = "trace")]
  pub fn remove_writer_proxy(&mut self, writer_guid: GUID) {
    if self.matched_writers.contains_key(&writer_guid) {
      self.matched_writers.remove(&writer_guid);
      self.send_status_change(DataReaderStatus::SubscriptionMatched {
        total: CountWithChange::new(self.writer_match_count_total, 0),
        current: CountWithChange::new(self.matched_writers.len() as i32, -1),
      });
    }
  }

  // Entire remote participant was lost.
  // Remove all remote readers belonging to it.
  #[tracing::instrument(level = "trace")]
  pub fn participant_lost(&mut self, guid_prefix: GuidPrefix) {
    let lost_readers: Vec<GUID> = self
      .matched_writers
      .range(guid_prefix.range())
      .map(|(g, _)| *g)
      .collect();
    for reader in lost_readers {
      self.remove_writer_proxy(reader);
    }
  }

  pub fn contains_writer(&self, entity_id: EntityId) -> bool {
    self
      .matched_writers
      .iter()
      .any(|(&g, _)| g.entity_id == entity_id)
  }

  #[cfg(test)]
  #[tracing::instrument(level = "trace")]
  pub(crate) fn matched_writer_add(
    &mut self,
    remote_writer_guid: GUID,
    remote_group_entity_id: EntityId,
    unicast_locator_list: Vec<Locator>,
    multicast_locator_list: Vec<Locator>,
    qos: &QosPolicies,
  ) {
    let proxy = RtpsWriterProxy::new(
      remote_writer_guid,
      unicast_locator_list,
      multicast_locator_list,
      remote_group_entity_id,
    );
    self.update_writer_proxy(proxy, qos);
  }

  fn matched_writer_lookup(&mut self, remote_writer_guid: GUID) -> Option<&mut RtpsWriterProxy> {
    self.matched_writers.get_mut(&remote_writer_guid)
  }

  // handles regular data message and updates history cache
  #[tracing::instrument(level = "trace", skip(data, data_flags))]
  pub fn handle_data_msg(
    &mut self,
    data: Data,
    data_flags: BitFlags<DATA_Flags>,
    mr_state: &MessageReceiverState,
  ) {
    //trace!("handle_data_msg entry");
    let receive_timestamp = Timestamp::now();

    // parse write_options out of the message
    let mut write_options_b = WriteOptionsBuilder::new();
    // Check if we have s source timestamp
    if let Some(source_timestamp) = mr_state.source_timestamp {
      write_options_b = write_options_b.source_timestamp(source_timestamp);
    }
    // Check if the message specifies a related_sample_identity
    let ri = DATA_Flags::cdr_representation_identifier(data_flags);
    if let Some(related_sample_identity) = data
      .inline_qos
      .as_ref()
      .and_then(|iqos| InlineQos::related_sample_identity(iqos, ri).ok())
      .flatten()
    {
      write_options_b = write_options_b.related_sample_identity(related_sample_identity);
    }

    let writer_guid = GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, data.writer_id);
    let writer_seq_num = data.writer_sn; // for borrow checker

    match self.data_to_ddsdata(data, data_flags) {
      Ok(ddsdata) => self.process_received_data(
        ddsdata,
        receive_timestamp,
        write_options_b.build(),
        writer_guid,
        writer_seq_num,
      ),
      Err(e) => debug!("Parsing DATA to DDSData failed: {}", e),
    }
  }

  #[tracing::instrument(level = "trace", skip(datafrag, datafrag_flags))]
  pub fn handle_datafrag_msg(
    &mut self,
    datafrag: &DataFrag,
    datafrag_flags: BitFlags<DATAFRAG_Flags>,
    mr_state: &MessageReceiverState,
  ) {
    let writer_guid = GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, datafrag.writer_id);
    let seq_num = datafrag.writer_sn;
    let receive_timestamp = Timestamp::now();

    // check if this submessage is expired already
    // TODO: Maybe this check is in the wrong place altogether? It should be
    // done when Datareader fetches data for the application.
    if let (Some(source_timestamp), Some(lifespan)) =
      (mr_state.source_timestamp, self.qos().lifespan)
    {
      let elapsed = receive_timestamp.duration_since(source_timestamp);
      if lifespan.duration < elapsed {
        info!(
          "DataFrag {:?} from {:?} lifespan exeeded. duration={:?} elapsed={:?}",
          seq_num, writer_guid, lifespan.duration, elapsed
        );
        return;
      }
    }

    // parse write_options out of the message
    // TODO: This is almost duplicate code from DATA processing
    let mut write_options_b = WriteOptionsBuilder::new();
    // Check if we have s source timestamp
    if let Some(source_timestamp) = mr_state.source_timestamp {
      write_options_b = write_options_b.source_timestamp(source_timestamp);
    }
    // Check if the message specifies a related_sample_identity
    let ri = DATAFRAG_Flags::cdr_representation_identifier(datafrag_flags);
    if let Some(related_sample_identity) = datafrag
      .inline_qos
      .as_ref()
      .and_then(|iqos| InlineQos::related_sample_identity(iqos, ri).ok())
      .flatten()
    {
      write_options_b = write_options_b.related_sample_identity(related_sample_identity);
    }

    let writer_seq_num = datafrag.writer_sn; // for borrow checker
    if let Some(writer_proxy) = self.matched_writer_lookup(writer_guid) {
      if let Some(complete_ddsdata) = writer_proxy.handle_datafrag(datafrag, datafrag_flags) {
        // Source timestamp (if any) will be the timestamp of the last fragment (that
        // completes the sample).
        self.process_received_data(
          complete_ddsdata,
          receive_timestamp,
          write_options_b.build(),
          writer_guid,
          writer_seq_num,
        );
      } else {
        // not yet complete, nothing more to do
      }
    } else {
      info!(
        "handle_datafrag_msg in stateful Reader {:?} has no writer proxy for {:?} topic={:?}",
        self.my_guid.entity_id, writer_guid, self.topic_name,
      );
    }
  }

  // common parts of processing DATA or a completed DATAFRAG (when all frags are
  // received)
  #[tracing::instrument(level = "trace", skip(ddsdata))]
  fn process_received_data(
    &mut self,
    ddsdata: DDSData,
    receive_timestamp: Timestamp,
    write_options: WriteOptions,
    writer_guid: GUID,
    writer_sn: SequenceNumber,
  ) {
    trace!(
      "process_received_data from {:?} seq={:?} topic={:?} reliability={:?} stateful={:?}",
      &writer_guid,
      writer_sn,
      self.topic_name,
      self.reliability,
      self.is_stateful,
    );
    if self.is_stateful {
      let my_entityid = self.my_guid.entity_id; // to please borrow checker
      if let Some(writer_proxy) = self.matched_writer_lookup(writer_guid) {
        if writer_proxy.should_ignore_change(writer_sn) {
          // change already present
          debug!(
            "process_received_data already have this seq={:?}",
            writer_sn
          );
          if my_entityid == EntityId::SPDP_BUILTIN_PARTICIPANT_READER {
            debug!("Accepting duplicate message to participant reader.");
            // This is an attmpted workaround to eProsima FastRTPS not
            // incrementing sequence numbers. (eProsime shapes demo 2.1.0 from
            // 2021)
          } else {
            return;
          }
        }
        // Add the change and get the instant
        writer_proxy.received_changes_add(writer_sn, receive_timestamp);
      } else {
        // no writer proxy found
        info!(
          "process_received_data in stateful Reader {:?} has no writer proxy for {:?} topic={:?}",
          my_entityid, writer_guid, self.topic_name,
        );
      }
    } else {
      // stateless reader
      todo!()
    }

    self.make_cache_change(
      ddsdata,
      receive_timestamp,
      write_options,
      writer_guid,
      writer_sn,
    );

    // Add to own track-keeping datastructure
    #[cfg(test)]
    self.seqnum_instant_map.insert(writer_sn, receive_timestamp);

    self.notify_cache_change();
  }

  #[tracing::instrument(level = "trace", skip(data, data_flags))]
  fn data_to_ddsdata(
    &self,
    data: Data,
    data_flags: BitFlags<DATA_Flags>,
  ) -> Result<DDSData, String> {
    let representation_identifier = DATA_Flags::cdr_representation_identifier(data_flags);

    match (
      data.serialized_payload,
      data_flags.contains(DATA_Flags::Data),
      data_flags.contains(DATA_Flags::Key),
    ) {
      (Some(sp), true, false) => {
        // data
        Ok(DDSData::new(sp))
      }

      (Some(sp), false, true) => {
        // key
        Ok(DDSData::new_disposed_by_key(
          Self::deduce_change_kind(&data.inline_qos, false, representation_identifier),
          sp,
        ))
      }

      (None, false, false) => {
        // no data, no key. Maybe there is inline QoS?
        // At least we should find key hash, or we do not know WTF the writer is talking
        // about
        let key_hash = if let Some(h) = data
          .inline_qos
          .as_ref()
          .and_then(|iqos| InlineQos::key_hash(iqos).ok())
          .flatten()
        {
          Ok(h)
        } else {
          info!("Received DATA that has no payload and no key_hash inline QoS - discarding");
          // Note: This case is normal when handling coherent sets.
          // The coherent set end marker is sent as DATA with no payload and not key, only
          // Inline QoS.
          Err("DATA with no contents".to_string())
        }?;
        // now, let's try to determine what is the dispose reason
        let change_kind =
          Self::deduce_change_kind(&data.inline_qos, false, representation_identifier);
        info!(
          "status change by Inline QoS: topic={:?} change={:?}",
          self.topic_name, change_kind
        );
        Ok(DDSData::new_disposed_by_key_hash(change_kind, key_hash))
      }

      (Some(_), true, true) => {
        // payload cannot be both key and data.
        // RTPS Spec 9.4.5.3.1 Flags in the Submessage Header says
        // "D=1 and K=1 is an invalid combination in this version of the protocol."
        warn!("Got DATA that claims to be both data and key - discarding.");
        Err("Ambiguous data/key received.".to_string())
      }

      (Some(_), false, false) => {
        // data but no data? - this should not be possible
        warn!("make_cache_change - Flags says no data or key, but got payload!");
        Err("DATA message has mystery contents".to_string())
      }
      (None, true, _) | (None, _, true) => {
        warn!("make_cache_change - Where is my SerializedPayload?");
        Err("DATA message contents missing".to_string())
      }
    }
  }

  // Returns if responding with ACKNACK?
  // TODO: Return value seems to go unused in callers.
  // ...except in test cases, but not sure if this is strictly necessary to have.
  #[tracing::instrument(level = "trace", skip(heartbeat))]
  pub fn handle_heartbeat_msg(
    &mut self,
    heartbeat: &Heartbeat,
    final_flag_set: bool,
    mr_state: MessageReceiverState,
  ) -> bool {
    let writer_guid =
      GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, heartbeat.writer_id);

    if self.reliability == policy::Reliability::BestEffort {
      debug!(
        "HEARTBEAT from {:?}, but this Reader is BestEffort. Ignoring. topic={:?} reader={:?}",
        writer_guid, self.topic_name, self.my_guid
      );
      // BestEffort Reader reacts only to DATA and GAP
      // See RTPS Spec Section "8.4.11 RTPS StatelessReader Behavior":
      // Figure 8.23 - Behavior of the Best-Effort StatefulReader with respect to each
      // matched Writer and
      // Figure 8.22 - Behavior of the Best-Effort StatelessReader
      return false;
    }

    if !self.matched_writers.contains_key(&writer_guid) {
      info!(
        "HEARTBEAT from {:?}, but no writer proxy available. topic={:?} reader={:?}",
        writer_guid, self.topic_name, self.my_guid
      );
      return false;
    }
    // sanity check
    if heartbeat.first_sn < SequenceNumber::default() {
      warn!(
        "Writer {:?} advertised SequenceNumbers from {:?} to {:?}!",
        writer_guid, heartbeat.first_sn, heartbeat.last_sn
      );
    }

    let writer_proxy = if let Some(wp) = self.matched_writer_lookup(writer_guid) {
      wp
    } else {
      error!("Writer proxy disappeared 1!");
      return false;
    };

    let mut mr_state = mr_state;
    mr_state.unicast_reply_locator_list = writer_proxy.unicast_locator_list.clone();

    if heartbeat.count <= writer_proxy.received_heartbeat_count {
      // This heartbeat was already seen an processed.
      return false;
    }
    writer_proxy.received_heartbeat_count = heartbeat.count;

    // remove fragmented changes until first_sn.
    let removed_instances = writer_proxy.irrelevant_changes_up_to(heartbeat.first_sn);

    // Remove instances from DDSHistoryCache
    {
      // Create local scope so that dds_cache write lock is dropped ASAP
      let mut cache = match self.dds_cache.write() {
        Ok(rwlock) => rwlock,
        // TODO: Should we panic here? Are we allowed to continue with poisoned DDSCache?
        Err(e) => panic!("The DDSCache of is poisoned. Error: {}", e),
      };
      for instant in removed_instances.values() {
        if cache
          .topic_remove_change(&self.topic_name, instant)
          .is_none()
        {
          debug!("WriterProxy told to remove an instant which was not present");
          /* This may be normal? */
        }
      }
    }

    let reader_id = self.entity_id();

    // this is duplicate code from above, but needed, because we need another
    // mutable borrow. TODO: Maybe could be written in some sensible way.
    let writer_proxy = if let Some(wp) = self.matched_writer_lookup(writer_guid) {
      wp
    } else {
      error!("Writer proxy disappeared 2!");
      return false;
    };

    // See if ACKNACK is needed, and generate one.
    let missing_seqnums = writer_proxy.missing_seqnums(heartbeat.first_sn, heartbeat.last_sn);

    // Interpretation of final flag in RTPS spec
    // 8.4.2.3.1 Readers must respond eventually after receiving a HEARTBEAT with
    // final flag not set
    //
    // Upon receiving a HEARTBEAT Message with final flag not set, the Reader must
    // respond with an ACKNACK Message. The ACKNACK Message may acknowledge
    // having received all the data samples or may indicate that some data
    // samples are missing. The response may be delayed to avoid message storms.

    if !missing_seqnums.is_empty() || !final_flag_set {
      let mut partially_received = Vec::new();
      // report of what we have.
      // We claim to have received all SNs before "base" and produce a set of missing
      // sequence numbers that are >= base.
      let reader_sn_state = match missing_seqnums.get(0) {
        Some(&first_missing) => {
          // Here we assume missing_seqnums are returned in order.
          // Limit the set to maximum that can be sent in acknack submessage.

          SequenceNumberSet::from_base_and_set(
            first_missing,
            &missing_seqnums
              .iter()
              .copied()
              .take_while(|sn| sn < &(first_missing + SequenceNumber::new(256)))
              .filter(|sn| {
                if writer_proxy.is_partially_received(*sn) {
                  partially_received.push(*sn);
                  false
                } else {
                  true
                }
              })
              .collect(),
          )
        }

        // Nothing missing. Report that we have all we have.
        None => SequenceNumberSet::new_empty(writer_proxy.all_ackable_before()),
      };

      let response_ack_nack = AckNack {
        reader_id,
        writer_id: heartbeat.writer_id,
        reader_sn_state,
        count: writer_proxy.next_ack_nack_sequence_number(),
      };

      // Sanity check
      //
      // Wrong. This sanity check is invalid. The condition
      // ack_base > heartbeat.last_sn + 1
      // May be legitimately true, if there are some changes available, and a GAP
      // after that. E.g. HEARTBEAT 1..8 and GAP 9..10. Then acknack_base == 11
      // and 11 > 8 + 1.
      //
      //
      // if response_ack_nack.reader_sn_state.base() > heartbeat.last_sn +
      // SequenceNumber::new(1) {   error!(
      //     "OOPS! AckNack sanity check tripped: HEARTBEAT = {:?} ACKNACK = {:?}
      // missing_seqnums = {:?} all_ackable_before = {:?} writer={:?}",
      //     &heartbeat, &response_ack_nack, missing_seqnums,
      // writer_proxy.all_ackable_before(), writer_guid,   );
      // }

      // The acknack can be sent now or later. The rest of the RTPS message
      // needs to be constructed. p. 48
      let flags = BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Endianness)
        | BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Final);

      let fflags = BitFlags::<NACKFRAG_Flags>::from_flag(NACKFRAG_Flags::Endianness);

      // send NackFrags, if any
      let mut nackfrags = Vec::new();
      for sn in partially_received {
        let count = writer_proxy.next_ack_nack_sequence_number();
        let mut missing_frags = writer_proxy.missing_frags_for(sn);
        let first_missing = missing_frags.next();
        if let Some(first) = first_missing {
          let missing_frags_set = iter::once(first).chain(missing_frags).collect(); // "undo" the .next() above
          let nf = NackFrag {
            reader_id,
            writer_id: writer_proxy.remote_writer_guid.entity_id,
            writer_sn: sn,
            fragment_number_state: FragmentNumberSet::from_base_and_set(first, &missing_frags_set),
            count,
          };
          nackfrags.push(nf);
        } else {
          error!("The dog ate my missing fragments.");
          // Really, this should not happen, as we are above checking
          // that this SN is really partially (and not fully) received.
        }
      }

      if !nackfrags.is_empty() {
        self.send_nackfrags_to(
          fflags,
          nackfrags,
          InfoDestination {
            guid_prefix: mr_state.source_guid_prefix,
          },
          &mr_state.unicast_reply_locator_list,
        );
      }

      self.send_acknack_to(
        flags,
        response_ack_nack,
        InfoDestination {
          guid_prefix: mr_state.source_guid_prefix,
        },
        &mr_state.unicast_reply_locator_list,
      );

      return true;
    }

    false
  } // fn

  #[tracing::instrument(level = "trace")]
  pub fn handle_gap_msg(&mut self, gap: &Gap, mr_state: &MessageReceiverState) {
    // ATM all things related to groups is ignored. TODO?

    let writer_guid = GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, gap.writer_id);

    if !self.is_stateful {
      debug!(
        "GAP from {:?}, reader is stateless. Ignoring. topic={:?} reader={:?}",
        writer_guid, self.topic_name, self.my_guid
      );
      return;
    }

    let writer_proxy = if let Some(wp) = self.matched_writer_lookup(writer_guid) {
      wp
    } else {
      info!(
        "GAP from {:?}, but no writer proxy available. topic={:?} reader={:?}",
        writer_guid, self.topic_name, self.my_guid
      );
      return;
    };

    // TODO: Implement GAP rules validation (Section 8.3.7.4.3) here.

    // Irrelevant sequence numbers communicated in the Gap message are
    // composed of two groups:
    //   1. All sequence numbers in the range gapStart <= sequence_number <
    // gapList.base
    let mut removed_changes: BTreeSet<Timestamp> = writer_proxy
      .irrelevant_changes_range(gap.gap_start, gap.gap_list.base())
      .values()
      .copied()
      .collect();

    //   2. All the sequence numbers that appear explicitly listed in the gapList.
    for seq_num in gap.gap_list.iter() {
      writer_proxy
        .set_irrelevant_change(seq_num)
        .map(|t| removed_changes.insert(t));
    }

    // Remove from DDSHistoryCache
    // TODO: Is this really correct?
    // Is the meaning of GAP to only inform that such changes are not available from
    // the writer? Or does it also mean that the DDSCache should remove them?
    let mut cache = match self.dds_cache.write() {
      Ok(rwlock) => rwlock,
      // TODO: Should we panic here? Are we allowed to continue with poisoned DDSCache?
      Err(e) => panic!("The DDSCache of is poisoned. Error: {}", e),
    };
    for instant in &removed_changes {
      cache.topic_remove_change(&self.topic_name, instant);
    }

    // Is this needed?
    // self.notify_cache_change();
  }

  pub fn handle_heartbeatfrag_msg(
    &mut self,
    heartbeatfrag: &HeartbeatFrag,
    _mr_state: &MessageReceiverState,
  ) {
    info!(
      "HeartbeatFrag handling not implemented. topic={:?}   {:?}",
      self.topic_name, heartbeatfrag
    );
  }

  // This is used to determine exact change kind in case we do not get a data
  // payload in DATA submessage
  #[tracing::instrument(level = "trace")]
  fn deduce_change_kind(
    inline_qos: &Option<ParameterList>,
    no_writers: bool,
    ri: RepresentationIdentifier,
  ) -> ChangeKind {
    match inline_qos
      .as_ref()
      .and_then(|iqos| InlineQos::status_info(iqos, ri).ok())
    {
      Some(si) => si.change_kind(), // get from inline QoS
      // TODO: What if si.change_kind() gives ALIVE ??
      None => {
        if no_writers {
          ChangeKind::NotAliveUnregistered
        } else {
          ChangeKind::NotAliveDisposed
        } // TODO: Is this reasonable default?
      }
    }
  }

  // Convert DATA submessage into a CacheChange and update history cache
  #[tracing::instrument(level = "trace", skip(data))]
  fn make_cache_change(
    &mut self,
    data: DDSData,
    receive_timestamp: Timestamp,
    write_options: WriteOptions,
    writer_guid: GUID,
    writer_sn: SequenceNumber,
  ) {
    let cache_change = CacheChange::new(writer_guid, writer_sn, write_options, data);
    let mut cache = match self.dds_cache.write() {
      Ok(rwlock) => rwlock,
      // TODO: Should we panic here? Are we allowed to continue with poisoned DDSCache?
      Err(e) => panic!("The DDSCache of is poisoned. Error: {}", e),
    };
    cache.add_change(&self.topic_name, &receive_timestamp, cache_change);
  }

  // notifies DataReaders (or any listeners that history cache has changed for
  // this reader) likely use of mio channel
  #[tracing::instrument(level = "trace")]
  pub fn notify_cache_change(&self) {
    match self.notification_sender.try_send(()) {
      Ok(()) => (),
      Err(mio_channel::TrySendError::Full(_)) => (), /* This is harmless. There is a */
      // notification in already.
      Err(mio_channel::TrySendError::Disconnected(_)) => {
        // If we get here, our DataReader has died. The Reader should now
        // dispose itself. TODO: Implement Reader disposal.
      }
      Err(mio_channel::TrySendError::Io(_)) => {
        // TODO: What does this mean? Can we ever get here?
      }
    }
  }

  #[tracing::instrument(level = "trace")]
  fn send_acknack_to(
    &self,
    flags: BitFlags<ACKNACK_Flags>,
    acknack: AckNack,
    info_dst: InfoDestination,
    dst_localtor_list: &[Locator],
  ) {
    let infodst_flags =
      BitFlags::<INFODESTINATION_Flags>::from_flag(INFODESTINATION_Flags::Endianness);

    let mut message = Message::new(Header {
      protocol_id: ProtocolId::default(),
      protocol_version: ProtocolVersion::THIS_IMPLEMENTATION,
      vendor_id: VendorId::THIS_IMPLEMENTATION,
      guid_prefix: self.my_guid.prefix,
    });

    message.add_submessage(info_dst.create_submessage(infodst_flags));

    message.add_submessage(acknack.create_submessage(flags));

    let bytes = message
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .unwrap();
    self
      .udp_sender
      .send_to_locator_list(&bytes, dst_localtor_list);
  }

  #[tracing::instrument(level = "trace")]
  fn send_nackfrags_to(
    &self,
    flags: BitFlags<NACKFRAG_Flags>,
    nackfrags: Vec<NackFrag>,
    info_dst: InfoDestination,
    dst_locator_list: &[Locator],
  ) {
    let infodst_flags =
      BitFlags::<INFODESTINATION_Flags>::from_flag(INFODESTINATION_Flags::Endianness);

    let mut message = Message::new(Header {
      protocol_id: ProtocolId::default(),
      protocol_version: ProtocolVersion::THIS_IMPLEMENTATION,
      vendor_id: VendorId::THIS_IMPLEMENTATION,
      guid_prefix: self.my_guid.prefix,
    });

    message.add_submessage(info_dst.create_submessage(infodst_flags));

    for nf in nackfrags {
      message.add_submessage(nf.create_submessage(flags));
    }

    let bytes = message
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .unwrap();
    self
      .udp_sender
      .send_to_locator_list(&bytes, dst_locator_list);
  }

  #[tracing::instrument(level = "trace")]
  pub fn send_preemptive_acknacks(&mut self) {
    let flags = BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Endianness);
    // Do not set final flag --> we are requesting immediate heartbeat from writers.

    // Detach the writer proxy set. This is a way to avoid multiple &mut self
    let mut writer_proxies = std::mem::take(&mut self.matched_writers);

    let reader_id = self.entity_id();
    for (_, writer_proxy) in writer_proxies
      .iter_mut()
      .filter(|(_, p)| p.no_changes_received())
    {
      let acknack_count = writer_proxy.next_ack_nack_sequence_number();
      self.send_acknack_to(
        flags,
        AckNack {
          reader_id,
          writer_id: writer_proxy.remote_writer_guid.entity_id,
          reader_sn_state: SequenceNumberSet::new_empty(SequenceNumber::new(1)),
          count: acknack_count,
        },
        InfoDestination {
          guid_prefix: writer_proxy.remote_writer_guid.prefix,
        },
        &writer_proxy.unicast_locator_list,
      );
    }
    // put writer proxies back
    self.matched_writers = writer_proxies;
  }

  pub fn topic_name(&self) -> &String {
    &self.topic_name
  }
} // impl

impl HasQoSPolicy for Reader {
  fn qos(&self) -> QosPolicies {
    self.qos_policy.clone()
  }
}

impl RTPSEntity for Reader {
  fn guid(&self) -> GUID {
    self.my_guid
  }
}

impl fmt::Debug for Reader {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Reader")
      .field("notification_sender, dds_cache", &"can't print".to_string())
      .field("topic_name", &self.topic_name)
      .field("my_guid", &self.my_guid)
      .field("heartbeat_response_delay", &self.heartbeat_response_delay)
      .field("received_hearbeat_count", &self.received_hearbeat_count)
      .finish()
  }
}

#[cfg(test)]
mod tests {
  use crate::{
    dds::{
      qos::policy::Reliability, statusevents::DataReaderStatus, typedesc::TypeDesc,
      with_key::datawriter::WriteOptions,
    },
    messages::submessages::submessage_elements::serialized_payload::SerializedPayload,
    structure::guid::{EntityId, EntityKind, GuidPrefix, GUID},
    Duration, QosPolicyBuilder,
  };
  use super::*;

  #[test]
  #[ignore]
  fn rtpsreader_notification() {
    let mut guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    guid.entity_id = EntityId::create_custom_entity_id([1, 2, 3], EntityKind::from(111));

    let (send, rec) = mio_channel::sync_channel::<()>(100);
    let (status_sender, _status_receiver) =
      mio_extras::channel::sync_channel::<DataReaderStatus>(100);
    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    dds_cache
      .write()
      .unwrap()
      .add_new_topic("test".to_string(), TypeDesc::new("testi".to_string()));

    let reader_ing = ReaderIngredients {
      guid,
      notification_sender: send,
      status_sender,
      topic_name: "test".to_string(),
      qos_policy: QosPolicies::qos_none(),
      data_reader_command_receiver: reader_command_receiver,
    };
    let mut reader = Reader::new(
      reader_ing,
      dds_cache,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
    );

    let writer_guid = GUID {
      prefix: GuidPrefix::new(&[1; 12]),
      entity_id: EntityId::create_custom_entity_id(
        [1; 3],
        EntityKind::WRITER_WITH_KEY_USER_DEFINED,
      ),
    };
    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.clone(),
      mr_state.multicast_reply_locator_list.clone(),
      &QosPolicies::qos_none(),
    );

    let reader_id = EntityId::create_custom_entity_id([1, 2, 3], EntityKind::from(111));
    let data = Data {
      reader_id,
      writer_id: writer_guid.entity_id,
      ..Data::default()
    };

    reader.handle_data_msg(data, BitFlags::<DATA_Flags>::empty(), &mr_state);

    // TODO:
    // Investaige whyt this fails. Is the test case correct?
    assert!(rec.try_recv().is_ok());
  }

  #[test]
  #[ignore]
  fn rtpsreader_handle_data() {
    let new_guid = GUID::default();

    let (send, rec) = mio_channel::sync_channel::<()>(100);
    let (status_sender, _status_receiver) =
      mio_extras::channel::sync_channel::<DataReaderStatus>(100);
    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    dds_cache
      .write()
      .unwrap()
      .add_new_topic("test".to_string(), TypeDesc::new("testi".to_string()));

    let reader_ing = ReaderIngredients {
      guid: new_guid,
      notification_sender: send,
      status_sender,
      topic_name: "test".to_string(),
      qos_policy: QosPolicies::qos_none(),
      data_reader_command_receiver: reader_command_receiver,
    };
    let mut new_reader = Reader::new(
      reader_ing,
      dds_cache.clone(),
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
    );

    let writer_guid = GUID {
      prefix: GuidPrefix::new(&[1; 12]),
      entity_id: EntityId::create_custom_entity_id(
        [1; 3],
        EntityKind::WRITER_WITH_KEY_USER_DEFINED,
      ),
    };

    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    new_reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.clone(),
      mr_state.multicast_reply_locator_list.clone(),
      &QosPolicies::qos_none(),
    );

    let d = Data {
      writer_id: writer_guid.entity_id,
      ..Default::default()
    };
    let d_seqnum = d.writer_sn;
    new_reader.handle_data_msg(d.clone(), BitFlags::<DATA_Flags>::empty(), &mr_state);

    // TODO: Investigate why this fails. Is the test case or implementation faulty?
    assert!(rec.try_recv().is_ok());

    let hc_locked = dds_cache.read().unwrap();

    let ddsdata = DDSData::new(d.serialized_payload.unwrap());
    let cc_built_here = CacheChange::new(writer_guid, d_seqnum, WriteOptions::default(), ddsdata);

    let cc_from_chache = hc_locked.topic_get_change(
      &new_reader.topic_name,
      new_reader.seqnum_instant_map.get(&d_seqnum).unwrap(),
    );
    // TODO: Investigate why this fails. Is the test case or implementation
    // faulty?
    assert_eq!(cc_from_chache.unwrap(), &cc_built_here);
  }

  #[test]
  fn rtpsreader_handle_heartbeat() {
    env_logger::init();
    let new_guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);

    let (send, _rec) = mio_channel::sync_channel::<()>(100);
    let (status_sender, _status_receiver) =
      mio_extras::channel::sync_channel::<DataReaderStatus>(100);
    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    dds_cache
      .write()
      .unwrap()
      .add_new_topic("test".to_string(), TypeDesc::new("testi".to_string()));
    let reliable_qos = QosPolicyBuilder::new()
      .reliability(Reliability::Reliable {
        max_blocking_time: Duration::from_millis(100),
      })
      .build();
    let reader_ing = ReaderIngredients {
      guid: new_guid,
      notification_sender: send,
      status_sender,
      topic_name: "test".to_string(),
      qos_policy: reliable_qos.clone(),
      data_reader_command_receiver: reader_command_receiver,
    };
    let mut new_reader = Reader::new(
      reader_ing,
      dds_cache,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
    );

    let writer_guid = GUID {
      prefix: GuidPrefix::new(&[1; 12]),
      entity_id: EntityId::create_custom_entity_id(
        [1; 3],
        EntityKind::WRITER_WITH_KEY_USER_DEFINED,
      ),
    };

    let writer_id = writer_guid.entity_id;

    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    new_reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.clone(),
      mr_state.multicast_reply_locator_list.clone(),
      &reliable_qos,
    );

    let d = DDSData::new(SerializedPayload::default());
    let mut changes = Vec::new();

    let hb_new = Heartbeat {
      reader_id: new_reader.entity_id(),
      writer_id,
      first_sn: SequenceNumber::new(1), // First hearbeat from a new writer
      last_sn: SequenceNumber::new(0),
      count: 1,
    };
    assert!(!new_reader.handle_heartbeat_msg(&hb_new, true, mr_state.clone())); // should be false, no ack

    let hb_one = Heartbeat {
      reader_id: new_reader.entity_id(),
      writer_id,
      first_sn: SequenceNumber::new(1), // Only one in writers cache
      last_sn: SequenceNumber::new(1),
      count: 2,
    };
    assert!(new_reader.handle_heartbeat_msg(&hb_one, false, mr_state.clone())); // Should send an ack_nack

    // After ack_nack, will receive the following change
    let change = CacheChange::new(
      new_reader.guid(),
      SequenceNumber::new(1),
      WriteOptions::default(),
      d.clone(),
    );
    new_reader.dds_cache.write().unwrap().add_change(
      &new_reader.topic_name,
      &Timestamp::now(),
      change.clone(),
    );
    changes.push(change);

    // Duplicate
    let hb_one2 = Heartbeat {
      reader_id: new_reader.entity_id(),
      writer_id,
      first_sn: SequenceNumber::new(1), // Only one in writers cache
      last_sn: SequenceNumber::new(1),
      count: 2,
    };
    assert!(!new_reader.handle_heartbeat_msg(&hb_one2, false, mr_state.clone())); // No acknack

    let hb_3_1 = Heartbeat {
      reader_id: new_reader.entity_id(),
      writer_id,
      first_sn: SequenceNumber::new(1), // writer has last 2 in cache
      last_sn: SequenceNumber::new(3),  // writer has written 3 samples
      count: 3,
    };
    assert!(new_reader.handle_heartbeat_msg(&hb_3_1, false, mr_state.clone())); // Should send an ack_nack

    // After ack_nack, will receive the following changes
    let change = CacheChange::new(
      new_reader.guid(),
      SequenceNumber::new(2),
      WriteOptions::default(),
      d.clone(),
    );
    new_reader.dds_cache.write().unwrap().add_change(
      &new_reader.topic_name,
      &Timestamp::now(),
      change.clone(),
    );
    changes.push(change);

    let change = CacheChange::new(
      new_reader.guid(),
      SequenceNumber::new(3),
      WriteOptions::default(),
      d,
    );
    new_reader.dds_cache.write().unwrap().add_change(
      &new_reader.topic_name,
      &Timestamp::now(),
      change.clone(),
    );
    changes.push(change);

    let hb_none = Heartbeat {
      reader_id: new_reader.entity_id(),
      writer_id,
      first_sn: SequenceNumber::new(4), // writer has no samples available
      last_sn: SequenceNumber::new(3),  // writer has written 3 samples
      count: 4,
    };
    assert!(new_reader.handle_heartbeat_msg(&hb_none, false, mr_state)); // Should sen acknack

    //assert_eq!(new_reader.sent_ack_nack_count, 3);
    // TODO: Cannot get the above count from Reader directly.
    // How to get it from writer proxies?
  }

  #[test]
  #[ignore]
  fn rtpsreader_handle_gap() {
    // TODO: investiage why this fails. Does the test case even make sense with
    // current code?
    let new_guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    let (send, _rec) = mio_channel::sync_channel::<()>(100);
    let (status_sender, _status_receiver) =
      mio_extras::channel::sync_channel::<DataReaderStatus>(100);
    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    dds_cache
      .write()
      .unwrap()
      .add_new_topic("test".to_string(), TypeDesc::new("testi".to_string()));

    let reader_ing = ReaderIngredients {
      guid: new_guid,
      notification_sender: send,
      status_sender,
      topic_name: "test".to_string(),
      qos_policy: QosPolicies::qos_none(),
      data_reader_command_receiver: reader_command_receiver,
    };
    let mut reader = Reader::new(
      reader_ing,
      dds_cache,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
    );

    let writer_guid = GUID {
      prefix: GuidPrefix::new(&[1; 12]),
      entity_id: EntityId::create_custom_entity_id(
        [1; 3],
        EntityKind::WRITER_WITH_KEY_USER_DEFINED,
      ),
    };
    let writer_id = writer_guid.entity_id;

    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.clone(),
      mr_state.multicast_reply_locator_list.clone(),
      &QosPolicies::qos_none(),
    );

    let n: i64 = 10;
    let mut d = Data {
      writer_id,
      ..Default::default()
    };
    let mut changes = Vec::new();

    for i in 0..n {
      d.writer_sn = SequenceNumber::new(i);
      reader.handle_data_msg(d.clone(), BitFlags::<DATA_Flags>::empty(), &mr_state);
      changes.push(reader.history_cache_change(d.writer_sn).unwrap().clone());
    }

    // make sequence numbers 1-3 and 5 7 irrelevant
    let mut gap_list = SequenceNumberSet::new(SequenceNumber::new(4), 7);
    gap_list.test_insert(SequenceNumber::new(5));
    gap_list.test_insert(SequenceNumber::new(7));

    let gap = Gap {
      reader_id: reader.entity_id(),
      writer_id,
      gap_start: SequenceNumber::new(1),
      gap_list,
    };

    // Cache changee muutetaan tutkiin datan kirjoittajaa.
    reader.handle_gap_msg(&gap, &mr_state);

    assert_eq!(
      reader.history_cache_change(SequenceNumber::new(0)),
      Some(changes[0].clone())
    );
    assert_eq!(reader.history_cache_change(SequenceNumber::new(1)), None);
    assert_eq!(reader.history_cache_change(SequenceNumber::new(2)), None);
    assert_eq!(reader.history_cache_change(SequenceNumber::new(3)), None);
    assert_eq!(
      reader.history_cache_change(SequenceNumber::new(4)),
      Some(changes[4].clone())
    );
    assert_eq!(reader.history_cache_change(SequenceNumber::new(5)), None);
    assert_eq!(
      reader.history_cache_change(SequenceNumber::new(6)),
      Some(changes[6].clone())
    );
    assert_eq!(reader.history_cache_change(SequenceNumber::new(7)), None);
    assert_eq!(
      reader.history_cache_change(SequenceNumber::new(8)),
      Some(changes[8].clone())
    );
    assert_eq!(
      reader.history_cache_change(SequenceNumber::new(9)),
      Some(changes[9].clone())
    );
  }
}
