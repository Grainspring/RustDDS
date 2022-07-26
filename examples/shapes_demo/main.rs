//! Interoperability test program for `RustDDS` library

#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::{
  env, io,
  time::{Duration, Instant},
};

use tracing_subscriber::{
  filter::{Directive, EnvFilter, LevelFilter},
  layer::SubscriberExt,
};
use tracing::{debug, error, trace};
use rustdds::{
  DomainParticipant, Keyed, QosPolicyBuilder, StatusEvented, TopicDescription, TopicKind,
};
use rustdds::policy::{Deadline, Durability, History, Reliability}; /* import all QoS
                                                                     * policies directly */
use serde::{Deserialize, Serialize};
use clap::{Arg, ArgMatches, Command}; // command line argument processing
use mio::{Events, Poll, PollOpt, Ready, Token}; // polling
use mio_extras::channel; // pollable channel
use rand::prelude::*;

#[derive(Serialize, Deserialize, Clone)]
struct Shape {
  color: String,
  x: i32,
  y: i32,
  shapesize: i32,
}

impl Keyed for Shape {
  type K = String;
  fn key(&self) -> String {
    self.color.clone()
  }
}

const DA_WIDTH: i32 = 240;
const DA_HEIGHT: i32 = 270;

const STOP_PROGRAM: Token = Token(0);
const READER_READY: Token = Token(1);
const READER_STATUS_READY: Token = Token(2);
const WRITER_STATUS_READY: Token = Token(3);

#[allow(clippy::too_many_lines)]
fn main() {
  let _r = init_env_logger("ENV_LOG");
  let matches = get_matches();

  // Process command line arguments
  let topic_name = matches.value_of("topic").unwrap_or("Square");
  let domain_id = matches
    .value_of("domain_id")
    .unwrap_or("0")
    .parse::<u16>()
    .unwrap_or(0);
  let color = matches.value_of("color").unwrap_or("BLUE");

  let domain_participant = DomainParticipant::new(domain_id)
    .unwrap_or_else(|e| panic!("DomainParticipant construction failed: {:?}", e));

  let mut qos_b = QosPolicyBuilder::new()
    .reliability(if matches.is_present("reliable") {
      Reliability::Reliable {
        max_blocking_time: rustdds::Duration::DURATION_ZERO,
      }
    } else {
      Reliability::BestEffort
    })
    .durability(match matches.value_of("durability") {
      Some("l") => Durability::TransientLocal,
      Some("t") => Durability::Transient,
      Some("p") => Durability::Persistent,
      _ => Durability::Volatile,
    })
    .history(match matches.value_of("history_depth").map(str::parse) {
      None | Some(Err(_)) => History::KeepAll,
      Some(Ok(d)) => {
        if d < 0 {
          History::KeepAll
        } else {
          History::KeepLast { depth: d }
        }
      }
    });
  let deadline_policy = match matches.value_of("deadline") {
    None => None,
    Some(dl) => match dl.parse::<f64>() {
      Ok(d) => Some(Deadline(rustdds::Duration::from_frac_seconds(d))),
      Err(e) => panic!("Expected numeric value for deadline. {:?}", e),
    },
  };

  if let Some(dl) = deadline_policy {
    qos_b = qos_b.deadline(dl);
  }

  assert!(
    !matches.is_present("partition"),
    "QoS policy Partition is not yet implemented."
  );

  assert!(
    !matches.is_present("interval"),
    "QoS policy Time Based Filter is not yet implemented."
  );

  assert!(
    !matches.is_present("ownership_strength"),
    "QoS policy Ownership Strength is not yet implemented."
  );

  let qos = qos_b.build();

  let loop_delay: Duration = match deadline_policy {
    None => Duration::from_millis(200), // This is the default rate
    Some(Deadline(dd)) => Duration::from(dd).mul_f32(0.8), // slightly faster than dealine
  };

  let topic = domain_participant
    .create_topic(
      topic_name.to_string(),
      "ShapeType".to_string(),
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {:?}", e));
  println!(
    "Topic name is {}. Type is {}.",
    topic.name(),
    topic.get_type().name()
  );
  trace!(
    "Topic name is {}. Type is {}.",
    topic.name(),
    topic.get_type().name()
  );
  // Set Ctrl-C handler
  let (stop_sender, stop_receiver) = channel::channel();
  ctrlc::set_handler(move || {
    stop_sender.send(()).unwrap_or(());
    // ignore errors, as we are quitting anyway
  })
  .expect("Error setting Ctrl-C handler");
  println!("Press Ctrl-C to quit.");

  let poll = Poll::new().unwrap();
  let mut events = Events::with_capacity(4);

  poll
    .register(
      &stop_receiver,
      STOP_PROGRAM,
      Ready::readable(),
      PollOpt::edge(),
    )
    .unwrap();

  let is_publisher = matches.is_present("publisher");
  let is_subscriber = matches.is_present("subscriber");

  let mut writer_opt = if is_publisher {
    debug!("Publisher");
    let publisher = domain_participant.create_publisher(&qos).unwrap();
    let mut writer = publisher
      .create_datawriter_cdr::<Shape>(&topic, None) // None = get qos policy from publisher
      .unwrap();
    poll
      .register(
        writer.as_status_evented(),
        WRITER_STATUS_READY,
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();
    Some(writer)
  } else {
    None
  };

  let mut reader_opt = if is_subscriber {
    debug!("Subscriber");
    let subscriber = domain_participant.create_subscriber(&qos).unwrap();
    let mut reader = subscriber
      .create_datareader_cdr::<Shape>(&topic, Some(qos))
      .unwrap();
    poll
      .register(&reader, READER_READY, Ready::readable(), PollOpt::edge())
      .unwrap();
    poll
      .register(
        reader.as_status_evented(),
        READER_STATUS_READY,
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();
    debug!("Created DataReader");
    Some(reader)
  } else {
    None
  };

  let mut shape_sample = Shape {
    color: color.to_string(),
    x: 0,
    y: 0,
    shapesize: 21,
  };
  let mut random_gen = thread_rng();
  // a bit complicated lottery to ensure we do not end up with zero velocity.
  let mut x_vel = if random() {
    random_gen.gen_range(1..5)
  } else {
    random_gen.gen_range(-5..-1)
  };
  let mut y_vel = if random() {
    random_gen.gen_range(1..5)
  } else {
    random_gen.gen_range(-5..-1)
  };

  let mut last_write = Instant::now();

  loop {
    poll.poll(&mut events, Some(loop_delay)).unwrap();
    for event in &events {
      match event.token() {
        STOP_PROGRAM => {
          if stop_receiver.try_recv().is_ok() {
            println!("Done.");
            return;
          }
        }
        READER_READY => {
          match reader_opt {
            Some(ref mut reader) => {
              loop {
                trace!("DataReader triggered");
                match reader.take_next_sample() {
                  Ok(Some(sample)) => match sample.into_value() {
                    Ok(sample) => {
                      println!(
                        "{:10.10} {:10.10} {:3.3} {:3.3} [{}]",
                        topic.name(),
                        sample.color,
                        sample.x,
                        sample.y,
                        sample.shapesize,
                      );
                      trace!(
                        "{:10.10} {:10.10} {:3.3} {:3.3} [{}]",
                        topic.name(),
                        sample.color,
                        sample.x,
                        sample.y,
                        sample.shapesize,
                      );
                    }
                    Err(key) => println!("Disposed key {:?}", key),
                  },
                  Ok(None) => break, // no more data
                  Err(e) => println!("DataReader error {:?}", e),
                } // match
              }
            }
            None => {
              error!("Where is my reader?");
            }
          }
        }
        READER_STATUS_READY => match reader_opt {
          Some(ref mut reader) => {
            while let Some(status) = reader.try_recv_status() {
              println!("DataReader status: {:?}", status);
              trace!("DataReader status: {:?}", status);
            }
          }
          None => {
            error!("Where is my reader?");
          }
        },

        WRITER_STATUS_READY => match writer_opt {
          Some(ref mut writer) => {
            while let Some(status) = writer.try_recv_status() {
              println!("DataWriter status: {:?}", status);
              trace!("DataWriter status: {:?}", status);
            }
          }
          None => {
            error!("Where is my writer?");
          }
        },
        other_token => {
          println!("Polled event is {:?}. WTF?", other_token);
          trace!("Polled event is {:?}. WTF?", other_token);
        }
      }
    }

    let r = move_shape(shape_sample, x_vel, y_vel);
    shape_sample = r.0;
    x_vel = r.1;
    y_vel = r.2;

    // write to DDS
    match writer_opt {
      Some(ref mut writer) => {
        let now = Instant::now();
        if last_write + loop_delay < now {
          // write to DDS
          trace!("Writing shape color {} in {:?}", &color, last_write);
          writer
            .write(shape_sample.clone(), None)
            .unwrap_or_else(|e| error!("DataWriter write failed: {:?}", e));
          last_write = now;
        }
      }
      None => {
        if is_publisher {
          error!("Where is my writer?");
        } else { /* never mind */
        }
      }
    }
  } // loop
}

/// In contrast to `init_env_logger` this allows you to choose an env var
/// other than `ENV_LOG`.
pub fn init_env_logger(env: &str) -> Result<(), core::fmt::Error> {
  let filter = match env::var(env) {
    Ok(env) => EnvFilter::new(env),
    _ => EnvFilter::default().add_directive(Directive::from(LevelFilter::WARN)),
  };
  let atrace_log = match env::var(String::from(env) + "_ATRACE") {
    Ok(_) => true,
    Err(_) => false,
  };
  if !atrace_log {
    let layer = tracing_tree::HierarchicalLayer::default()
      .with_writer(io::stderr)
      .with_indent_lines(true)
      .with_targets(true)
      .with_indent_amount(2);
    let layer = layer.with_thread_ids(true).with_thread_names(true);
    let subscriber = tracing_subscriber::Registry::default()
      .with(filter)
      .with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();
  } else {
    let layer = tracing_libatrace::layer().unwrap();
    let subscriber = tracing_subscriber::Registry::default()
      .with(filter)
      .with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();
  }
  Ok(())
}

fn get_matches() -> ArgMatches {
  Command::new("RustDDS-interop")
    .version("0.2.2")
    .author("Juhana Helovuo <juhe@iki.fi>")
    .about("Command-line \"shapes\" interoperability test.")
    .arg(
      Arg::new("domain_id")
        .short('d')
        .value_name("id")
        .help("Sets the DDS domain id number")
        .takes_value(true),
    )
    .arg(
      Arg::new("topic")
        .short('t')
        .value_name("name")
        .help("Sets the topic name")
        .takes_value(true)
        .required(true),
    )
    .arg(
      Arg::new("color")
        .short('c')
        .value_name("color")
        .help("Color to publish (or filter)")
        .takes_value(true),
    )
    .arg(
      Arg::new("durability")
        .short('D')
        .value_name("durability")
        .help("Set durability")
        .takes_value(true)
        .possible_values(&["v", "l", "t", "p"]),
    )
    .arg(
      Arg::new("publisher")
        .help("Act as publisher")
        .short('P')
        .required_unless_present("subscriber"),
    )
    .arg(
      Arg::new("subscriber")
        .help("Act as subscriber")
        .short('S')
        .required_unless_present("publisher"),
    )
    .arg(
      Arg::new("best_effort")
        .help("BEST_EFFORT reliability")
        .short('b')
        .conflicts_with("reliable"),
    )
    .arg(
      Arg::new("reliable")
        .help("RELIABLE reliability")
        .short('r')
        .conflicts_with("best_effort"),
    )
    .arg(
      Arg::new("history_depth")
        .help("Keep history depth")
        .short('k')
        .takes_value(true)
        .value_name("depth"),
    )
    .arg(
      Arg::new("deadline")
        .help("Set a 'deadline' with interval (seconds)")
        .short('f')
        .takes_value(true)
        .value_name("interval"),
    )
    .arg(
      Arg::new("partition")
        .help("Set a 'partition' string")
        .short('p')
        .takes_value(true)
        .value_name("partition"),
    )
    .arg(
      Arg::new("interval")
        .help("Apply 'time based filter' with interval (seconds)")
        .short('i')
        .takes_value(true)
        .value_name("interval"),
    )
    .arg(
      Arg::new("ownership_strength")
        .help("Set ownership strength [-1: SHARED]")
        .short('s')
        .takes_value(true)
        .value_name("strength"),
    )
    .get_matches()
}

#[allow(clippy::similar_names)]
fn move_shape(shape: Shape, xv: i32, yv: i32) -> (Shape, i32, i32) {
  let half_size = shape.shapesize / 2 + 1;
  let mut x = shape.x + xv;
  let mut y = shape.y + yv;

  let mut xv_new = xv;
  let mut yv_new = yv;

  if x < half_size {
    x = half_size;
    xv_new = -xv;
  }
  if x > DA_WIDTH - half_size {
    x = DA_WIDTH - half_size;
    xv_new = -xv;
  }
  if y < half_size {
    y = half_size;
    yv_new = -yv;
  }
  if y > DA_HEIGHT - half_size {
    y = DA_HEIGHT - half_size;
    yv_new = -yv;
  }
  (
    Shape {
      color: shape.color,
      x,
      y,
      shapesize: shape.shapesize,
    },
    xv_new,
    yv_new,
  )
}
