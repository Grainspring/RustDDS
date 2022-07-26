mod display_string;
mod messages;
mod stateful_list;
mod visualization_helpers;
mod visualizator_app;

use std::{
  env,
  error::Error,
  io,
  time::{Duration, Instant},
};

use messages::{DataUpdate, RosCommand};
use visualizator_app::VisualizatorApp;
use mio::{Events, Poll, PollOpt, Ready, Token};
use mio_extras::channel as mio_channel;
use rustdds::ros2::{builtin_datatypes::NodeInfo, RosParticipant};
use crossterm::{
  event::{DisableMouseCapture, EnableMouseCapture},
  execute,
  terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use tui::{
  backend::{Backend, CrosstermBackend},
  Terminal,
};
use tracing_subscriber::{
  filter::{Directive, EnvFilter, LevelFilter},
  layer::SubscriberExt,
};

const ROS2_COMMAND_TOKEN: Token = Token(1000);
const ROS2_NODE_RECEIVED_TOKEN: Token = Token(1001);
const TOPIC_UPDATE_TIMER_TOKEN: Token = Token(1002);

// Application main loop
// listens updates from ros2_loop routine
// listens user kayboard inputs
// updates GUI
fn run_app<B: Backend>(
  terminal: &mut Terminal<B>,
  mut app: VisualizatorApp,
  tick_rate: Duration,
  command_sender: mio_channel::SyncSender<RosCommand>,
) -> io::Result<()> {
  let mut last_tick = Instant::now();
  loop {
    // here check if ros thread has found new data and update it to VisualizatorApp
    // todo: handle error
    while let Ok(data) = &mut app.receiver.try_recv() {
      match data {
        DataUpdate::NewROSParticipantFound { participant } => {
          &mut app.add_new_ros_participant(participant.clone())
        }
        DataUpdate::DiscoveredTopics { topics } => &mut app.set_discovered_topics(topics.clone()),
        DataUpdate::DiscoveredNodes { nodes } => &mut app.set_node_infos(nodes.clone()),
      };
    }

    terminal.draw(|f| app.ui(f))?;

    let timeout = tick_rate
      .checked_sub(last_tick.elapsed())
      .unwrap_or_else(|| Duration::from_secs(0));

    //If user presses quit button then send command to stop ros loop and exit from
    // this loop also.
    if app.handle_user_input(&timeout) {
      //TODO HANDLE RESULT
      let _send_command_result = command_sender.send(RosCommand::StopRosLoop);
      return Ok(());
    }

    if last_tick.elapsed() >= tick_rate {
      last_tick = Instant::now();
    }
  }
}

// This loop runs in separate thead
// Polls ROS events and sends data via mio_channel to main thread.
fn ros2_loop(
  sender: mio_channel::SyncSender<DataUpdate>,
  command_receiver: mio_channel::Receiver<RosCommand>,
) {
  let mut ros_participant = RosParticipant::new().unwrap();
  //let mut ros_participant = &mut visualizator_app.ros_participant;
  let poll = Poll::new().unwrap();

  let mut update_timer = mio_extras::timer::Timer::default();
  update_timer.set_timeout(Duration::from_secs(1), ());

  poll
    .register(
      &ros_participant,
      ROS2_NODE_RECEIVED_TOKEN,
      Ready::readable(),
      PollOpt::edge(),
    )
    .unwrap();

  poll
    .register(
      &command_receiver,
      ROS2_COMMAND_TOKEN,
      Ready::readable(),
      PollOpt::edge(),
    )
    .unwrap();

  poll
    .register(
      &update_timer,
      TOPIC_UPDATE_TIMER_TOKEN,
      Ready::readable(),
      PollOpt::edge(),
    )
    .unwrap();

  loop {
    let mut events = Events::with_capacity(100);
    poll.poll(&mut events, None).unwrap();

    for event in events.iter() {
      if event.token() == ROS2_NODE_RECEIVED_TOKEN {
        let new_participants = ros_participant.handle_node_read();

        let mut node_infos: Vec<NodeInfo> = ros_participant
          .get_all_discovered_local_ros_node_infos()
          .values()
          .cloned()
          .collect();
        let node_infos_external_vec: Vec<Vec<NodeInfo>> = ros_participant
          .get_all_discovered_external_ros_node_infos()
          .values()
          .cloned()
          .collect();
        let mut node_infos_external = vec![];
        for mut v in node_infos_external_vec {
          node_infos_external.append(&mut v);
        }

        node_infos.append(&mut node_infos_external);
        let _res_nodes_send = sender.send(DataUpdate::DiscoveredNodes { nodes: node_infos });

        for participant in new_participants {
          let _res_participant_send =
            sender.send(DataUpdate::NewROSParticipantFound { participant });
        }

        let new_topics = ros_participant.discovered_topics();
        let _res_topics_send = sender.send(DataUpdate::DiscoveredTopics { topics: new_topics });
      } else if event.token() == ROS2_COMMAND_TOKEN {
        match command_receiver.try_recv() {
          Ok(command) => {
            match command {
              RosCommand::StopRosLoop => {
                return;
              }
            }
            //TODO HANDLE ERROR
          }
          Err(_e) => {}
        }
      } else if event.token() == TOPIC_UPDATE_TIMER_TOKEN {
        let new_topics = ros_participant.discovered_topics();
        let _res_topics_send = sender.send(DataUpdate::DiscoveredTopics { topics: new_topics });
        update_timer.set_timeout(Duration::from_secs(1), ());
      }
    }
  }
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

fn main() -> Result<(), Box<dyn Error>> {
  let _r = init_env_logger("ENV_LOG");
  enable_raw_mode().unwrap();
  let mut stdout = io::stdout();
  execute!(stdout, EnterAlternateScreen, EnableMouseCapture).unwrap();
  let backend = CrosstermBackend::new(stdout);
  let mut terminal = Terminal::new(backend).unwrap();

  //This channel sends DataUpdate messages to Visualizator App to be displayed
  let (sender, receiver) = mio_channel::sync_channel::<DataUpdate>(10);
  //This channel sends command messages from Visualizator to ROS thread.
  let (command_sender, command_receiver) = mio_channel::sync_channel::<RosCommand>(10);

  let jhandle = std::thread::spawn(move || ros2_loop(sender, command_receiver));
  let visualizor_app = VisualizatorApp::new(receiver);

  //This is GUI refresh rate
  let tick_rate = Duration::from_millis(250);
  let _res = run_app(&mut terminal, visualizor_app, tick_rate, command_sender);

  jhandle.join().unwrap();

  disable_raw_mode()?;
  execute!(
    terminal.backend_mut(),
    LeaveAlternateScreen,
    DisableMouseCapture
  )?;
  terminal.show_cursor()?;

  Ok(())
}
