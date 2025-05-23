use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Barrier};

use anyhow::Context;
use atlas_client::concurrent_client::ConcurrentClient;
use atlas_common::collections::HashMap;
use atlas_common::peer_addr::PeerAddr;

use intmap::IntMap;
use nolock::queues::mpsc::jiffy::{async_queue, AsyncSender};

use atlas_client::client::ordered_client::Ordered;
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::{async_runtime as rt, channel, init, InitConfig};
use atlas_metrics::{with_metric_level, with_metrics, MetricLevel};
use konst::primitive::parse_usize;
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::Trigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::{LogFile, RollingFileAppender};
use log4rs::append::Append;
use log4rs::config::{Appender, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::threshold::ThresholdFilter;
use log4rs::Config;
use progressive_state_transfer;
use rand::Rng;
use rand_core::SeedableRng;
use rand_distr::Standard;
use rand_xoshiro::SplitMix64;
use semaphores::RawSemaphore;

use crate::common::*;
use crate::generator::{generate_key_pool, generate_kv_pairs, Generator, Operation};
use crate::serialize::Action;

#[derive(Debug)]
pub struct InitTrigger {
    has_been_triggered: AtomicBool,
}

impl Trigger for InitTrigger {
    fn trigger(&self, file: &LogFile) -> anyhow::Result<bool> {
        if file.len_estimate() == 0 {
            println!("Not triggering rollover because file is empty");

            self.has_been_triggered.store(true, Relaxed);
            return Ok(false);
        }

        Ok(self
            .has_been_triggered
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_ok())
    }
    
    fn is_pre_process(&self) -> bool {
        todo!()
    }
}

fn format_old_log(id: u32, str: &str) -> String {
    format!("./logs/log_{}/old/febft{}.log.{}", id, str, "{}")
}

fn format_log(id: u32, str: &str) -> String {
    format!("./logs/log_{}/febft{}.log", id, str)
}

fn policy(id: u32, str: &str) -> CompoundPolicy {
    let trigger = InitTrigger {
        has_been_triggered: AtomicBool::new(false),
    };

    let roller = FixedWindowRoller::builder()
        .base(1)
        .build(format_old_log(id, str).as_str(), 5)
        .context("MsgLog Error")
        .unwrap();

    CompoundPolicy::new(Box::new(trigger), Box::new(roller))
}

fn file_appender(id: u32, str: &str) -> Box<dyn Append> {
    Box::new(
        RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{l} {d} - {m}{n}")))
            .build(format_log(id, str).as_str(), Box::new(policy(id, str)))
            .context("Failed to create rolling file appender")
            .unwrap(),
    )
}

fn generate_log(id: u32) {
    let console_appender = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} {d} - {m}{n}")))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("comm", file_appender(id, "_comm")))
        .appender(Appender::builder().build("reconfig", file_appender(id, "_reconfig")))
        .appender(Appender::builder().build("common", file_appender(id, "_common")))
        .appender(Appender::builder().build("consensus", file_appender(id, "_consensus")))
        .appender(Appender::builder().build("file", file_appender(id, "")))
        .appender(Appender::builder().build("log_transfer", file_appender(id, "_log_transfer")))
        .appender(Appender::builder().build("state_transfer", file_appender(id, "_state_transfer")))
        .appender(Appender::builder().build("decision_log", file_appender(id, "_decision_log")))
        .appender(Appender::builder().build("replica", file_appender(id, "_replica")))
        .appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(LevelFilter::Warn)))
                .build("console", Box::new(console_appender)),
        )
        .logger(
            Logger::builder()
                .appender("comm")
                .build("atlas_communication", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("common")
                .build("atlas_common", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("reconfig")
                .build("atlas_reconfiguration", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("log_transfer")
                .build("atlas_log_transfer", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("decision_log")
                .build("atlas_decision_log", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("replica")
                .build("atlas_smr_replica", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("consensus")
                .build("febft_pbft_consensus", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("state_transfer")
                .build("febft_state_transfer", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("state_transfer")
                .build("progressive_state_transfer", LevelFilter::Debug),
        )
        .logger(
            Logger::builder()
                .appender("state_transfer")
                .build("atlas_divisible_state", LevelFilter::Debug),
        )
        .build(Root::builder().appender("file").build(LevelFilter::Debug))
        .context("MsgLog Error")
        .unwrap();

    let _handle = log4rs::init_config(config).context("MsgLog Error").unwrap();
}

pub fn main() {
    let is_client = std::env::var("CLIENT").map(|x| x == "1").unwrap_or(false);

    let single_server = std::env::var("SINGLE_SERVER")
        .map(|x| x == "1")
        .unwrap_or(false);

    let threadpool_threads = parse_usize(
        std::env::var("THREADPOOL_THREADS")
            .unwrap_or(String::from("2"))
            .as_str(),
    )
    .unwrap();
    let async_threads = parse_usize(
        std::env::var("ASYNC_THREADS")
            .unwrap_or(String::from("2"))
            .as_str(),
    )
    .unwrap();

    let id: u32 = std::env::var("ID")
        .iter()
        .flat_map(|id| id.parse())
        .next()
        .unwrap();

    println!("Starting...");

    if !is_client {
        let id: u32 = std::env::var("ID")
            .iter()
            .flat_map(|id| id.parse())
            .next()
            .unwrap();

        generate_log(id);

        let conf = InitConfig {
            //If we are the client, we want to have many threads to send stuff to replicas
            threadpool_threads,
            async_threads,
            //If we are the client, we don't want any threads to send to other clients as that will never happen
            id: Some(id.to_string()),
        };

        let _guard = unsafe { init(conf).unwrap() };
        let node_id = NodeId::from(id);

        atlas_metrics::initialize_metrics(
            vec![
                with_metrics(febft_pbft_consensus::bft::metric::metrics()),
                with_metrics(atlas_core::metric::metrics()),
                with_metrics(atlas_communication::metric::metrics()),
                with_metrics(atlas_smr_replica::metric::metrics()),
                with_metrics(atlas_log_transfer::metrics::metrics()),
                with_metrics(atlas_view_transfer::metrics::metrics()),
                with_metrics(atlas_divisible_state::metrics::metrics()),
                with_metrics(progressive_state_transfer::stp::metrics::metrics()),
                with_metric_level(MetricLevel::Info),
            ],
            influx_db_config(node_id),
        );

        if !single_server {
            main_();
        } else {
            run_single_server(node_id);
        }
    } else {
        let conf = InitConfig {
            //If we are the client, we want to have many threads to send stuff to replicas
            threadpool_threads,
            async_threads,
            //If we are the client, we don't want any threads to send to other clients as that will never happen
            id: None,
        };

        let _guard = unsafe { init(conf).unwrap() };

        let mut first_id: u32 = env::var("ID")
            .unwrap_or(String::from("1000"))
            .parse()
            .unwrap();

        atlas_metrics::initialize_metrics(
            vec![
                with_metrics(atlas_communication::metric::metrics()),
                with_metrics(atlas_client::metric::metrics()),
                with_metric_level(MetricLevel::Info),
            ],
            influx_db_config(NodeId::from(first_id)),
        );

        client_async_main();
    }
}

fn main_() {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    println!("Read configurations.");

    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(replicas_config.len())
        .enumerate()
        .map(|(id, sk)| (id as u64, sk))
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    println!("Read keys.");

    let mut pending_threads = Vec::with_capacity(4);

    let barrier = Arc::new(Barrier::new(replicas_config.len()));

    for replica in &replicas_config {
        let barrier = barrier.clone();
        let id = NodeId::from(replica.id);

        println!("Starting replica {:?}", id);

        let addrs = {
            let mut addrs = IntMap::new();

            for other in &replicas_config {
                let id = NodeId::from(other.id);
                let addr = format!("{}:{}", other.ipaddr, other.portno);
                let replica_addr = format!("{}:{}", replica.ipaddr, replica.rep_portno.unwrap());

                let (socket, host) = crate::addr!(&replica.hostname => addr);

                let replica_p_addr = PeerAddr::new(socket, host);

                addrs.insert(id.into(), replica_p_addr);
            }

            for other in &clients_config {
                let id = NodeId::from(other.id);
                let addr = format!("{}:{}", other.ipaddr, other.portno);

                let (socket, host) = crate::addr!(&other.hostname => addr);
                let client_addr = PeerAddr::new(socket, host);

                addrs.insert(id.into(), client_addr);
            }

            addrs
        };

        let sk = secret_keys.remove(id.into()).unwrap();

        println!("Setting up replica...");
        let fut = setup_replica(
            replicas_config.len(),
            id,
            sk,
            addrs,
            public_keys.clone(),
            None,
        );

        let thread = std::thread::Builder::new()
            .name(format!("Node {:?}", id))
            .spawn(move || {
                let mut replica = rt::block_on(async move {
                    println!("Bootstrapping replica #{}", u32::from(id));
                    let mut replica = fut.await.unwrap();
                    println!("Running replica #{}", u32::from(id));
                    replica
                });

                barrier.wait();

                println!("{:?} // Let's go", id);

                barrier.wait();

                replica.run().unwrap();
            })
            .unwrap();

        pending_threads.push(thread);
    }

    //We will only launch a single OS monitoring thread since all replicas also run on the same system
    //crate::os_statistics::start_statistics_thread(NodeId(0));

    drop((secret_keys, public_keys, clients_config, replicas_config));

    // run forever
    for mut x in pending_threads {
        x.join();
    }
}

fn run_single_server(id: NodeId) {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    println!("Read configurations.");

    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(replicas_config.len())
        .enumerate()
        .map(|(id, sk)| (id as u64, sk))
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    println!("Read keys.");

    let replica_id: usize = std::env::args()
        .nth(1)
        .expect("No replica specified")
        .trim()
        .parse()
        .expect("Expected an integer");

    let replica = &replicas_config[replica_id];

    let id = NodeId::from(replica.id);

    println!("Starting replica {:?}", id);

    let addrs = {
        let mut addrs = IntMap::new();

        for other in &replicas_config {
            let id = NodeId::from(other.id);
            let addr = format!("{}:{}", other.ipaddr, other.portno);
            let replica_addr = format!("{}:{}", other.ipaddr, other.rep_portno.unwrap());

            let (socket, host) = crate::addr!(&replica.hostname => addr);

            let replica_p_addr = PeerAddr::new(socket, host);

            addrs.insert(id.into(), replica_p_addr);
        }

        for client in &clients_config {
            let id = NodeId::from(client.id);
            let addr = format!("{}:{}", client.ipaddr, client.portno);

            let (socket, host) = crate::addr!(&client.hostname => addr);
            let client_addr = PeerAddr::new(socket, host);

            addrs.insert(id.into(), client_addr);
        }

        addrs
    };

    let sk = secret_keys.remove(id.into()).unwrap();

    println!("Setting up replica...");
    let fut = setup_replica(
        replicas_config.len(),
        id,
        sk,
        addrs,
        public_keys.clone(),
        None,
    );

    let mut replica = rt::block_on(async move {
        println!("Bootstrapping replica #{}", u32::from(id));
        let replica = fut.await.unwrap();
        println!("Running replica #{}", u32::from(id));
        replica
    });

    replica.run().unwrap();
    //We will only launch a single OS monitoring thread since all replicas also run on the same system
    // crate::os_statistics::start_statistics_thread(NodeId(0));

    drop((secret_keys, public_keys, clients_config, replicas_config));
}

fn client_async_main() {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    let mut first_id: u32 = env::var("ID")
        .unwrap_or(String::from("1000"))
        .parse()
        .unwrap();

    let client_count: u32 = env::var("NUM_CLIENTS")
        .unwrap_or(String::from("1"))
        .parse()
        .unwrap();

    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(clients_config.len())
        .enumerate()
        .map(|(id, sk)| (first_id as u64 + id as u64, sk))
        .chain(
            sk_stream()
                .take(replicas_config.len())
                .enumerate()
                .map(|(id, sk)| (id as u64, sk)),
        )
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    let (tx, mut rx) = channel::new_bounded_async(clients_config.len());

    for i in 0..client_count {
        let id = NodeId::from(i + first_id);

        let addrs = {
            let mut addrs = IntMap::new();
            for replica in &replicas_config {
                let id = NodeId::from(replica.id);
                let addr = format!("{}:{}", replica.ipaddr, replica.portno);
                let replica_addr = format!("{}:{}", replica.ipaddr, replica.rep_portno.unwrap());

                let (socket, host) = crate::addr!(&replica.hostname => addr);

                let replica_p_addr = PeerAddr::new(socket, host);

                addrs.insert(id.into(), replica_p_addr);
            }

            for other in &clients_config {
                let id = NodeId::from(other.id);
                let addr = format!("{}:{}", other.ipaddr, other.portno);

                let (socket, host) = crate::addr!(&other.hostname => addr);
                let client_addr = PeerAddr::new(socket, host);

                addrs.insert(id.into(), client_addr);
            }

            addrs
        };

        let sk = secret_keys.remove(id.into()).unwrap();

        let fut = setup_client(
            replicas_config.len(),
            id,
            sk,
            addrs,
            public_keys.clone(),
            None,
        );

        let mut tx = tx.clone();

        rt::spawn(async move {
            println!("Bootstrapping client #{}", u32::from(id));
            let client = fut.await.unwrap();
            println!("Done bootstrapping client #{}", u32::from(id));
            tx.send(client).await.unwrap();
        });
    }

    drop((secret_keys, public_keys, replicas_config));

    let mut clients = Vec::with_capacity(client_count as usize);

    for _i in 0..client_count {
        clients.push(rt::block_on(rx.recv()).unwrap());
    }

    //We have all the clients, start the OS resource monitoring thread
    //crate::os_statistics::start_statistics_thread(NodeId(first_cli));

    let mut handles = Vec::with_capacity(client_count as usize);
    let keypool = generate_key_pool(100000);
    let generator = Arc::new(Generator::new(keypool, 100000));

    for client in clients {
        let id = client.id();
        let gen = generator.clone();
        // generate_log(id.0);
        let cli_len = client_count as usize;

        let h = std::thread::Builder::new()
            .name(format!("Client {:?}", client.id()))
            .spawn(move || run_client(client, gen, cli_len))
            .expect(format!("Failed to start thread for client {:?} ", &id.id()).as_str());

        handles.push(h);

        // Delay::new(Duration::from_millis(5)).await;
    }

    drop(clients_config);

    for h in handles {
        let _ = h.join();
    }
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}

fn run_client(client: SMRClient, generator: Arc<Generator>, n_clients: usize) {
    let id = client.id().0.clone();
    let concurrent_requests = get_concurrent_rqs();
    let concurrent_client = ConcurrentClient::from_client(client, concurrent_requests).unwrap();
  //  let sem = Arc::new(RawSemaphore::new(concurrent_requests));

    let mut rand = SplitMix64::seed_from_u64((6453 + (id * 1242)).into());
    //let rounds = NUM_KEYS / n_clients;
   // let rem = NUM_KEYS % n_clients;
    //loading phase first
    /* println!(" number of loading rounds {:?} with remainder {:?}", rounds, rem);
        for i in 0..rounds {

            if let Some(key) = generator.get(i*n_clients + id as usize) {
            let map = generate_kv_pairs(&mut rand);

            let ser_map = bincode::serialize(&map).expect("failed to serialize map");
            let req = Action::Insert(key.as_bytes().to_vec(),ser_map);
            let _res = rt::block_on(concurrent_client.update::<Ordered>(Arc::from(req))).expect("error");
            } else {
                println!("No key with idx {:?}", i+id as usize);
            }
        }

        if id == 1 {
            for i in 0..rem {
            if let Some(key) = generator.get(rounds*n_clients + i as usize) {
                let map = generate_kv_pairs(&mut rand);

                let ser_map = bincode::serialize(&map).expect("failed to serialize map");
                let req = Action::Insert(key.as_bytes().to_vec(),ser_map);
                let _res = rt::block_on(concurrent_client.update::<Ordered>(Arc::from(req))).expect("error");
                } else {
                    println!("No key with idx {:?}", i+id as usize);
                }
            }
        }
    */
    for _ in 0..10000000 as u64 {
        let key = generator.get_key_zipf(&mut rand);
        let map = generate_kv_pairs(&mut rand);

        let ser_map = bincode::serialize(&map).expect("failed to serialize map");
        let req = Action::Insert(key.as_bytes().to_vec(),ser_map);
        println!("update");
        let _res = rt::block_on(concurrent_client.update::<Ordered>(Arc::from(req))).expect("error");
    }
}
