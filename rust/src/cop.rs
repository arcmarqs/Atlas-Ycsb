use std::env;
use std::env::args;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use anyhow::Context;
use atlas_common::collections::HashMap;
use atlas_common::error::*;
use intmap::IntMap;
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
use rand::Rng;

use atlas_client::client::ordered_client::Ordered;
use atlas_client::concurrent_client::ConcurrentClient;
use atlas_common::crypto::signature::{KeyPair, PublicKey};
use atlas_common::node_id::NodeId;
use atlas_common::peer_addr::PeerAddr;
use atlas_common::{async_runtime as rt, channel, init, InitConfig};
use atlas_metrics::{with_metric_level, with_metrics, MetricLevel};
use rand_core::SeedableRng;
use rand_distr::Standard;
use rand_xoshiro::SplitMix64;
use semaphores::RawSemaphore;

use crate::common::*;
use crate::generator::{generate_key_pool, generate_kv_pairs, generate_monotonic_keypool, Generator, Operation};
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
        false
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
        .build(Root::builder().appender("file").build(LevelFilter::Warn))
        .context("MsgLog Error")
        .unwrap();

    let _handle = log4rs::init_config(config).context("MsgLog Error").unwrap();
}

pub fn main() {
    let is_client: bool = std::env::var("CLIENT").map(|x| x == "1").unwrap_or(false);

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

    if !is_client {
        let id: u32 = std::env::args()
            .nth(1)
            .expect("No replica specified")
            .trim()
            .parse()
            .expect("Expected an integer");

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

        main_(node_id);
    } else {
        let conf = InitConfig {
            //If we are the client, we want to have many threads to send stuff to replicas
            threadpool_threads,
            async_threads,
            //If we are the client, we don't want any threads to send to other clients as that will never happen
            id: None,
        };

        let _guard = unsafe { init(conf).unwrap() };

        let first_id: u32 = env::var("ID")
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

fn main_(id: NodeId) {
    let mut replica = {
        println!("Started working on the replica");

        //TODO: don't have this hardcoded?
        let first_cli = NodeId::from(1000u32);

        let clients_config = parse_config("./config/clients.config").unwrap();
        let replicas_config = parse_config("./config/replicas.config").unwrap();

        println!("Finished reading replica config.");

        let mut secret_keys: IntMap<KeyPair> = sk_stream()
            .take(clients_config.len())
            .enumerate()
            .map(|(id, sk)| (u64::from(first_cli) + id as u64, sk))
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

        println!("Finished reading keys.");

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

        /* let comm_stats = Some(Arc::new(CommStats::new(id,
        first_cli,
        MicrobenchmarkData::MEASUREMENT_INTERVAL))); */

        let comm_stats = None;

        let sk = secret_keys.remove(id.into()).unwrap();

        let fut = setup_replica(
            replicas_config.len(),
            id,
            sk,
            addrs,
            public_keys.clone(),
            comm_stats,
        );

        println!("Bootstrapping replica #{}", u32::from(id));
        let replica = rt::block_on(fut).unwrap();
        println!("Running replica #{}", u32::from(id));

        //Here we want to launch a statistics thread for each replica since they are on different machines
        //crate::os_statistics::start_statistics_thread(id);

        replica
    };

    // run forever
    replica.run().unwrap();
}

fn client_async_main() {
    let clients_config = parse_config("./config/clients.config").unwrap();
    let replicas_config = parse_config("./config/replicas.config").unwrap();

    let arg_vec: Vec<String> = args().collect();

    let default = String::from("1000");

    println!("arg_vec: {:?}", arg_vec);

    let mut first_id: u32 = env::var("ID")
        .unwrap_or(String::from("1000"))
        .parse()
        .unwrap();

    let client_count: u32 = env::var("NUM_CLIENTS")
        .unwrap_or(String::from("1"))
        .parse()
        .unwrap();

    //let client_count: u32 = env::var("NUM_CLIENTS").unwrap_or(String::from("1")).parse().unwrap();

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

    let (tx, mut rx) = channel::new_bounded_async(8);

    generate_log(1000);
    let comm_stats = None;
    for i in 0..client_count {
        let id = NodeId::from(first_id + i);

        //(id.0 as u32);

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
            comm_stats.clone(),
        );

        let mut tx = tx.clone();
        rt::spawn(async move {
            //We can have this async initializer no problem, just don't want it to be used to actually send
            //The requests/control the clients
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
    let num_keys = get_num_keys();
    let mut handles = Vec::with_capacity(client_count as usize);
    let keypool = generate_monotonic_keypool(num_keys.clone());
    let generator = Arc::new(Generator::new(keypool, num_keys.try_into().unwrap()));

    for client in clients {
        let id = client.id();
        let gen = generator.clone();
        //generate_log(id.0);
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
    let sem = Arc::new(RawSemaphore::new(concurrent_requests));

    let mut rand = SplitMix64::seed_from_u64((6453 + (id * 1242)).into());
    let rounds = get_num_keys() / n_clients;
    let rem = get_num_keys() % n_clients;
    //loading phase first
    println!(
        "client {:?} loading {:?} rounds with {:?} remainder",
        id, rounds, rem
    );

    for i in 0..rounds {
        if let Some(key) = generator.get(i * n_clients + id as usize) {
            let map = generate_kv_pairs(&mut rand);
            let ser_map = bincode::serialize(&map).expect("failed to serialize map");
            let req = Action::Insert(key.into(), ser_map);
            sem.acquire();

            let sem_clone = sem.clone();

            concurrent_client
                .update_callback::<Ordered>(
                    Arc::from(req),
                    Box::new(move |_rep| {
                        sem_clone.release();
                    }),
                )
                .expect("error");
        } else {
            println!("No key with idx {:?}", i * n_clients + id as usize);
        }
    }

    if id == 1 {
        for i in 0..rem {
            if let Some(key) = generator.get(rounds * n_clients + i as usize) {
                let map = generate_kv_pairs(&mut rand);

                let ser_map = bincode::serialize(&map).expect("failed to serialize map");
                let req = Action::Insert(key.into(), ser_map);
                sem.acquire();

                let sem_clone = sem.clone();

                concurrent_client
                    .update_callback::<Ordered>(
                        Arc::from(req),
                        Box::new(move |_rep| {
                            sem_clone.release();
                        }),
                    )
                    .expect("error");
            } else {
                println!("No key with idx {:?}", rounds * n_clients + i as usize);
            }
        }
    }


    for _ in 0..9000000000 as usize {
        let key = generator.get_key_zipf(&mut rand);

        /*    let request = match &op {
            Operation::Read => {
              //  println!("Read {:?}",&ser_key);
                Action::Read(ser_key)
            },
            Operation::Insert =>{
                let map = generate_kv_pairs(&mut rand);
               // println!("Insert {:?} {:?}", &key,&map);
                let ser_map = bincode::serialize(&map).expect("failed to serialize map");
                Action::Insert(ser_key,ser_map)
            },
            Operation::Remove =>{
                //println!("Remove {:?}",&ser_key);

                Action::Remove(ser_key)

            },
            Operation::Update => {

                let map = generate_kv_pairs(&mut rand);
                println!("Update {:?}",&key);

                let ser_map = bincode::serialize(&map).expect("failed to serialize map");
                Action::Insert(ser_key,ser_map)
            },
        };*/
        let map = generate_kv_pairs(&mut rand);

        let ser_map = bincode::serialize(&map).expect("failed to serialize map");
        println!("Update {:?}", &key);

        let req = Action::Insert(key.into(), ser_map);
        sem.acquire();

        let sem_clone = sem.clone();

        concurrent_client
            .update_callback::<Ordered>(
                Arc::from(req),
                Box::new(move |_rep| {
                    println!("Repl {:?}", &_rep);

                    sem_clone.release();
                }),
            )
            .expect("error");
        /*let _ = match rt::block_on(concurrent_client.update::<Ordered>(Arc::from(request)).expect("error").as_ref() {
            crate::serialize::Reply::None => None,
            crate::serialize::Reply::Single(bytes) =>{
            let map: HashMap<String,String> = bincode::deserialize(&bytes).expect("failed to deserialize reply");
            Some(map)
            },
        };*/

        //println!("Reply: {:?}", &res);
    }

    println!("completed all ops");
}

