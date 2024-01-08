use std::collections::BTreeMap;
use std::sync::Arc;
use std::default::Default;
use std::io::{Read, Write};
use anyhow::Context;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_application::state::monolithic_state::MonolithicState;
use serde::{Serialize, Deserialize};


use atlas_common::error::*;


pub struct KvData;

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum Action {
    Read(Vec<u8>),
    Insert(Vec<u8>,Vec<u8>),
    Remove(Vec<u8>)
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum Reply {
    None,
    Single(Vec<u8>),
}

#[derive(Clone,Serialize,Deserialize)]
pub struct State {
    pub db: BTreeMap<Vec<u8>,Vec<u8>>,
}

impl State {
    pub fn new() -> Self {
        Self { db: BTreeMap::new()
    }
    }
}

impl MonolithicState for State {
    fn serialize_state<W>(mut w: W, request: &Self) -> Result<()> where W: Write {
        let buf = bincode::serialize(request).expect("failed to serialize");
        w.write_all(buf.as_ref()).context("Failed to serialize state")
    }

    fn deserialize_state<R>(r: R) -> Result<Self> where R: Read, Self: Sized {
        let buf = r.bytes().map(|b| b.expect("failed to read byte")).collect::<Vec<_>>();
        let state: BTreeMap<Vec<u8>, Vec<u8>> = bincode::deserialize(&buf).expect("Failed to deserialize");
        Ok(State {db: state})
    }

    fn size(&self) -> usize {
        let first = self.db.first_key_value().map(|(key,value)| key.len() + value.len()).expect("failed sum");

        self.db.len() * first
    }
}

impl ApplicationData for KvData{
    type Request = Arc<Action>;
    type Reply = Arc<Reply>;

    fn serialize_request<W>(w: W, request: &Self::Request) -> Result<()> where W: Write {
        let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

        let mut rq_msg: messages_capnp::request::Builder = root.init_root();
    
        rq_msg.set_action(bincode::serialize(request).unwrap().as_slice());

        capnp::serialize::write_message(w, &root)
            .context("Failed to serialize request")
    }

    fn deserialize_request<R>(r: R) -> Result<Self::Request> where R: Read {

        let reader = capnp::serialize::read_message(r, Default::default()).context("Failed to read message")?;

        let request_msg : messages_capnp::request::Reader = reader.get_root()
            .context("Failed to read request message")?;

            let _data = request_msg.get_action().context("Failed to get data")?;
            let action : Action = bincode::deserialize(_data).context("Failed to get data")?;
            Ok(Arc::new(action))
        
    }

    fn serialize_reply<W>(w: W, reply: &Self::Reply) -> Result<()> where W: Write {
        let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

        let mut rq_msg: messages_capnp::reply::Builder = root.init_root();
        rq_msg.set_data(bincode::serialize(reply).unwrap().as_slice());
      
        capnp::serialize::write_message(w, &root)
            .context("Failed to serialize reply")
    }

    fn deserialize_reply<R>(r: R) -> Result<Self::Reply> where R: Read {

        let reader = capnp::serialize::read_message(r, Default::default()).context("Failed to read message")?;

        let request_msg : messages_capnp::reply::Reader = reader.get_root().context("Failed to read reply message")?;

        let _data = request_msg.get_data().context("Failed to read reply message")?;
        let rep: Reply = bincode::deserialize(_data).context("Failed to read reply message")?;
        Ok(Arc::new(rep))
    }
}

mod messages_capnp {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}