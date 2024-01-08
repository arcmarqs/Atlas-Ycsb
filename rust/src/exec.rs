use std::sync::Arc;

use atlas_common::error::*;
use atlas_smr_application::app::{Application, Request, Reply, UnorderedBatch, BatchReplies, UpdateBatch};



use crate::serialize::{KvData, self, State};

#[derive(Default)]
pub struct KVApp;

impl Application<State> for KVApp {
    type AppData = KvData;

    fn initial_state() -> Result<State> {
     
        let state = State::new();   
                
        Ok(state)
    }

fn unordered_execution(&self, state: &State, request: Request<Self, State>) -> Reply<Self, State> {
        todo!()
    }

    fn update(
        &self,
        state: &mut State,
        request: Request<Self, State>,
    ) -> Reply<Self, State> {
        let reply_inner = match request.as_ref() {
            serialize::Action::Read(key) => {
                match state.db.get(key) {
                    Some(vec) => {

                        serialize::Reply::Single(vec.to_owned())
                    },
                    None => serialize::Reply::None,
                }

            }
            serialize::Action::Insert(key, value) => {
                
                match state.db.insert(key.to_owned(), value.to_owned()) {
                    Some(vec) => {

                        serialize::Reply::Single(vec.to_owned())
                    },
                    None => serialize::Reply::None,
                }
            }
            serialize::Action::Remove(key) => { 

                match state.db.remove(key) {
                    Some(vec) => {

                        serialize::Reply::Single(vec.to_owned())
                    },
                    None => serialize::Reply::None,
                }
            }
        };

       // state.db.flush();
        Arc::new(reply_inner)
    }

fn update_batch(
        &self,
        state: &mut State,
        batch: UpdateBatch<Request<Self, State>>,
    ) -> BatchReplies<Reply<Self, State>> {
        let mut reply_batch = BatchReplies::with_capacity(batch.len());

        for update in batch.into_inner() {
            let (peer_id, sess, opid, req) = update.into_inner();
            let reply = self.update(state, req);
            reply_batch.add(peer_id, sess, opid, reply);
        }

        reply_batch
    }

fn unordered_batched_execution(
        &self,
        state: &State,
        requests: UnorderedBatch<Request<Self, State>>,
    ) -> BatchReplies<Reply<Self, State>> {
        let mut reply_batch = BatchReplies::with_capacity(requests.len());

        for unordered_req in requests.into_inner() {
            let (peer_id, sess, opid, req) = unordered_req.into_inner();
            let reply = self.unordered_execution(&state, req);
            reply_batch.add(peer_id, sess, opid, reply);
        }

        reply_batch
    }
}