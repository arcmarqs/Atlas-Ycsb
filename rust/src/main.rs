use std::{collections::{BTreeMap, HashMap}};

use rand::{ Rng, SeedableRng};
use rand_xoshiro::SplitMix64;

use crate::generator::{generate_kv_pairs, generate_monotonic_keypool, Generator, Operation};

mod generator;
mod local;
mod common;
mod exec;
mod cop;
mod serialize;

//#[cfg(not(target_env = "msvc"))]
//use tikv_jemallocator::Jemalloc;

//#[cfg(not(target_env = "msvc"))]
//#[global_allocator]
//static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    let is_local = std::env::var("LOCAL")
        .map(|x| x == "1")
        .unwrap_or(false);

    println!("Starting local? {}", is_local);

    if is_local {
        local::main()
    } else {
        cop::main()
    }
}


/* 
fn main() {
    let mut state: BTreeMap<Vec<u8>,HashMap<String,String>> = BTreeMap::new();
    let kp = generate_monotonic_keypool(10000);
    let mut opcount = [0;4];
    let mut freq_count: BTreeMap<Vec<u8>,i32> = BTreeMap::new();
    let mut rand = SplitMix64::seed_from_u64(6453343);
    let generator = Generator::new(kp, 10000);
    for _ in 0..1000 {
        let key = generator.get_key_zipf(&mut rand);
        freq_count.entry(key.clone()).and_modify(|val| {*val += 1}  ).or_insert(0);
        let action: Operation = rand.gen();

        match &action {
            Operation::Read => {
                println!("read {:?} res {:?}", &key, state.get(&key));
                opcount[0] += 1
            },
            Operation::Insert =>{
                let map = generate_kv_pairs(&mut rand);
                println!("insert {:?} {:?}", &key, map);
                state.insert(key, map);
                opcount[1] += 1
            },
            Operation::Remove =>{ 
                println!("remove {:?} res {:?}", &key, state.remove(&key));
                opcount[2] += 1
            },
            Operation::Update => {
                println!("update {:?} res {:?}", &key, state.get(&key));
                let map = generate_kv_pairs(&mut rand);
                state.insert(key, map);
                opcount[3] += 1
            },
        }
    }

    println!("OPCOUNT {:?}", opcount);
    println!("{:?}",state);
}*/