use std::process::exit;

mod generator;
mod local;
mod common;
mod exec;
mod cop;
mod serialize;
mod os_statistics;

//#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

//#[cfg(not(target_env = "msvc"))]
//#[global_allocator]
//static GLOBAL: Jemalloc = Jemalloc;


#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
    let mut state: BTreeMap<String,HashMap<String,String>> = BTreeMap::new();
    let kp = generate_key_pool(1000);
    let mut opcount = [0;4];
    let mut freq_count: BTreeMap<String,i32> = BTreeMap::new();
    let mut rand = thread_rng();
    let generator = Generator::new(Arc::new(kp),1000);
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
                let map = generate_kv_pairs();
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
                let map = generate_kv_pairs();
                state.insert(key, map);
                opcount[3] += 1
            },
        }
    }

    println!("OPCOUNT {:?}", opcount);
    println!("{:?}",state);
}

*/
