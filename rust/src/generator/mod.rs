use std::iter::once;
use std::collections::HashMap;
use rand::{distributions::DistString, Rng};
use rand_xoshiro::{self, SplitMix64};
use rand_core::SeedableRng;
use rand_distr::{Alphanumeric, Distribution, Standard, WeightedIndex, Zipf, Uniform};
use sharded_slab::Pool;
const PRIMARY_KEY_LEN: usize = 8;
const SECONDARY_KEY_LEN: usize = 6;
const VALUE_LEN: usize = 6;
const HASHMAP_LEN: usize = 1;

// for more "randomness" in the distribution this should be between  ]0.0,0.24[
const ZIPF_CONSTANT: f64 = 0.0;
#[derive(Debug)]
pub struct Generator {
    pool: Pool<String>,
    distribution: Zipf<f64>,
    size: u64,
}

impl Generator {
    pub fn new(pool: Pool<String>, size: u64) -> Self {
        let distribution = Zipf::new(size, ZIPF_CONSTANT).expect("fail");
        Self {
            pool,
            distribution,
            size,
        }
    }

    //get a random Zipfian distributed key, if the zipfian constant is 0, the distribution will be uniform, constants > ~.25 will start to behave more like an exponential distribution.
    pub fn get_key_zipf<R: Rng + ?Sized>(&self, rng: &mut R) -> String {
        // the distribution generates integers starting at 1 while the indexes of the Pool start at 0.
        let index = (self.distribution.sample(rng) - 1.0) as usize;
        let key = self.pool.get(index);

        key.unwrap().clone()
    }

    //get a random, uniformly distributed key
    pub fn get_rand_key<R: Rng + ?Sized>(&self, rng: &mut R) -> String {
        self.pool.get(Uniform::new(0, self.size).sample(rng) as usize).unwrap().clone()
    }
}



#[derive(Debug)]
pub struct Entry {
    key: String,
    value: String,
}

impl Entry {
    pub fn new(key: String, value: String) -> Self {
        Self {
            key,
            value,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        once(self.key.as_str()).chain(once(self.value.as_str()))
    }
}

impl Distribution<Entry> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Entry {
        let key = Alphanumeric.sample_string(rng, SECONDARY_KEY_LEN);
        let value = Alphanumeric.sample_string(rng, VALUE_LEN); 
        Entry::new(key, value)
    }
}

pub fn generate_key_pool(num_keys: usize) -> Pool<String> {

    let pool: Pool<String> = Pool::new();
    let mut rand = SplitMix64::seed_from_u64(528);
    for _ in 0..num_keys {
        let _ = pool.create_with(|s| s.push_str(Alphanumeric.sample_string(&mut rand, PRIMARY_KEY_LEN).as_str())).unwrap();
    }    

    pool
}

pub fn generate_kv_pairs<R: Rng>(rand: &mut R) -> HashMap<String,String> {
    let mut map: HashMap<String,String> = HashMap::new();

    for _ in 0..HASHMAP_LEN {
        let entry: Entry = rand.gen();
        map.insert(entry.key, entry.value);
    }

    map

}

#[derive(Debug,Clone)]
pub enum Operation {
    Read,
    Insert,
    Remove,
    Update,
}

impl Distribution<Operation> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Operation {
        let w = WeightedIndex::new([READ_OPS, INSERT_OPS, REMOVE_OPS, UPDATE_OPS]).expect("error creating weighted distribution");

        match w.sample(rng) {
            0 => Operation::Read,
            1 => Operation::Insert,
            2 => Operation::Remove,
            3 => Operation::Update,
            _ => panic!("INVALID OPERATION"),
        }
    }
}

/* 
pub fn generate_keys(num_keys: usize, path: &str) -> Result<(),std::io::Error> {
    let keys = File::create(path)?;
    let wtr = io::BufWriter::new(keys);
    let mut csv_writer = csv::Writer::from_writer(wtr);
    let mut rand = SplitMix64::seed_from_u64(123523);

    for _i in 0..num_keys {
        let key: String = Alphanumeric.sample_string(&mut rand, PRIMARY_KEY_LEN);
        csv_writer.write_record(once(key.as_str()))?;
    }    
    
    csv_writer.flush()
}

pub fn generate_entries(num_entries: usize, path: &str) -> Result<(),std::io::Error> {
    let entries = File::create(path)?;
    let wtr = io::BufWriter::new(entries);
    let mut csv_writer = csv::Writer::from_writer(wtr);
    let mut rand = SplitMix64::seed_from_u64(725145);

    for _i in 0..num_entries {
        let entry: Entry = rand.gen();
        csv_writer.write_record(entry.iter())?;
    }    
    
    csv_writer.flush()
}

#[derive(Debug)]
pub struct Record {
    key: String,
    values: HashMap<String, String>
}

impl Record {
    fn new(key: String, values: HashMap<String,String>) -> Self {
        Self {
            key,
            values
        }
    }
}

pub fn create_records(num_records: usize, key_path: &str, entry_path: &str) -> Result<Vec<Record>,std::io::Error> {
    let mut records: Vec<Record> = Vec::new();
    let mut key_rdr = csv::Reader::from_path(key_path)?;
    let mut value_rdr = csv::Reader::from_path(entry_path)?;

    let mut rand = SplitMix64::seed_from_u64(536);

    let binding = key_rdr.records().choose_multiple(&mut rand,num_records);
    let key_iter = binding.iter().map(|rec| rec.as_ref().expect("fail"));
    let val_iter = value_rdr.records().collect::<Vec<_>>();
    for key in key_iter {
        let mut map: HashMap<String,String> = HashMap::new();
        let key = key.get(0).unwrap().to_owned();
        let mut rng: SplitMix64 = Seeder::from(key.as_str()).make_rng();
        let value = val_iter.as_slice().choose_multiple(&mut rng, HASHMAP_LEN);
        let value_iter = value.map(|rec| rec.as_ref());
        map.extend(value_iter.map(|val| (val.expect("fail").get(0).unwrap().to_string(),val.expect("fail").get(1).unwrap().to_string())));

        records.push(Record::new(key,map))
    }

    Ok(records)
}
*/