use std::error::Error;
use std::{fs::File, iter::once};
use std::collections::HashMap;
use std::io;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{distributions::{Distribution, Standard, DistString}, Rng};
use rand_xoshiro::{self, SplitMix64};
use rand_core::SeedableRng;
use rand_distr::Alphanumeric;
use csv;
use rand_seeder::Seeder;

const PRIMARY_KEY_LEN: usize = 12;
const SECONDARY_KEY_LEN: usize = 6;
const VALUE_LEN: usize = 6;
const HASHMAP_LEN: usize = 1;

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


fn generate_keys(num_keys: usize, path: &str) -> Result<(),std::io::Error> {
    let keys = File::create(path)?;
    let wtr = io::BufWriter::new(keys);
    let mut csv_writer = csv::Writer::from_writer(wtr);
    let mut rand = SplitMix64::seed_from_u64(528);

    for _i in 0..num_keys {
        let key: String = Alphanumeric.sample_string(&mut rand, PRIMARY_KEY_LEN);
        csv_writer.write_record(once(key.as_str()))?;
    }    
    
    csv_writer.flush()
}

fn generate_entries(num_entries: usize, path: &str) -> Result<(),std::io::Error> {
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

fn create_records(num_records: usize, key_path: &str, entry_path: &str) -> Result<Vec<Record>,std::io::Error> {
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

fn main() {
    let keys_path = "keys.csv";
    let entries_path = "entries.csv";

    generate_entries(100, entries_path).expect("fail");
    generate_keys(100, keys_path).expect("fail");

    let recs = create_records(10,keys_path,entries_path).expect("fail");
    println!("{:?}",recs);
}
