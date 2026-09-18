//! Turns a samply profile into the hotspot records `report.sh` renders.
//!
//! ```text
//! gunzip -c profile.json.gz | hotspots profile.json.syms.json <scenario> [top]
//! ```
//!
//! The profile is read from stdin so nothing here has to deal with gzip. Samples whose leaf is a
//! park are counted separately and excluded: a parked worker is not spending time, it is spare
//! capacity, and leaving it in buries every real frame.

use std::collections::HashMap;
use std::env;
use std::fs::read_to_string;
use std::io::Read as _;
use std::io::stdin;

use serde_json::Value;

const PARK: [&str; 2] = ["__psynch_cvwait", "__psynch_mutexwait"];

struct Symbol {
    rva: u64,
    end: u64,
    name: usize,
}

fn main() {
    let mut args = env::args().skip(1);
    let syms_path = args.next().expect("usage: hotspots <syms.json> <scenario> [top]");
    let scenario = args.next().unwrap_or_else(|| "unknown".to_owned());
    let top: usize = args.next().map_or(15, |value| value.parse().expect("invalid top"));

    let mut profile = String::new();
    stdin().read_to_string(&mut profile).expect("failed to read profile from stdin");
    let profile: Value = serde_json::from_str(&profile).expect("failed to parse profile");
    let syms: Value = serde_json::from_str(&read_to_string(&syms_path).expect("failed to read syms"))
        .expect("failed to parse syms");

    let strings: Vec<&str> = syms["string_table"].as_array().expect("string_table").iter().map(as_str).collect();
    let symbols = symbol_tables(&syms);
    let lib_names: Vec<&str> = profile["libs"]
        .as_array()
        .expect("libs")
        .iter()
        .map(|lib| lib["debugName"].as_str().unwrap_or("unknown"))
        .collect();

    let mut self_samples: HashMap<String, u64> = HashMap::new();
    let (mut on_cpu, mut parked) = (0_u64, 0_u64);

    for thread in profile["threads"].as_array().expect("threads") {
        let local: Vec<&str> =
            thread["stringArray"].as_array().expect("stringArray").iter().map(as_str).collect();
        let func_name = ints(&thread["funcTable"]["name"]);
        let func_resource = &thread["funcTable"]["resource"];
        let resource_lib = ints(&thread["resourceTable"]["lib"]);
        let frame_func = ints(&thread["frameTable"]["func"]);
        let frame_address = &thread["frameTable"]["address"];
        let stack_frame = ints(&thread["stackTable"]["frame"]);
        let mut names: HashMap<usize, String> = HashMap::new();

        for sample in thread["samples"]["stack"].as_array().expect("samples") {
            let Some(node) = sample.as_u64() else { continue };
            let frame = stack_frame[node as usize];
            let name = names.entry(frame).or_insert_with(|| {
                let func = frame_func[frame];
                match func_resource[func].as_u64() {
                    Some(resource) => {
                        let lib = lib_names[resource_lib[resource as usize]];
                        let address = frame_address[frame].as_u64().unwrap_or(0);
                        resolve(&symbols, &strings, lib, address)
                    }
                    None => local[func_name[func]].to_owned(),
                }
            });
            if PARK.contains(&name.as_str()) {
                parked += 1;
            } else {
                on_cpu += 1;
                *self_samples.entry(name.clone()).or_default() += 1;
            }
        }
    }

    let mut ranked: Vec<(String, u64)> = self_samples.into_iter().collect();
    ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let total = on_cpu + parked;
    let parked_pct = if total > 0 { 100.0 * parked as f64 / total as f64 } else { 0.0 };
    // name goes last, it is the only field that can contain spaces
    for (rank, (name, samples)) in ranked.into_iter().take(top).enumerate() {
        println!(
            "hotspot scenario={scenario} rank={} self_pct={:.2} samples={samples} on_cpu={on_cpu} \
             parked_pct={parked_pct:.1} name={name}",
            rank + 1,
            100.0 * samples as f64 / on_cpu as f64
        );
    }
}

fn as_str(value: &Value) -> &str {
    value.as_str().unwrap_or("")
}

fn ints(value: &Value) -> Vec<usize> {
    value.as_array().map_or_else(Vec::new, |array| {
        array.iter().map(|v| v.as_u64().unwrap_or(0) as usize).collect()
    })
}

fn symbol_tables(syms: &Value) -> HashMap<String, Vec<Symbol>> {
    let mut tables = HashMap::new();
    for lib in syms["data"].as_array().expect("data") {
        let name = lib["debug_name"].as_str().unwrap_or("unknown").to_owned();
        let mut symbols: Vec<Symbol> = lib["symbol_table"]
            .as_array()
            .expect("symbol_table")
            .iter()
            .map(|entry| {
                let rva = entry["rva"].as_u64().unwrap_or(0);
                // the innermost inlined frame is the one the sample is really in
                let name = entry["frames"]
                    .as_array()
                    .and_then(|frames| frames.first())
                    .and_then(|frame| frame["function"].as_u64())
                    .or_else(|| entry["symbol"].as_u64())
                    .unwrap_or(0) as usize;
                Symbol { rva, end: rva + entry["size"].as_u64().unwrap_or(0), name }
            })
            .collect();
        symbols.sort_unstable_by_key(|symbol| symbol.rva);
        tables.insert(name, symbols);
    }
    tables
}

fn resolve(tables: &HashMap<String, Vec<Symbol>>, strings: &[&str], lib: &str, address: u64) -> String {
    let Some(symbols) = tables.get(lib) else {
        return format!("{lib}+{address:#x}");
    };
    let index = symbols.partition_point(|symbol| symbol.rva <= address);
    if index == 0 {
        return format!("{lib}+{address:#x}");
    }
    let symbol = &symbols[index - 1];
    if address >= symbol.end { format!("{lib}+{address:#x}") } else { strings[symbol.name].to_owned() }
}
