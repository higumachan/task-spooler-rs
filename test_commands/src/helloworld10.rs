use std::env;
use std::thread::sleep;
use std::time::Duration;
use std::str::FromStr;

fn main() {
    for i in 0..10 {
        println!("helloworld{}", i);
        let c: Vec<String> = env::args().collect();
        sleep(Duration::from_secs(u64::from_str(&c[1]).unwrap()));
    }
}