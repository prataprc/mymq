use std::io;
use std::{io::prelude::*, net, thread, time};

fn main_listener() {
    let listener = net::TcpListener::bind("0.0.0.0:34254").unwrap();
    // accept connections and process them serially
    let mut streams = vec![];
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        streams.push(stream);
    }
    println!("exit");
}

fn main() {
    thread::spawn(|| main_listener());
    thread::sleep(time::Duration::from_secs(1));

    let mut clients = vec![];
    for _ in 0..1024 {
        let client =
            mio::net::TcpStream::connect("127.0.0.1:34254".parse().unwrap()).unwrap();
        clients.push(client);
    }
    println!("Created {} clients", clients.len());

    let start = time::Instant::now();
    for client in clients.iter_mut() {
        match client.read(&mut [0; 128]) {
            Ok(n) => println!("read {} bytes", n),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => (),
            Err(err) => println!("err: {}", err),
        }
    }
    println!(
        "Read completed for {} clients, elapsed {:?}",
        clients.len(),
        start.elapsed()
    );
}
