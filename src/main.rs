use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;
use std::time::{Instant, Duration};

struct Storage {
    data: HashMap<String, String>,
    expirations: HashMap<String, Instant>,
}

impl Storage {
    fn new() -> Self {
        Storage {
            data: HashMap::new(),
            expirations: HashMap::new(),
        }
    }

    fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    fn set_ex(&mut self, key: String, value: String, milliseconds: u64) {
        self.data.insert(key.clone(), value);
        let expiration_time = Instant::now() + Duration::from_millis(milliseconds);
        self.expirations.insert(key, expiration_time);
    }

    fn get(&mut self, key: &str) -> Option<&String> {
        if let Some(expiration_time) = self.expirations.get(key) {
            if Instant::now() >= *expiration_time {
                self.data.remove(key);
                self.expirations.remove(key);
                None
            } else {
                self.data.get(key)
            }
        } else {
            self.data.get(key)
        }
    }
}

async fn accept_connection(stream: &mut TcpStream) {
    let mut buf = [0; 512];
    let mut storage = Storage::new();

    loop {
        let value = stream.read(&mut buf).await;
        if let Ok(value) = value {
            let tokens = String::from_utf8_lossy(&buf[0..value]);
            let commands: Vec<&str> = tokens.split("\\r\\n").collect();
            if let Some(command) = commands.get(2) {
                match command {
                    &"ping" => send_message(stream, "+PONG\r\n").await,
                    &"echo" => {
                        let response = format!("+{}\r\n", commands.get(4).unwrap());
                        send_message(stream, &response).await;
                    }
                    &"set" => {
                        if let Some(expiration) = commands.get(10) {
                            storage.set_ex(
                                commands.get(4).expect("No key provided").to_string(),
                                commands.get(6).expect("No value provided").to_string(),
                                expiration.parse::<u64>().unwrap(),
                            );
                        } else {
                            storage.set(
                                commands.get(4).expect("No key provided").to_string(),
                                commands.get(6).expect("No value provided").to_string(),
                            );
                        }
                        send_message(stream, "+OK\r\n").await;
                    }
                    &"get" => {
                        if let Some(value) = storage.get(commands.get(4).expect("No key provided"))
                        { send_message(stream, &format!("+{}\r\n", value)).await; }
                        else { send_message(stream, "$-1\r\n").await; } 
                    },
                    _ => send_message(stream, "-ERR\r\n").await,
                }
            } else { send_message(stream, "-ERR\r\n").await; }
        } else {
            println!("Disconnected");
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening on 127.0.0.1:6379");
    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("New connection");
                tokio::spawn(async move { accept_connection(&mut stream).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn send_message(stream: &mut TcpStream, message: &str) {
    stream.write(message.as_bytes()).await.unwrap();
}