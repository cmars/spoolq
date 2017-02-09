extern crate futures;
extern crate notify;
extern crate serde;
extern crate serde_json;
extern crate textnonce;
extern crate chrono;

use std::sync::mpsc;
use std::error::Error as StdError;
use std::ops::Drop;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

use serde::{Serialize, Deserialize};

pub struct Queue<T> {
    path: String,
    _placeholder: std::marker::PhantomData<T>,
}

impl<T: Serialize + Deserialize> Queue<T> {
    pub fn new(path: &str) -> Result<Queue<T>, std::io::Error> {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path)?;
        Ok(Queue::<T> {
            path: path.to_string(),
            _placeholder: std::marker::PhantomData,
        })
    }

    pub fn push(&self, item: T) -> Result<(), std::io::Error> {
        let mut item_path = std::path::PathBuf::from(&self.path);
        let item_name = format!("{}-{}",
                                chrono::UTC::now().timestamp(),
                                textnonce::TextNonce::sized_urlsafe(32).unwrap().into_string());
        item_path.push(item_name);
        let complete_path = item_path.to_str().unwrap();
        item_path.with_extension("inc");
        let incomplete_path = item_path.to_str().unwrap().to_string();

        {
            let mut item_file = std::fs::OpenOptions::new().write(true)
                .mode(0o600)
                .create_new(true)
                .open(&incomplete_path)?;
            match serde_json::to_writer(&mut item_file, &item) {
                Ok(_) => {}
                Err(e) => return Err(to_ioerror(&e)),
            }
        }
        std::fs::rename(incomplete_path, complete_path)?;
        Ok(())
    }

    pub fn pop(&self) -> Result<Option<T>, std::io::Error> {
        let dirh = std::fs::read_dir(&self.path)?;
        let mut items: Vec<_> = dirh.filter(|item| match item {
                &Ok(ref dirent) => {
                    let p = dirent.path();
                    match p.extension() {
                        Some(_) => false,
                        None => p.is_file(),
                    }
                }
                &Err(_) => false,
            })
            .map(|item| item.unwrap())
            .collect();
        items.sort_by_key(|dir| dir.path());
        if items.is_empty() {
            return Ok(None);
        }
        let item_path = &items[0].path();
        {
            let item_file = std::fs::OpenOptions::new().read(true)
                .open(item_path)?;
            let _cleanup = Cleanup::File(item_path.to_str().unwrap().to_string());
            let maybe_item: Result<T, serde_json::error::Error> =
                serde_json::from_reader(item_file);
            match maybe_item {
                Ok(item) => return Ok(Some(item)),
                Err(e) => return Err(to_ioerror(&e)),
            }
        }
    }
}

pub struct QueueStream<T> {
    queue: Queue<T>,
    _watcher: notify::RecommendedWatcher,
    rx: mpsc::Receiver<notify::DebouncedEvent>,
}

impl<T: Serialize + Deserialize> QueueStream<T> {
    pub fn new(path: &str) -> Result<QueueStream<T>, std::io::Error> {
        let queue = Queue::<T>::new(path)?;
        let (tx, rx) = mpsc::channel();
        let watcher = match notify::Watcher::new(tx, std::time::Duration::from_secs(1)) {
            Ok(watcher) => watcher,
            Err(notify::Error::Io(e)) => return Err(e),
            Err(e) => return Err(to_ioerror(&e)),
        };
        Ok(QueueStream::<T> {
            queue: queue,
            _watcher: watcher,
            rx: rx,
        })
    }
}

impl<T: Serialize + Deserialize> futures::stream::Stream for QueueStream<T> {
    type Item = T;
    type Error = std::io::Error;

    fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
        match self.rx.try_recv() {
            Ok(_) => {
                match self.queue.pop() {
                    Ok(Some(t)) => Ok(futures::Async::Ready(Some(t))),
                    Ok(None) => Ok(futures::Async::NotReady),
                    Err(e) => Err(to_ioerror(&e)),
                }
            }
            Err(mpsc::TryRecvError::Empty) => Ok(futures::Async::NotReady),
            Err(mpsc::TryRecvError::Disconnected) => Ok(futures::Async::Ready(None)),
        }
    }
}

fn to_ioerror(e: &StdError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.description())
}

enum Cleanup {
    File(String),
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        match self {
            &mut Cleanup::File(ref path) => {
                match std::fs::remove_file(path) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("warning: failed to remove file {}: {}", path, e);
                    }
                }
            }
        }
    }
}
