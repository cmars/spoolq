#[macro_use]
extern crate serde_derive;

extern crate futures;
extern crate notify;
extern crate serde;
extern crate serde_json;
extern crate textnonce;

use std::sync::mpsc;
use std::error::Error as StdError;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

use serde::{Serialize, Deserialize};

/// A durable queue backed by the filesystem.
///
/// This queue stores items of type T as files in a spool directory. Files are serialized with
/// serde_json so T must derive serde's Serialize and Deserialize traits.
///
/// The queue's directory should only contain items of the same type. Any files in the spool
/// that fail to deserialize will be discarded.
pub struct Queue<T> {
    path: String,
    seq: u32,
    _placeholder: std::marker::PhantomData<T>,
}

impl<T: Serialize + Deserialize> Queue<T> {
    /// Create a new `Queue<T>` using the given directory path for storage.
    pub fn new(path: &str) -> Result<Queue<T>, std::io::Error> {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path)?;
        Ok(Queue::<T> {
            path: path.to_string(),
            seq: 0,
            _placeholder: std::marker::PhantomData,
        })
    }

    /// Push an item into the Queue.
    pub fn push(&mut self, item: T) -> Result<(), std::io::Error> {
        let mut item_path = std::path::PathBuf::from(&self.path);
        let item_name = format!("{:016x}-{}", self.seq, rand_string());
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
        self.seq += 1;
        Ok(())
    }

    /// Pop an item off the queue without regard to ordering.
    ///
    /// This method returns the first matching directory entry. Queue ordering cannot be
    /// guaranteed, as the serialized file is chosen based on the filesystem's ordering.
    ///
    /// Use this method when you value speed over order-correctness.
    pub fn pop(&self) -> Result<Option<T>, std::io::Error> {
        let dirh = std::fs::read_dir(&self.path)?;
        for maybe_dirent in dirh {
            let item_path = match maybe_dirent {
                Ok(dirent) => dirent.path(),
                Err(e) => return Err(e),
            };
            {
                let item_file = std::fs::OpenOptions::new().read(true)
                    .open(&item_path)?;
                let _cleanup = cleanup::Cleanup::File(item_path.to_str().unwrap().to_string());
                let maybe_item: Result<T, serde_json::error::Error> =
                    serde_json::from_reader(item_file);
                match maybe_item {
                    Ok(item) => return Ok(Some(item)),
                    Err(e) => return Err(to_ioerror(&e)),
                }
            }
        }
        Ok(None)
    }

    /// Pull the next queued item off the queue.
    ///
    /// This method returns the next item, based on an internal monotonically increasing sequence
    /// number starting at 0 when the Queue is instantiated.
    ///
    /// This method may be much slower, especially if the queue size grows large.
    ///
    /// If multiple threads or processes are pulling from the same queue directory, globally
    /// duplicates may occur, since reading the item's file and then removing it is not atomic.
    pub fn pull(&self) -> Result<Option<T>, std::io::Error> {
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
        if items.is_empty() {
            return Ok(None);
        }
        items.sort_by_key(|item| item.file_name());
        let item_path = &items[0].path();
        {
            let item_file = std::fs::OpenOptions::new().read(true)
                .open(item_path)?;
            let _cleanup = cleanup::Cleanup::File(item_path.to_str().unwrap().to_string());
            let maybe_item: Result<T, serde_json::error::Error> =
                serde_json::from_reader(item_file);
            match maybe_item {
                Ok(item) => return Ok(Some(item)),
                Err(e) => return Err(to_ioerror(&e)),
            }
        }
    }
}

/// Process a Queue<T> as a stream of future values.
pub struct QueueStream<T> {
    queue: Queue<T>,
    _watcher: notify::RecommendedWatcher,
    rx: mpsc::Receiver<notify::DebouncedEvent>,
}

impl<T: Serialize + Deserialize> QueueStream<T> {
    /// Create a new QueueStream<T> with the given spool path.
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

    /// Attempt to pull the next item off the stream.
    ///
    /// This method polls the underlying filesystem watcher for changes since the last poll.
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

mod cleanup {
    use std;
    use std::ops::Drop;

    pub enum Cleanup {
        File(String),
        #[allow(dead_code)]
        Dir(String), // not really dead code, tests use this.
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
                &mut Cleanup::Dir(ref path) => {
                    match std::fs::remove_dir_all(path) {
                        Ok(_) => {}
                        Err(e) => {
                            println!("warning: failed to remove file {}: {}", path, e);
                        }
                    }
                }
            }
        }
    }
}

fn rand_string() -> String {
    textnonce::TextNonce::sized_urlsafe(32).unwrap().into_string()
}

#[cfg(test)]
mod tests {
    use std;
    use std::collections::HashSet;
    use super::*;

    #[derive(Serialize, Deserialize)]
    #[derive(PartialEq, Debug)]
    struct Foo {
        i: i32,
        b: bool,
        s: String,
    }

    fn new_queue() -> (Queue<Foo>, cleanup::Cleanup) {
        let mut spool_path_buf = std::env::temp_dir();
        spool_path_buf.push(rand_string());
        let spool_dir = spool_path_buf.to_str().unwrap();
        let _cleanup = cleanup::Cleanup::Dir(spool_dir.to_string());
        let q = Queue::<Foo>::new(spool_dir).unwrap();
        (q, _cleanup)
    }

    #[test]
    fn test_push_pop() {
        let (mut q, _cleanup) = new_queue();
        assert!(q.push(Foo {
                i: 999,
                b: true,
                s: "foo".to_string(),
            })
            .is_ok());
        let result = q.pop().unwrap().unwrap();
        assert_eq!(result,
                   Foo {
                       i: 999,
                       b: true,
                       s: "foo".to_string(),
                   });
        assert!(match q.pop() {
            Ok(None) => true,
            _ => false,
        })
    }

    #[test]
    fn test_push_pull() {
        let (mut q, _cleanup) = new_queue();
        assert!(q.push(Foo {
                i: 888,
                b: false,
                s: "bar".to_string(),
            })
            .is_ok());
        let result = q.pull().unwrap().unwrap();
        assert_eq!(result,
                   Foo {
                       i: 888,
                       b: false,
                       s: "bar".to_string(),
                   });
        assert!(match q.pull() {
            Ok(None) => true,
            _ => false,
        })
    }

    #[test]
    fn test_push_pop_many() {
        let (mut q, _cleanup) = new_queue();
        let mut indexes = HashSet::<i32>::new();
        for i in 0..100 {
            assert!(q.push(Foo {
                    i: i,
                    b: i % 3 == 0,
                    s: format!("#{}", i),
                })
                .is_ok());
            indexes.insert(i);
        }
        for _ in 0..100 {
            let item = q.pop().unwrap().unwrap();
            assert_eq!(item.b, item.i % 3 == 0);
            assert_eq!(item.s, format!("#{}", item.i));
            assert!(item.i > -1);
            assert!(item.i < 100);
            indexes.remove(&item.i);
        }
        assert!(match q.pop() {
            Ok(None) => true,
            _ => false,
        });
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_push_pull_many() {
        let (mut q, _cleanup) = new_queue();
        for i in 0..100 {
            assert!(q.push(Foo {
                    i: i,
                    b: i % 3 == 0,
                    s: format!("#{}", i),
                })
                .is_ok());
        }
        for i in 0..100 {
            let item = q.pull().unwrap().unwrap();
            assert_eq!(item.i, i);
            assert_eq!(item.b, i % 3 == 0);
            assert_eq!(item.s, format!("#{}", i));
        }
        assert!(match q.pull() {
            Ok(None) => true,
            _ => false,
        })
    }
}
