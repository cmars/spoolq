#[macro_use]
extern crate serde_derive;

extern crate futures;
extern crate serde;
extern crate serde_json;
extern crate textnonce;

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
        let complete_path = item_path.to_str().unwrap().to_string();
        let incomplete_path = item_path.with_extension("inc").to_str().unwrap().to_string();

        {
            let mut item_file = std::fs::OpenOptions::new().write(true)
                .mode(0o600)
                .create_new(true)
                .open(&incomplete_path)?;
            serde_json::to_writer(&mut item_file, &item).map_err(to_ioerror)?;
        }
        std::fs::rename(incomplete_path, complete_path)?;
        self.seq += 1;
        Ok(())
    }

    /// Pop an item off the queue.
    ///
    /// This method returns the first matching directory entry. Queue ordering cannot be guaranteed
    /// to be consistent across all operating systems and filesystems, as the serialized file will
    /// be chosen based on the filesystem's directory entry ordering.
    ///
    /// Popped items are not removed from the filesystem immediately; instead, they are marked for
    /// deletion. Use flush() to cause the items to be permanently removed from the underlying
    /// filesystem.
    pub fn pop(&self) -> Result<Option<T>, std::io::Error> {
        let dirh = std::fs::read_dir(&self.path)?;
        for maybe_dirent in dirh {
            let item_path = match maybe_dirent {
                Ok(dirent) => {
                    let p = dirent.path();
                    if let Some(_) = p.extension() {
                        continue;
                    }
                    p
                }
                Err(e) => return Err(e),
            };
            let stage_path = item_path.with_extension("pop");
            {
                let item_file = std::fs::OpenOptions::new().read(true)
                    .open(&item_path)?;
                let item = serde_json::from_reader(item_file).map_err(to_ioerror)?;
                std::fs::rename(item_path, stage_path)?;
                return Ok(Some(item));
            }
        }
        Ok(None)
    }

    /// Flush removes all pending item files marked for deletion.
    pub fn flush(&self) -> Result<(), std::io::Error> {
        let dirh = std::fs::read_dir(&self.path)?;
        for maybe_dirent in dirh {
            match maybe_dirent {
                Ok(dirent) => {
                    let p = dirent.path();
                    match p.extension() {
                        Some(e) => {
                            if e != "pop" {
                                continue;
                            }
                        }
                        None => continue,
                    }
                    std::fs::remove_file(p)?;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Recover unmarks all pending item files that were previously marked for deletion.
    ///
    /// Use recover to ensure that popped items are processed at least once, when it is uncertain
    /// whether they were processed due to a crash.
    ///
    /// This method is only recommended if items are being processed idempotently.
    pub fn recover(&self) -> Result<(), std::io::Error> {
        let dirh = std::fs::read_dir(&self.path)?;
        for maybe_dirent in dirh {
            match maybe_dirent {
                Ok(dirent) => {
                    let p = dirent.path();
                    if let Some(e) = p.extension() {
                        if e != "pop" {
                            continue;
                        }
                    }
                    let unmarked =
                        p.parent().unwrap().join(std::path::Path::new(p.file_stem().unwrap()));
                    std::fs::rename(p, unmarked)?;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// Process a Queue<T> as a stream of future values.
pub struct QueueStream<T> {
    queue: Queue<T>,
}

impl<T: Serialize + Deserialize> QueueStream<T> {
    /// Get a reference to the underlying queue.
    pub fn queue(&self) -> &Queue<T> {
        &self.queue
    }
    /// Get a mutable reference to the underlying queue.
    pub fn mut_queue(&mut self) -> &mut Queue<T> {
        &mut self.queue
    }
}

impl<T: Serialize + Deserialize> QueueStream<T> {
    /// Create a new QueueStream<T> with the given spool path.
    pub fn new(q: Queue<T>) -> QueueStream<T> {
        QueueStream::<T> { queue: q }
    }
}

impl<T: Serialize + Deserialize> futures::stream::Stream for QueueStream<T> {
    type Item = T;
    type Error = std::io::Error;

    /// Attempt to pop the next item off the stream.
    ///
    /// This method polls the underlying filesystem watcher for changes since the last poll.
    fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
        match self.queue.pop() {
            Ok(Some(t)) => Ok(futures::Async::Ready(Some(t))),
            Ok(None) => Ok(futures::Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

fn to_ioerror<E: StdError>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.description())
}

mod cleanup {
    use std;
    use std::ops::Drop;

    #[allow(dead_code)]
    pub enum Cleanup {
        Dir(String), // not really dead code, tests use this.
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            match self {
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

    use futures::{Future, Stream};

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
    fn test_recover_flush() {
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
        q.flush().unwrap();
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

        q.recover().unwrap();
        for _ in 0..100 {
            let item = q.pop().unwrap().unwrap();
            assert_eq!(item.b, item.i % 3 == 0);
            assert_eq!(item.s, format!("#{}", item.i));
            assert!(item.i > -1);
            assert!(item.i < 100);
        }
        assert!(match q.pop() {
            Ok(None) => true,
            _ => false,
        });
        q.flush().unwrap();
        q.recover().unwrap();
        assert!(match q.pop() {
            Ok(None) => true,
            _ => false,
        });
    }

    #[test]
    fn test_push_in_stream_out() {
        let (q, _cleanup) = new_queue();
        let mut qs = QueueStream::new(q);
        for i in 0..100 {
            assert!(qs.mut_queue()
                .push(Foo {
                    i: i,
                    b: i % 3 == 0,
                    s: format!("#{}", i),
                })
                .is_ok());
        }
        let f = qs.take(100).fold(0,
                                  |agg, item| -> Result<i32, std::io::Error> { Ok(agg + item.i) });
        let result = f.wait().unwrap();
        assert_eq!(result, 4950); // 0+1+2+..+99
    }
}
