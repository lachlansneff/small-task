use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    fmt::{self, Debug},
    mem,
    future::Future,
    pin::Pin,
};
use multitask::{Executor, Task};
use parking::Unparker;

macro_rules! pin_mut {
    ($($x:ident),*) => { $(
        // Move the value to ensure that it is owned
        let mut $x = $x;
        // Shadow the original binding so that it can't be directly accessed
        // ever again.
        #[allow(unused_mut)]
        let mut $x = unsafe {
            Pin::new_unchecked(&mut $x)
        };
    )* }
}

pub struct TaskPool {
    executor: Arc<Executor>,
    threads: Vec<(JoinHandle<()>, Arc<Unparker>)>,
    shutdown_flag: Arc<AtomicBool>,
}

impl TaskPool {
    pub fn create() -> Self {
        Self::create_num_threads(num_cpus::get())
    }

    pub fn create_num_threads(num_threads: usize) -> Self {
        let executor = Arc::new(Executor::new());
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let threads = (0..num_threads).map(|_| {
            let ex = Arc::clone(&executor);
            let flag = Arc::clone(&shutdown_flag);
            let (p, u) = parking::pair();
            let unparker = Arc::new(u);
            let u = Arc::clone(&unparker);
            // Run an executor thread.
            let handle = thread::spawn(move || {
                let ticker = ex.ticker(move || u.unpark());
                loop {
                    if flag.load(Ordering::Acquire) {
                        break;
                    }

                    if !ticker.tick() {
                        p.park();
                    }
                }
            });

            (handle, unparker)
        }).collect();

        Self {
            executor,
            threads,
            shutdown_flag,
        }
    }

    pub fn thread_num(&self) -> usize {
        self.threads.len()
    }

    pub fn scope<'scope, F>(&self, f: F)
    where
        F: FnOnce(&mut Scope<'scope>) + 'scope + Send,
    {
        // let ex = Arc::clone(&self.executor);
        let executor: &'scope Executor = unsafe { mem::transmute(&*self.executor) };

        let fut = async move {
            let mut scope = Scope {
                executor,
                spawned: Vec::new(),
            };

            f(&mut scope);

            // Find a way to get rid of this dependency.
            futures_util::future::join_all(scope.spawned).await;
        };

        pin_mut!(fut);

        // let fut: Pin<&mut (dyn Future<Output=()> + Send)> = fut;
        let fut: Pin<&'static mut (dyn Future<Output = ()> + Send + 'static)> = unsafe {
            mem::transmute(fut as Pin<&mut (dyn Future<Output=()> + Send)>)
        };

        let task = self.executor.spawn(fut);

        pollster::block_on(task);
    }

    pub fn shutdown(self) -> Result<(), ThreadPanicked> {
        let mut this = self;
        this.shutdown_internal()
    }

    fn shutdown_internal(&mut self) -> Result<(), ThreadPanicked> {
        self.shutdown_flag.store(true, Ordering::Release);

        for (_, unparker) in &self.threads {
            unparker.unpark();
        }
        for (join_handle, _) in self.threads.drain(..) {
            join_handle
                .join()
                .expect("task thread panicked while executing");
        }
        Ok(())
    }
}

impl Drop for TaskPool {
    fn drop(&mut self) {
        self.shutdown_internal().unwrap();
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct ThreadPanicked(());

impl Debug for ThreadPanicked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a task thread panicked during execution")
    }
}

pub struct Scope<'scope> {
    executor: &'scope Executor,
    spawned: Vec<Task<()>>,
}

impl<'scope> Scope<'scope> {
    pub fn spawn<Fut: Future<Output = ()> + 'scope + Send>(&mut self, f: Fut)
    {
        let fut: Pin<Box<dyn Future<Output = ()> + 'scope + Send>> = Box::pin(f);
        let fut: Pin<Box<dyn Future<Output = ()> + 'static + Send>> = unsafe { mem::transmute(fut) };

        let task = self.executor.spawn(fut);
        self.spawned.push(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn it_works() {
        let pool = TaskPool::create();

        let foo = Box::new(42);

        pool.scope(|scope| {
            for _ in 0..1000 {
                scope.spawn(async {
                    if *foo != 42 {
                        println!("not 42!?!?");
                    }
                });
            }
        });
    }
}
