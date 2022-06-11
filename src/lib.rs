use std::{
    future::Future,
    io,
    marker::PhantomData,
    mem::MaybeUninit,
    os::unix::io::RawFd,
    pin::Pin,
    ptr,
    ptr::NonNull,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    thread::JoinHandle,
};

use flume::{Receiver, Sender};
use thiserror::Error;
use uring_sys2::*;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("background thread is not alive")]
    NoBackgroundThread,
}

pub struct Uring {
    id_gen: AtomicU64,
    sender: Sender<Task>,
    handle: Option<JoinHandle<Result<(), Error>>>,
}

impl Drop for Uring {
    fn drop(&mut self) {
        let _ = self.sender.send(Task {
            id: self.id_gen.fetch_and(1, Ordering::Relaxed),
            kind: Box::new(Terminate {}),
        });
        if let Some(handle) = self.handle.take() {
            if let Err(err) = handle.join() {
                eprintln!("{:?}", err);
            }
        }
    }
}

impl Uring {
    pub fn new(entries: usize) -> io::Result<Self> {
        let (sender, receiver) = flume::unbounded();
        let mut worker = UringWorker::new(entries, receiver)?;

        let handle = Some(std::thread::spawn(move || unsafe {
            worker.run()?;
            Ok(())
        }));

        Ok(Self {
            id_gen: AtomicU64::new(0),
            sender,
            handle,
        })
    }

    pub fn read(&self, fd: RawFd, buf: Vec<u8>, offset: usize) -> Result<MyFuture<Vec<u8>>, Error> {
        let state = Arc::new(Mutex::new(FutureState {
            result: None,
            waker: None,
        }));
        self.sender
            .send(Task {
                id: self.id_gen.fetch_add(1, Ordering::Relaxed),
                kind: Box::new(ReadOperation {
                    buf,
                    offset: offset as u64,
                    fd,
                    state: Arc::clone(&state),
                }),
            })
            .map_err(|_| Error::NoBackgroundThread)?;
        Ok(MyFuture(Arc::clone(&state), PhantomData::default()))
    }

    pub fn write(
        &self,
        fd: RawFd,
        buf: Vec<u8>,
        offset: usize,
    ) -> Result<MyFuture<Vec<u8>>, Error> {
        let state = Arc::new(Mutex::new(FutureState {
            result: None,
            waker: None,
        }));
        self.sender
            .send(Task {
                id: self.id_gen.fetch_add(1, Ordering::Relaxed),
                kind: Box::new(WriteOperation {
                    buf,
                    offset: offset as u64,
                    fd,
                    state: Arc::clone(&state),
                }),
            })
            .map_err(|_| Error::NoBackgroundThread)?;
        Ok(MyFuture(Arc::clone(&state), PhantomData::default()))
    }
}

pub struct MyFuture<'a, T>(Arc<Mutex<FutureState<T>>>, PhantomData<&'a Uring>);

#[derive(Debug)]
struct FutureState<T> {
    result: Option<io::Result<T>>,
    waker: Option<Waker>,
}

impl<T> FutureState<T> {
    fn set_result_and_wake(&mut self, res: i32, result: T) {
        self.result = Some(if res < 0 {
            Err(io::Error::from_raw_os_error(-res))
        } else {
            Ok(result)
        });
        if let Some(ref waker) = self.waker {
            waker.wake_by_ref()
        }
    }
}

impl<'a, T> Future for MyFuture<'a, T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.0.lock().unwrap();

        if let Some(result) = state.result.take() {
            Poll::Ready(result)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

struct UringWorker {
    ring: io_uring,
    ongoing: usize,
    receiver: Receiver<Task>,
}

// FIXME
//  we are not going to pass `UringWorker` around between threads; just want to pass it from a thread
//  that invoked `Uring::new` to the background thread on initialization.
unsafe impl Send for UringWorker {}

impl UringWorker {
    pub(crate) fn new(entries: usize, receiver: Receiver<Task>) -> io::Result<Self> {
        let mut ring = MaybeUninit::uninit();
        let ring = unsafe {
            let ret = io_uring_queue_init(entries as u32, ring.as_mut_ptr(), 0);
            if ret < 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            ring.assume_init()
        };
        Ok(Self {
            ring,
            receiver,
            ongoing: 0,
        })
    }

    fn submit(&mut self) -> io::Result<()> {
        unsafe {
            let count = io_uring_submit(&mut self.ring);
            if count < 0 {
                Err(io::Error::from_raw_os_error(-count))
            } else {
                self.ongoing += count as usize;
                Ok(())
            }
        }
    }

    fn sqe(&mut self) -> io::Result<NonNull<io_uring_sqe>> {
        unsafe {
            loop {
                match NonNull::new(io_uring_get_sqe(&mut self.ring)) {
                    Some(sqe) => return Ok(sqe),
                    None => self.submit()?,
                }
            }
        }
    }

    unsafe fn prepare(&mut self, mut task: Task) -> io::Result<()> {
        let sqe = self.sqe()?;
        task.kind.prepare(sqe);
        let user_data = Box::new(UserData {
            header: UserDataHeader {
                id: task.id,
                task_kind: task.kind.task_kind(),
            },
            op: task.kind,
        });
        io_uring_sqe_set_data(sqe.as_ptr(), Box::into_raw(user_data) as *mut _);
        Ok(())
    }

    pub(crate) unsafe fn run(&mut self) -> Result<(), io::Error> {
        let mut keep_alive = true;

        // FIXME
        //  add backoff when there is no task to do.
        while keep_alive {
            // Process incoming I/O requests from user threads as much as possible.
            while let Ok(task) = self.receiver.try_recv() {
                if task.kind.task_kind() == TaskKind::Terminate {
                    keep_alive = false;
                } else {
                    self.prepare(task)?;
                }
            }

            // Batch-send I/O requests.
            self.submit()?;

            // Process completed I/O.
            while self.ongoing > 0 {
                let mut cqe = ptr::null_mut();
                let ret = io_uring_peek_cqe(&mut self.ring, &mut cqe);
                if ret == -libc::EAGAIN {
                    break;
                } else if ret < 0 {
                    eprintln!("{}", io::Error::from_raw_os_error(-ret));
                }
                self.ongoing -= 1;

                let header = (*cqe).user_data as *mut UserDataHeader;
                if header.is_null() {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                let res = (*cqe).res;
                io_uring_cqe_seen(&mut self.ring, cqe);

                match (*header).task_kind {
                    TaskKind::Read => {
                        let user_data = Box::from_raw(header as *mut UserData<Box<ReadOperation>>);
                        user_data.op.set_result(res);
                    }
                    TaskKind::Write => {
                        let user_data = Box::from_raw(header as *mut UserData<WriteOperation>);
                        user_data.op.set_result(res);
                    }
                    TaskKind::Terminate => (),
                }
            }
        }

        io_uring_queue_exit(&mut self.ring);

        Ok(())
    }
}

#[derive(Debug)]
#[repr(C)]
struct UserData<T> {
    header: UserDataHeader,
    op: T,
}

#[derive(Debug)]
#[repr(C)]
struct UserDataHeader {
    id: u64,
    task_kind: TaskKind,
}

#[repr(C)]
struct Task {
    id: u64,
    kind: Box<dyn UringOperation>,
}

trait UringOperation: Send + 'static {
    fn task_kind(&self) -> TaskKind;
    unsafe fn prepare(&mut self, sqe: NonNull<io_uring_sqe>);
    fn set_result(self, res: i32);
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
enum TaskKind {
    Read,
    Write,
    Terminate,
}

#[derive(Debug)]
#[repr(C)]
struct ReadOperation {
    buf: Vec<u8>,
    offset: u64,
    fd: RawFd,
    state: Arc<Mutex<FutureState<Vec<u8>>>>,
}

impl UringOperation for ReadOperation {
    fn task_kind(&self) -> TaskKind {
        TaskKind::Read
    }

    unsafe fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        io_uring_prep_read(
            sqe.as_ptr(),
            self.fd,
            self.buf.as_mut_ptr() as *mut _,
            self.buf.len() as u32,
            self.offset,
        );
    }

    fn set_result(self, res: i32) {
        let mut state = self.state.lock().unwrap();
        state.set_result_and_wake(res, self.buf);
    }
}

#[repr(C)]
struct WriteOperation {
    buf: Vec<u8>,
    offset: u64,
    fd: RawFd,
    state: Arc<Mutex<FutureState<Vec<u8>>>>,
}

impl UringOperation for WriteOperation {
    fn task_kind(&self) -> TaskKind {
        TaskKind::Write
    }

    unsafe fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        io_uring_prep_write(
            sqe.as_ptr(),
            self.fd,
            self.buf.as_ptr() as *const _,
            self.buf.len() as u32,
            self.offset,
        );
    }

    fn set_result(self, res: i32) {
        let mut state = self.state.lock().unwrap();
        state.set_result_and_wake(res, self.buf);
    }
}

#[derive(Debug, Default)]
struct Terminate {}

impl UringOperation for Terminate {
    fn task_kind(&self) -> TaskKind {
        TaskKind::Terminate
    }

    unsafe fn prepare(&mut self, _sqe: NonNull<io_uring_sqe>) {}

    fn set_result(self, _res: i32) {}
}
