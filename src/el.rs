use mio::event::Events;
use mio::net::TcpStream;
use mio::{Interest, Poll, Token};
use std::collections::HashMap;
use std::io::Error;
use std::io::Result;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

type ElHandler<'a> = Box<dyn FnMut(&mut EventLoop) + Send + 'a>;
type EvHandler<'a> = Box<dyn FnMut(&mut EventLoop, Token) + Send + 'a>;
type EvHandlerRes<'a> = Box<dyn FnMut(&mut EventLoop, Token) -> Option<Duration> + Send + 'a>;

struct FdRegister<'a> {
    id: Token,
    interest: Interest,
    reader: Option<EvHandler<'a>>,
    writer: Option<EvHandler<'a>>,
}

struct TimerRegister<'a> {
    id: Token,
    time: Instant,
    handler: EvHandler<'a>,
    finalizer: Option<ElHandler<'a>>,
}

const TIME_EVENTS: u8 = 0x1;
const FILE_EVENTS: u8 = 0x2;
const ALL_EVENTS: u8 = TIME_EVENTS | FILE_EVENTS;
const DONT_WAIT: u8 = 0x4;

pub struct EventLoop<'a> {
    el: Poll,
    stop: bool,
    maxfd: i32,
    last_time: i64,
    size: usize,
    fd_registry: HashMap<usize, FdRegister<'a>>,
    tm_registry: HashMap<usize, TimerRegister<'a>>,
    events: Events,
    before_sleep: Option<Box<dyn FnMut(&mut EventLoop) + 'a>>,
    token_seq: usize,
}

impl<'a> EventLoop<'a> {
    pub fn new(size: usize) -> EventLoop<'a> {
        EventLoop {
            el: Poll::new().unwrap(),
            stop: false,
            maxfd: 0,
            last_time: 0,
            size,
            fd_registry: HashMap::new(),
            tm_registry: HashMap::new(),
            events: Events::with_capacity(size),
            before_sleep: None,
            token_seq: 0,
        }
    }

    pub fn set_before_sleep(&mut self, before_sleep: impl FnMut(&mut EventLoop) + Send + 'a) {
        self.before_sleep = Some(before_sleep);
    }

    pub fn add_fd_event<S>(
        &self,
        stream: &mut S,
        interest: Interest,
        handler: impl FnMut(&mut EventLoop, Token) + 'a,
    ) -> Result<()>
    where
        S: event::Source + ?Sized + AsRawFd,
    {
        let fd = stream.as_raw_fd() as usize;

        let mut register = FdRegister {
            id: Token(fd),
            interest,
            reader: None,
            writer: None,
        };

        self.el.registry().register(stream, Token(fd), interest)?;

        if interest.is_readable() {
            register.reader = Some(handler);
        }
        if interest.is_writable() {
            register.writer = Some(handler);
        }

        self.fd_registry.insert(fd, register);

        Ok(())
    }

    pub fn delete_fd_event(&self, stream: &mut TcpStream, del_interest: Interest) -> Result<()> {
        let fd = stream.as_raw_fd() as usize;

        let mut register = self.fd_registry.get_mut(&fd);
        if register.is_none() {
            return Err(Error::new(std::io::ErrorKind::Other, "not found"));
        }

        let mut register1 = register.unwrap();

        if register1.interest.remove(del_interest).is_some() {
            self.el
                .registry()
                .reregister(stream, Token(fd), register1.interest)?;
        } else {
            self.el.registry().deregister(stream)?;
            self.fd_registry.remove(&fd);
        }

        Ok(())
    }

    pub fn add_timer_event(
        &self,
        d: Duration,
        handler: impl FnMut(&mut EventLoop, Token) -> Option<Duration> + Send + 'a,
        finalizer: Option<impl FnMut(&mut EventLoop) + 'a>,
    ) -> Result<()> {
        let id = self.token_seq;
        self.token_seq += 1;

        let register = TimerRegister {
            id: Token(id),
            time: Instant::now() + d,
            handler,
            finalizer,
        };

        self.tm_registry.insert(id, register);

        Ok(())
    }

    pub fn delete_timer_event(&self, id: usize) -> Result<()> {
        self.fd_registry.remove(&id);

        Ok(())
    }

    pub fn poll(&self, timeout: Option<Duration>) -> Result<()> {
        self.el.poll(&mut self.events, timeout)
    }

    pub fn run(&mut self) {
        self.stop = false;

        while !self.stop {
            if let Some(before_sleep) = self.before_sleep {
                before_sleep.as_ref()(self);
            }

            self.process_events(ALL_EVENTS);
        }
    }

    fn search_nearest_timer(&self) -> Option<Instant> {
        let mut nearest = None;

        for (_, register) in self.tm_registry.iter() {
            if nearest.is_none() || register.time < nearest.unwrap() {
                nearest = Some(register.time);
            }
        }

        nearest
    }

    fn process_time_events(&mut self) -> usize {
        let mut processed = 0;

        're_iterate: loop {
            for (id, register) in self.tm_registry.iter() {
                let now = Instant::now();
                if register.time <= now {
                    let res = (register.handler)(self, register.id);
                    processed += 1;

                    if let Some(dur) = res {
                        register.time = now + dur;
                    } else {
                        self.tm_registry.remove(id);
                    }

                    // the tm_registry may be changed, so we need to re-iterate
                    continue 're_iterate;
                }
            }

            break;
        }

        processed
    }

    fn process_events(&mut self, flags: u8) -> usize {
        let mut processed = 0;
        let mut timeout = None;

        if flags & TIME_EVENTS > 0 {
            if let Some(nearest) = self.search_nearest_timer() {
                timeout = Some(nearest - Instant::now());
            } else if flags & DONT_WAIT > 0 {
                timeout = Some(Duration::ZERO);
            } else {
                timeout = None;
            }
        }

        self.poll(timeout);

        for event in self.events.iter() {
            let token = event.token();
            // it's fired event so unwarp is safe
            let register = self.fd_registry.get(&token.0).unwrap();

            let mut rfired = false;

            if register.interest.is_readable() && event.is_readable() {
                rfired = true;
                register.reader.unwrap()(self, token);
            }

            if register.interest.is_writable() && event.is_writable() {
                // only fire write event if not fired read event
                if !rfired {
                    let writer = register.writer.unwrap();
                    // only fire if write handler is different from read handler
                    if register.reader.is_none()
                        || writer as *const _ != register.reader.unwrap() as *const _
                    {
                        writer(self, token);
                    }
                }
            }

            processed += 1;
        }

        if flags & TIME_EVENTS > 0 {
            processed += self.process_time_events();
        }

        processed
    }
}
