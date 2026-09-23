// Child process values. A child process *is* an event emitter (like in Node);
// its mutable state lives in the emitter's user data so the safe accessors and
// the spawn machinery share one implementation.
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use Purs_Node_EventEmitter::{purust_emitter_emit, purust_emitter_listener_count, EventEmitter};

pub type UnsafeChildProcess = EventEmitter;

/// Stdio slots: the primitive strings, a shared stream or a raw descriptor.
pub enum StdIO {
    Pipe,
    Ignore,
    Overlapped,
    Ipc,
    Inherit,
    /// `defaultStdIO`: the slot keeps Node's default (a pipe).
    Default,
    Stream(Rc<Purs_Node_Stream::Stream>),
    Fd(i32),
}

/// Either a signal number or its name.
pub enum KillSignal {
    Number(i64),
    Name(String),
}

pub enum Shell {
    Enabled,
    Disabled,
    Command(String),
}

/// Text results honour the `encoding` option; buffers stay buffers.
pub enum StringOrBuffer {
    Str(String),
    Buffer(Rc<Purs_Node_Buffer_Immutable::ImmutableBuffer>),
}

pub fn stdio_box(stdio: StdIO) -> Rc<StdIO> {
    Rc::new(stdio)
}

pub fn string_or_buffer_box(value: StringOrBuffer) -> Rc<StringOrBuffer> {
    Rc::new(value)
}

pub fn string_or_buffer_bytes(value: &StringOrBuffer) -> Vec<u8> {
    match value {
        StringOrBuffer::Str(text) => purust_core::purust_string_to_utf8_lossy(text).into_bytes(),
        StringOrBuffer::Buffer(buffer) => buffer.bytes(),
    }
}

pub fn string_or_buffer_text(value: &StringOrBuffer) -> String {
    match value {
        StringOrBuffer::Str(text) => text.clone(),
        StringOrBuffer::Buffer(buffer) => {
            String::from_utf8_lossy(&buffer.bytes()).into_owned()
        }
    }
}

pub struct ChildState {
    pub spawn_file: String,
    pub spawn_args: Vec<String>,
    pub stdio: Vec<crate::UnknownType>,
    pub pid: i64,
    pub stdin: Option<Rc<EventEmitter>>,
    pub stdout: Option<Rc<EventEmitter>>,
    pub stderr: Option<Rc<EventEmitter>>,
    pub stdin_child: Option<std::process::ChildStdin>,
    pub exit_code: Option<i64>,
    pub signal_code: Option<String>,
    pub exited: bool,
    pub closed: bool,
    pub killed: bool,
    pub connected: bool,
    pub spawn_emitted: bool,
    pub spawn_delivered: bool,
    pub spawn_failed: bool,
    pub error: Option<crate::UnknownType>,
    pub error_delivered: bool,
    pub exit_delivered: bool,
    pub close_delivered: bool,
    /// Microtask queue captured at spawn time, when one was installed. Events
    /// discovered by the waiter thread are delivered through it so PureScript
    /// callbacks always run in the runtime's context.
    pub queue: Option<crate::UnknownType>,
}

impl ChildState {
    pub fn new(spawn_file: String, spawn_args: Vec<String>) -> ChildState {
        ChildState {
            spawn_file,
            spawn_args,
            stdio: Vec::new(),
            pid: 0,
            stdin: None,
            stdout: None,
            stderr: None,
            stdin_child: None,
            exit_code: None,
            signal_code: None,
            exited: false,
            closed: false,
            killed: false,
            connected: false,
            spawn_emitted: false,
            spawn_delivered: false,
            spawn_failed: false,
            error: None,
            error_delivered: false,
            exit_delivered: false,
            close_delivered: false,
            queue: None,
        }
    }
}

pub type ChildStateHandle = Arc<Mutex<ChildState>>;

pub fn purust_child_new(state: ChildState) -> Rc<EventEmitter> {
    let emitter = Rc::new(EventEmitter::new_native());
    emitter.set_user_data(crate::Value::Class(Rc::new(Arc::new(Mutex::new(state)))));
    let hooked = emitter.clone();
    Purs_Node_EventEmitter::purust_emitter_set_listen_hook(
        &emitter,
        Arc::new(move |_event| {
            purust_child_flush(&hooked);
        }),
    );
    emitter
}

pub fn purust_child_state(child: &Rc<EventEmitter>) -> ChildStateHandle {
    child
        .user_data()
        .expect("Node.ChildProcess: process without native state")
        .unwrap_class::<ChildStateHandle>()
        .clone()
}

/// Exit signals reach PureScript as `KillSignal` handles, not bare strings.
pub fn purust_kill_signal_value(number: i32) -> crate::UnknownType {
    let name = purust_signal_name(number);
    crate::Value::Class(Rc::new(Rc::new(KillSignal::Name(name))))
}

pub fn purust_kill_signal_value_named(name: &str) -> crate::UnknownType {
    crate::Value::Class(Rc::new(Rc::new(KillSignal::Name(name.to_owned()))))
}

fn class_nullable(value: Option<crate::UnknownType>) -> crate::UnknownType {
    let nullable = match value {
        Some(value) => Purs_Data_Nullable::Data_Nullable_notNull(value),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    };
    crate::Value::Class(Rc::new(nullable))
}

/// Delivers lifecycle events that already happened to whoever listens first.
/// Node emits these asynchronously; the port keeps them until a listener exists
/// so a `once` attached right after `spawn` cannot miss them.
pub fn purust_child_flush(child: &Rc<EventEmitter>) {
    // spawn
    let ready = {
        let state = purust_child_state(child);
        let state = state.lock().unwrap();
        state.spawn_emitted && !state.spawn_delivered
    };
    if ready && purust_emitter_listener_count(child, "spawn") > 0 {
        {
            let state = purust_child_state(child);
            state.lock().unwrap().spawn_delivered = true;
        }
        purust_emitter_emit(child, "spawn", Vec::new());
    }
    // error
    let pending_error = {
        let state = purust_child_state(child);
        let state = state.lock().unwrap();
        if !state.spawn_failed || state.error_delivered {
            None
        } else {
            state.error.clone()
        }
    };
    if let Some(error) = pending_error {
        if purust_emitter_listener_count(child, "error") > 0 {
            {
                let state = purust_child_state(child);
                state.lock().unwrap().error_delivered = true;
            }
            purust_emitter_emit(child, "error", vec![error]);
        }
    }
    // exit
    let (pending_exit, code, signal) = {
        let state = purust_child_state(child);
        let state = state.lock().unwrap();
        (
            state.exited && !state.exit_delivered,
            state.exit_code,
            state.signal_code.clone(),
        )
    };
    if pending_exit && purust_emitter_listener_count(child, "exit") > 0 {
        {
            let state = purust_child_state(child);
            state.lock().unwrap().exit_delivered = true;
        }
        let args = vec![
            class_nullable(code.map(crate::mk_int)),
            class_nullable(signal.map(|name| purust_kill_signal_value_named(&name))),
        ];
        purust_emitter_emit(child, "exit", args);
    }
    // close: like Node, `close` carries the same (code, signal) pair as `exit`.
    let (pending_close, close_code, close_signal) = {
        let state = purust_child_state(child);
        let state = state.lock().unwrap();
        (
            state.closed && !state.close_delivered,
            state.exit_code,
            state.signal_code.clone(),
        )
    };
    if pending_close && purust_emitter_listener_count(child, "close") > 0 {
        {
            let state = purust_child_state(child);
            state.lock().unwrap().close_delivered = true;
        }
        let args = vec![
            class_nullable(close_code.map(crate::mk_int)),
            class_nullable(close_signal.map(|name| purust_kill_signal_value_named(&name))),
        ];
        purust_emitter_emit(child, "close", args);
    }
}

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

/// Node accepts either a signal number or its name for a `KillSignal`.
pub fn purust_signal_number(value: &Rc<KillSignal>) -> i32 {
    match value.as_ref() {
        KillSignal::Number(number) => *number as i32,
        KillSignal::Name(name) => purust_signal_from_name(name).unwrap_or(0),
    }
}

pub fn purust_signal_name(number: i32) -> String {
    match number {
        libc::SIGHUP => "SIGHUP",
        libc::SIGINT => "SIGINT",
        libc::SIGQUIT => "SIGQUIT",
        libc::SIGILL => "SIGILL",
        libc::SIGTRAP => "SIGTRAP",
        libc::SIGABRT => "SIGABRT",
        libc::SIGBUS => "SIGBUS",
        libc::SIGFPE => "SIGFPE",
        libc::SIGKILL => "SIGKILL",
        libc::SIGUSR1 => "SIGUSR1",
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGUSR2 => "SIGUSR2",
        libc::SIGPIPE => "SIGPIPE",
        libc::SIGALRM => "SIGALRM",
        libc::SIGTERM => "SIGTERM",
        libc::SIGCHLD => "SIGCHLD",
        libc::SIGCONT => "SIGCONT",
        libc::SIGSTOP => "SIGSTOP",
        libc::SIGTSTP => "SIGTSTP",
        libc::SIGTTIN => "SIGTTIN",
        libc::SIGTTOU => "SIGTTOU",
        libc::SIGURG => "SIGURG",
        libc::SIGXCPU => "SIGXCPU",
        libc::SIGXFSZ => "SIGXFSZ",
        libc::SIGVTALRM => "SIGVTALRM",
        libc::SIGPROF => "SIGPROF",
        libc::SIGWINCH => "SIGWINCH",
        libc::SIGIO => "SIGIO",
        libc::SIGSYS => "SIGSYS",
        _ => "",
    }
    .to_owned()
}

pub fn purust_signal_from_name(name: &str) -> Option<i32> {
    let name = name.trim();
    if name.is_empty() {
        return Some(libc::SIGTERM);
    }
    if let Ok(number) = name.parse::<i32>() {
        return Some(number);
    }
    let number = match name.to_ascii_uppercase().as_str() {
        "SIGHUP" => libc::SIGHUP,
        "SIGINT" => libc::SIGINT,
        "SIGQUIT" => libc::SIGQUIT,
        "SIGILL" => libc::SIGILL,
        "SIGTRAP" => libc::SIGTRAP,
        "SIGABRT" | "SIGIOT" => libc::SIGABRT,
        "SIGBUS" => libc::SIGBUS,
        "SIGFPE" => libc::SIGFPE,
        "SIGKILL" => libc::SIGKILL,
        "SIGUSR1" => libc::SIGUSR1,
        "SIGSEGV" => libc::SIGSEGV,
        "SIGUSR2" => libc::SIGUSR2,
        "SIGPIPE" => libc::SIGPIPE,
        "SIGALRM" => libc::SIGALRM,
        "SIGTERM" => libc::SIGTERM,
        "SIGCHLD" => libc::SIGCHLD,
        "SIGCONT" => libc::SIGCONT,
        "SIGSTOP" => libc::SIGSTOP,
        "SIGTSTP" => libc::SIGTSTP,
        "SIGTTIN" => libc::SIGTTIN,
        "SIGTTOU" => libc::SIGTTOU,
        "SIGURG" => libc::SIGURG,
        "SIGXCPU" => libc::SIGXCPU,
        "SIGXFSZ" => libc::SIGXFSZ,
        "SIGVTALRM" => libc::SIGVTALRM,
        "SIGPROF" => libc::SIGPROF,
        "SIGWINCH" => libc::SIGWINCH,
        "SIGIO" => libc::SIGIO,
        "SIGSYS" => libc::SIGSYS,
        _ => return None,
    };
    Some(number)
}

// ---------------------------------------------------------------------------
// Stdio / shell / signal constructors
// ---------------------------------------------------------------------------

pub fn Node_ChildProcess_Types_pipe() -> Rc<StdIO> {
    stdio_box(StdIO::Pipe)
}

pub fn Node_ChildProcess_Types_ignore() -> Rc<StdIO> {
    stdio_box(StdIO::Ignore)
}

pub fn Node_ChildProcess_Types_overlapped() -> Rc<StdIO> {
    stdio_box(StdIO::Overlapped)
}

pub fn Node_ChildProcess_Types_ipc() -> Rc<StdIO> {
    stdio_box(StdIO::Ipc)
}

pub fn Node_ChildProcess_Types_inherit() -> Rc<StdIO> {
    stdio_box(StdIO::Inherit)
}

pub fn Node_ChildProcess_Types_defaultStdIO() -> Rc<StdIO> {
    stdio_box(StdIO::Default)
}

pub fn Node_ChildProcess_Types_shareStream(
    stream: Rc<Purs_Node_Stream::Stream>,
) -> Rc<StdIO> {
    stdio_box(StdIO::Stream(stream))
}

pub fn Node_ChildProcess_Types_fileDescriptor(fd: i64) -> Rc<StdIO> {
    stdio_box(StdIO::Fd(fd as i32))
}

pub fn Node_ChildProcess_Types_fileDescriptor_prime(
    fd: Rc<Purs_Node_FS::FileDescriptor>,
) -> Rc<StdIO> {
    use std::os::unix::io::AsRawFd;
    let raw = {
        let file = fd.file.lock().unwrap();
        file.as_raw_fd()
    };
    stdio_box(StdIO::Fd(raw))
}

pub fn Node_ChildProcess_Types_intSignal(value: i64) -> Rc<KillSignal> {
    Rc::new(KillSignal::Number(value))
}

pub fn Node_ChildProcess_Types_stringSignal(value: String) -> Rc<KillSignal> {
    Rc::new(KillSignal::Name(value))
}

pub fn Node_ChildProcess_Types_enableShell() -> Rc<Shell> {
    Rc::new(Shell::Enabled)
}

pub fn Node_ChildProcess_Types_customShell(value: String) -> Rc<Shell> {
    Rc::new(Shell::Command(value))
}

pub fn Node_ChildProcess_Types_showKillSignal(value: Rc<KillSignal>) -> String {
    match value.as_ref() {
        KillSignal::Number(number) => number.to_string(),
        KillSignal::Name(name) => name.clone(),
    }
}

pub fn Node_ChildProcess_Types_showShell(value: Rc<Shell>) -> String {
    match value.as_ref() {
        Shell::Enabled => "true".to_owned(),
        Shell::Disabled => "false".to_owned(),
        Shell::Command(command) => command.clone(),
    }
}

pub fn Node_ChildProcess_Types_fromKillSignalImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(
        |from_int, from_str, signal| {
            let signal = signal.unwrap_class::<Rc<KillSignal>>().clone();
            match signal.as_ref() {
                KillSignal::Number(number) => from_int.unwrap_func1()(crate::mk_int(*number)),
                KillSignal::Name(name) => {
                    from_str.unwrap_func1()(crate::Value::String(name.clone()))
                }
            }
        },
    )))
}
