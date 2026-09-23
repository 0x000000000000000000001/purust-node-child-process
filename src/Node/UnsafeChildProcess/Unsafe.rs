// Low level child process support. Spawning uses std::process; pipes are wired
// to the native node-streams implementation, and a waiter thread reports the
// exit status back into the emitter state.
use std::io::Read;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::process::{ChildStdin, Command, Stdio};
use std::rc::Rc;

use Purs_Node_ChildProcess_Types::{
    purust_child_flush, purust_child_new, purust_child_state, purust_signal_name,
    purust_signal_number, string_or_buffer_box, string_or_buffer_bytes, string_or_buffer_text,
    ChildState, KillSignal, Shell, StdIO, StringOrBuffer,
};
use Purs_Node_Errors_SystemError::{purust_system_error_from_io, purust_system_error_value};
use Purs_Node_EventEmitter::EventEmitter;

/// The microtask queue of the calling context, when one is installed.
fn current_queue() -> Option<Rc<purust_core::microtasks::Queue>> {
    std::panic::catch_unwind(purust_core::microtasks::current).ok()
}

fn current_queue_value() -> Option<crate::UnknownType> {
    current_queue().map(|queue| crate::Value::Class(Rc::new(queue)))
}

fn deliver(queue: &Option<crate::UnknownType>, job: impl FnOnce() + Send + Sync + 'static) {
    match queue {
        Some(queue) => {
            let queue = queue
                .unwrap_class::<Rc<purust_core::microtasks::Queue>>()
                .clone();
            queue.enqueue(job);
        }
        None => job(),
    }
}

fn options_field(options: &crate::UnknownType, key: &str) -> Option<crate::UnknownType> {
    // The no-options variants pass `Unit` (there is no option record at all).
    if matches!(options.resolve(), crate::Value::Unit) {
        return None;
    }
    let value = options.__purust_foreign_object().get(key)?;
    match value.resolve() {
        // Absent options are `Nullable null`; `Unit` shows up for `undefined`.
        crate::Value::Unit => None,
        crate::Value::Class(payload) => match payload.downcast_ref::<Rc<Purs_Data_Nullable::Nullable>>() {
            Some(nullable) => nullable.value(),
            None => Some(value),
        },
        _ => Some(value),
    }
}

fn option_string(options: &crate::UnknownType, key: &str) -> Option<String> {
    options_field(options, key).map(|value| value.unwrap_string())
}

fn option_int(options: &crate::UnknownType, key: &str) -> Option<i64> {
    options_field(options, key).map(|value| match value.resolve() {
        crate::Value::Int(number) => *number,
        crate::Value::Number(number) => *number as i64,
        _ => 0,
    })
}

fn option_bool(options: &crate::UnknownType, key: &str) -> Option<bool> {
    options_field(options, key).map(|value| value.unwrap_bool())
}

fn option_bytes(options: &crate::UnknownType, key: &str) -> Option<Vec<u8>> {
    options_field(options, key).map(|value| bytes_of(&value))
}

fn bytes_of(value: &crate::UnknownType) -> Vec<u8> {
    match value.resolve() {
        crate::Value::String(text) => purust_core::purust_string_to_utf8_lossy(text).into_bytes(),
        _ => value
            .unwrap_class::<Rc<Purs_Node_Buffer_Immutable::ImmutableBuffer>>()
            .bytes(),
    }
}

fn string_or_buffer_handle(value: StringOrBuffer) -> crate::UnknownType {
    crate::Value::Class(Rc::new(string_or_buffer_box(value)))
}

fn result_value(bytes: Vec<u8>, encoding: Option<&String>) -> crate::UnknownType {
    match encoding.map(|encoding| encoding.as_str()) {
        None | Some("buffer") => string_or_buffer_handle(StringOrBuffer::Buffer(
            Purs_Node_Buffer_Immutable::purust_buffer_from_bytes(bytes),
        )),
        Some(encoding) => {
            let name = Purs_Node_Encoding::purust_encoding_from_name(encoding);
            string_or_buffer_handle(StringOrBuffer::Str(
                Purs_Node_Encoding::purust_encoding_decode(name, &bytes),
            ))
        }
    }
}

fn class_nullable(value: Option<crate::UnknownType>) -> crate::UnknownType {
    let nullable = match value {
        Some(value) => Purs_Data_Nullable::Data_Nullable_notNull(value),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    };
    crate::Value::Class(Rc::new(nullable))
}

fn unbox_child(value: &crate::UnknownType) -> Rc<EventEmitter> {
    value.unwrap_class::<Rc<EventEmitter>>().clone()
}

// ---------------------------------------------------------------------------
// Stdio planning
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum StdioPlan {
    Pipe,
    Ignore,
    Inherit,
    Fd(i32),
    Stream,
}

fn plan_for(value: &crate::UnknownType) -> StdioPlan {
    match value.unwrap_class::<Rc<StdIO>>().as_ref() {
        StdIO::Ignore | StdIO::Overlapped | StdIO::Ipc => StdioPlan::Ignore,
        StdIO::Inherit => StdioPlan::Inherit,
        StdIO::Fd(fd) => StdioPlan::Fd(*fd),
        StdIO::Stream(_) => StdioPlan::Stream,
        StdIO::Pipe | StdIO::Default => StdioPlan::Pipe,
    }
}

fn stdio_plans(options: &crate::UnknownType) -> [StdioPlan; 3] {
    let mut plans = [StdioPlan::Pipe; 3];
    if let Some(stdio) = options_field(options, "stdio") {
        if let crate::Value::Array(entries) = stdio.resolve() {
            for (index, entry) in entries.iter().take(3).enumerate() {
                plans[index] = plan_for(entry);
            }
        }
    }
    plans
}

fn stdio_of(plan: StdioPlan) -> Stdio {
    match plan {
        StdioPlan::Pipe | StdioPlan::Stream => Stdio::piped(),
        StdioPlan::Ignore => Stdio::null(),
        StdioPlan::Inherit => Stdio::inherit(),
        StdioPlan::Fd(fd) => {
            // Duplicate the descriptor so dropping the Stdio never closes the
            // descriptor the caller owns.
            let duplicated = unsafe { libc::dup(fd) };
            if duplicated < 0 {
                Stdio::null()
            } else {
                unsafe { Stdio::from_raw_fd(duplicated) }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Spawning
// ---------------------------------------------------------------------------

fn shell_command(options: &crate::UnknownType) -> Option<String> {
    options_field(options, "shell").map(|value| {
        let shell = value.unwrap_class::<Rc<Shell>>().clone();
        match shell.as_ref() {
            Shell::Enabled => Some("/bin/sh".to_owned()),
            Shell::Disabled => None,
            Shell::Command(command) => Some(command.clone()),
        }
    })?
}

fn apply_common(command: &mut Command, options: &crate::UnknownType, shell: bool) {
    if let Some(cwd) = option_string(options, "cwd") {
        command.current_dir(cwd);
    }
    match options_field(options, "env") {
        Some(environment) => {
            command.env_clear();
            for (key, value) in environment.__purust_foreign_object().entries() {
                if let crate::Value::String(value) = value.resolve() {
                    command.env(key, value);
                }
            }
        }
        None => {}
    }
    if !shell {
        if let Some(argv0) = option_string(options, "argv0") {
            command.arg0(argv0);
        }
    }
    if let Some(uid) = option_int(options, "uid") {
        command.uid(uid as u32);
    }
    if let Some(gid) = option_int(options, "gid") {
        command.gid(gid as u32);
    }
    if option_bool(options, "detached").unwrap_or(false) {
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }
}

fn build_command(
    file: &str,
    args: &[String],
    options: &crate::UnknownType,
) -> (Command, bool) {
    let shell = shell_command(options);
    let mut command = match &shell {
        Some(shell_path) => {
            let mut command = Command::new(shell_path);
            let mut line = file.to_owned();
            for argument in args {
                line.push(' ');
                line.push_str(argument);
            }
            command.arg("-c").arg(line);
            command
        }
        None => {
            let mut command = Command::new(file);
            command.args(args);
            command
        }
    };
    apply_common(&mut command, options, shell.is_some());
    (command, shell.is_some())
}

fn spawn_error(options: &crate::UnknownType, file: &str, error: &std::io::Error) -> crate::UnknownType {
    let syscall = if shell_command(options).is_some() {
        "spawn /bin/sh"
    } else {
        "spawn"
    };
    purust_system_error_value(purust_system_error_from_io(error, syscall, Some(file)))
}

fn finish_child(child: &Rc<EventEmitter>, status: std::io::Result<std::process::ExitStatus>) {
    use std::os::unix::process::ExitStatusExt;
    {
        let state = purust_child_state(child);
        let mut state = state.lock().unwrap();
        if let Ok(status) = status {
            state.exit_code = status.code().map(|code| code as i64);
            state.signal_code = status
                .signal()
                .map(purust_signal_name)
                .filter(|name| !name.is_empty());
        }
        state.exited = true;
        state.closed = true;
    }
    purust_child_flush(child);
}

fn start_drain(
    stream: Rc<EventEmitter>,
    mut reader: Box<dyn Read + Send>,
    queue: Option<crate::UnknownType>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buffer = [0u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    let bytes = buffer[..count].to_vec();
                    let pushed = stream.clone();
                    deliver(&queue, move || {
                        Purs_Node_Stream::purust_stream_push(&pushed, bytes);
                    });
                }
            }
        }
        let ended = stream.clone();
        deliver(&queue, move || {
            Purs_Node_Stream::purust_stream_end(&ended);
        });
    })
}

struct Spawned {
    child: Rc<EventEmitter>,
    stdin: Option<Rc<EventEmitter>>,
    stdout: Option<Rc<EventEmitter>>,
    stderr: Option<Rc<EventEmitter>>,
    drains: Vec<std::thread::JoinHandle<()>>,
}

fn spawn_child(file: &str, args: &[String], options: &crate::UnknownType) -> Spawned {
    let queue = current_queue_value();
    let plans = stdio_plans(options);
    let (mut command, _shell) = build_command(file, args, options);
    command.stdin(stdio_of(plans[0]));
    command.stdout(stdio_of(plans[1]));
    command.stderr(stdio_of(plans[2]));
    match command.spawn() {
        Ok(mut process) => {
            let mut state = ChildState::new(file.to_owned(), args.to_vec());
            state.pid = process.id() as i64;
            state.spawn_emitted = true;
            state.queue = current_queue_value();
            state.stdio = plans
                .iter()
                .map(|plan| {
                    let stdio = match plan {
                        StdioPlan::Pipe | StdioPlan::Stream => StdIO::Pipe,
                        StdioPlan::Ignore => StdIO::Ignore,
                        StdioPlan::Inherit => StdIO::Inherit,
                        StdioPlan::Fd(fd) => StdIO::Fd(*fd),
                    };
                    crate::Value::Class(Rc::new(Purs_Node_ChildProcess_Types::stdio_box(stdio)))
                })
                .collect();
            let child = purust_child_new(state);

            let mut stdin_stream = None;
            if let Some(stdin) = process.stdin.take() {
                let stream = Purs_Node_Stream::purust_writable_new();
                Purs_Node_Stream::purust_stream_set_write_fd(&stream, stdin.as_raw_fd());
                {
                    let state = purust_child_state(&child);
                    let mut state = state.lock().unwrap();
                    state.stdin = Some(stream.clone());
                    state.stdin_child = Some(stdin);
                }
                stdin_stream = Some(stream);
            }
            let mut drains = Vec::new();
            let mut stdout_stream = None;
            if let Some(stdout) = process.stdout.take() {
                let stream = Purs_Node_Stream::purust_readable_from_bytes(Vec::new());
                drains.push(start_drain(stream.clone(), Box::new(stdout), queue.clone()));
                {
                    let state = purust_child_state(&child);
                    state.lock().unwrap().stdout = Some(stream.clone());
                }
                stdout_stream = Some(stream);
            }
            let mut stderr_stream = None;
            if let Some(stderr) = process.stderr.take() {
                let stream = Purs_Node_Stream::purust_readable_from_bytes(Vec::new());
                drains.push(start_drain(stream.clone(), Box::new(stderr), queue.clone()));
                {
                    let state = purust_child_state(&child);
                    state.lock().unwrap().stderr = Some(stream.clone());
                }
                stderr_stream = Some(stream);
            }

            let waiter_child = child.clone();
            let waiter_queue = current_queue_value();
            let waiter = std::thread::spawn(move || {
                let status = process.wait();
                deliver(&waiter_queue, move || {
                    finish_child(&waiter_child, status);
                });
            });

            // Timeout handling mirrors Node: kill with `killSignal` after the
            // configured duration.
            if let Some(timeout) = option_int(options, "timeout") {
                if timeout > 0 {
                    let timed_child = child.clone();
                    let signal = options_field(options, "killSignal")
                        .map(|value| {
                            let signal = value.unwrap_class::<Rc<KillSignal>>().clone();
                            purust_signal_number(&signal)
                        })
                        .unwrap_or(libc::SIGTERM);
                    std::thread::spawn(move || {
                        std::thread::sleep(std::time::Duration::from_millis(timeout as u64));
                        let state = purust_child_state(&timed_child);
                        let (pid, exited) = {
                            let state = state.lock().unwrap();
                            (state.pid, state.exited)
                        };
                        if pid > 0 && !exited {
                            unsafe {
                                libc::kill(pid as libc::pid_t, signal);
                            }
                        }
                    });
                }
            }

            let _ = waiter;
            purust_child_flush(&child);
            Spawned {
                child,
                stdin: stdin_stream,
                stdout: stdout_stream,
                stderr: stderr_stream,
                drains,
            }
        }
        Err(error) => {
            let mut state = ChildState::new(file.to_owned(), args.to_vec());
            state.spawn_failed = true;
            state.closed = true;
            state.error = Some(spawn_error(options, file, &error));
            let child = purust_child_new(state);
            purust_child_flush(&child);
            Spawned {
                child,
                stdin: None,
                stdout: None,
                stderr: None,
                drains: Vec::new(),
            }
        }
    }
}

fn string_or_buffer_bytes_of(value: &crate::UnknownType) -> Vec<u8> {
    let handle = value.unwrap_class::<Rc<StringOrBuffer>>().clone();
    string_or_buffer_bytes(&handle)
}

fn stream_bytes(stream: &Option<Rc<EventEmitter>>) -> Vec<u8> {
    match stream {
        Some(stream) => Purs_Node_Stream::purust_stream_bytes(stream),
        None => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Synchronous variants
// ---------------------------------------------------------------------------

fn run_sync(
    file: &str,
    args: &[String],
    options: &crate::UnknownType,
) -> std::io::Result<(std::process::Output, i64)> {
    let plans = stdio_plans(options);
    let (mut command, _) = build_command(file, args, options);
    command.stdin(if plans[0] == StdioPlan::Pipe {
        Stdio::piped()
    } else {
        stdio_of(plans[0])
    });
    command.stdout(if plans[1] == StdioPlan::Pipe {
        Stdio::piped()
    } else {
        stdio_of(plans[1])
    });
    command.stderr(if plans[2] == StdioPlan::Pipe {
        Stdio::piped()
    } else {
        stdio_of(plans[2])
    });
    let mut process = command.spawn()?;
    let pid = process.id() as i64;
    let input = option_bytes(options, "input");
    if let Some(bytes) = input {
        if let Some(mut stdin) = process.stdin.take() {
            use std::io::Write;
            let _ = stdin.write_all(&bytes);
        }
    }
    drop(process.stdin.take());
    process.wait_with_output().map(|output| (output, pid))
}

fn raise_io(error: &std::io::Error, file: &str) -> ! {
    Purs_Node_Errors_SystemError::purust_system_error_raise(purust_system_error_from_io(
        error,
        "spawn",
        Some(file),
    ))
}


/// Nullable handles cross as class-boxed native values.
fn nullable_handle(value: Option<crate::UnknownType>) -> crate::UnknownType {
    let nullable = match value {
        Some(value) => Purs_Data_Nullable::Data_Nullable_notNull(value),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    };
    crate::Value::Class(Rc::new(nullable))
}

pub fn Node_UnsafeChildProcess_Unsafe_unsafeStdin(
    child: Rc<crate::UnsafeChildProcess>,
) -> Rc<Purs_Data_Nullable::Nullable> {
    let stdin = purust_child_state(&child).lock().unwrap().stdin.clone();
    match stdin {
        Some(stream) => Purs_Data_Nullable::Data_Nullable_notNull(Purs_Node_Stream::purust_stream_box(
            stream,
        )),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    }
}

pub fn Node_UnsafeChildProcess_Unsafe_unsafeStdout(
    child: Rc<crate::UnsafeChildProcess>,
) -> Rc<Purs_Data_Nullable::Nullable> {
    let stdout = purust_child_state(&child).lock().unwrap().stdout.clone();
    match stdout {
        Some(stream) => Purs_Data_Nullable::Data_Nullable_notNull(Purs_Node_Stream::purust_stream_box(
            stream,
        )),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    }
}

pub fn Node_UnsafeChildProcess_Unsafe_unsafeStderr(
    child: Rc<crate::UnsafeChildProcess>,
) -> Rc<Purs_Data_Nullable::Nullable> {
    let stderr = purust_child_state(&child).lock().unwrap().stderr.clone();
    match stderr {
        Some(stream) => Purs_Data_Nullable::Data_Nullable_notNull(Purs_Node_Stream::purust_stream_box(
            stream,
        )),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    }
}

fn exec_sync_value(command: &str, options: &crate::UnknownType) -> crate::UnknownType {
    match run_sync("/bin/sh", &["-c".to_owned(), command.to_owned()], options) {
        Ok((output, _pid)) => {
            let encoding = option_string(options, "encoding");
            result_value(output.stdout, encoding.as_ref())
        }
        Err(error) => raise_io(&error, command),
    }
}

pub fn Node_UnsafeChildProcess_Unsafe_execSyncImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|command| {
        let command = command.unwrap_string();
        exec_sync_value(&command, &crate::Value::Unit)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execSyncOptsImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|command, options| {
        exec_sync_value(&command.unwrap_string(), &options)
    })))
}

fn exec_file_sync_value(file: &str, args: &[String], options: &crate::UnknownType) -> crate::UnknownType {
    match run_sync(file, args, options) {
        Ok((output, _pid)) => {
            let encoding = option_string(options, "encoding");
            result_value(output.stdout, encoding.as_ref())
        }
        Err(error) => raise_io(&error, file),
    }
}

pub fn Node_UnsafeChildProcess_Unsafe_execFileSyncImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|file, args| {
        let file = file.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        exec_file_sync_value(&file, &args, &crate::Value::Unit)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execFileSyncOptsImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|file, args, options| {
        let file = file.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        exec_file_sync_value(&file, &args, &options)
    })))
}

fn spawn_sync_value(file: &str, args: &[String], options: &crate::UnknownType) -> crate::UnknownType {
    // Force pipes so the result carries stdout/stderr like Node does.
    let mut options = options.__purust_foreign_object().entries();
    let with_pipe = |fields: &mut Vec<(String, crate::UnknownType)>| {
        if !fields.iter().any(|(key, _)| key == "stdio") {
            fields.push((
                "stdio".to_owned(),
                crate::mk_array(vec![
                    crate::Value::String("pipe".to_owned()),
                    crate::Value::String("pipe".to_owned()),
                    crate::Value::String("pipe".to_owned()),
                ]),
            ));
        }
    };
    with_pipe(&mut options);
    let mut fields = purust_core::RecordFields::new();
    for (key, value) in options {
        fields.insert(key, value);
    }
    let record = crate::Value::DynamicRecord(perceus_ptr::PerceusPtr::new(fields));
    let encoding = option_string(&record, "encoding");
    match run_sync(file, args, &record) {
        Ok((output, pid)) => {
            let stdout = result_value(output.stdout, encoding.as_ref());
            let stderr = result_value(output.stderr, encoding.as_ref());
            let status = output.status.code().map(|code| code as i64);
            let signal = {
                use std::os::unix::process::ExitStatusExt;
                output.status.signal().map(purust_signal_name)
            };
            let mut fields = purust_core::RecordFields::new();
            fields.insert("pid".to_owned(), crate::mk_int(pid));
            fields.insert(
                "output".to_owned(),
                crate::mk_array(vec![crate::mk_unit(()), stdout.clone(), stderr.clone()]),
            );
            fields.insert("stdout".to_owned(), stdout);
            fields.insert("stderr".to_owned(), stderr);
            fields.insert(
                "status".to_owned(),
                class_nullable(status.map(crate::mk_int)),
            );
            fields.insert(
                "signal".to_owned(),
                class_nullable(signal.map(crate::Value::String)),
            );
            fields.insert("error".to_owned(), class_nullable(None));
            crate::Value::DynamicRecord(perceus_ptr::PerceusPtr::new(fields))
        }
        Err(error) => {
            let system_error = purust_system_error_value(purust_system_error_from_io(
                &error,
                "spawnSync",
                Some(file),
            ));
            let mut fields = purust_core::RecordFields::new();
            fields.insert("pid".to_owned(), class_nullable(None));
            fields.insert("output".to_owned(), crate::mk_array(Vec::new()));
            fields.insert("stdout".to_owned(), class_nullable(None));
            fields.insert("stderr".to_owned(), class_nullable(None));
            fields.insert("status".to_owned(), class_nullable(None));
            fields.insert("signal".to_owned(), class_nullable(None));
            fields.insert("error".to_owned(), class_nullable(Some(system_error)));
            crate::Value::DynamicRecord(perceus_ptr::PerceusPtr::new(fields))
        }
    }
}

pub fn Node_UnsafeChildProcess_Unsafe_spawnSyncImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|command, args| {
        let command = command.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        spawn_sync_value(&command, &args, &crate::Value::Unit)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_spawnSyncOptsImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|command, args, options| {
        let command = command.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        spawn_sync_value(&command, &args, &options)
    })))
}

// ---------------------------------------------------------------------------
// Asynchronous variants
// ---------------------------------------------------------------------------

fn start_exec_thread(spawned: Spawned, callback: crate::UnknownType, encoding: Option<String>, nullable_error: bool) {
    let queue = {
        let state = purust_child_state(&spawned.child);
        let queue = state.lock().unwrap().queue.clone();
        queue
    };
    std::thread::spawn(move || {
        for drain in spawned.drains {
            let _ = drain.join();
        }
        // The waiter thread reaps the process; the streams are finished here.
        let stdout = stream_bytes(&spawned.stdout);
        let stderr = stream_bytes(&spawned.stderr);
        let stdout = result_value(stdout, encoding.as_ref());
        let stderr = result_value(stderr, encoding.as_ref());
        let error = {
            let state = purust_child_state(&spawned.child);
            let state = state.lock().unwrap();
            if state.spawn_failed {
                state.error.clone()
            } else {
                None
            }
        };
        let error_value = if nullable_error {
            class_nullable(error)
        } else {
            class_nullable(error)
        };
        deliver(&queue, move || {
            callback.unwrap_func3()(error_value, stdout, stderr);
        });
    });
}

fn exec_async(file: &str, args: &[String], options: &crate::UnknownType, callback: crate::UnknownType) -> Rc<EventEmitter> {
    let spawned = spawn_child(file, args, options);
    let encoding = option_string(options, "encoding");
    start_exec_thread(
        Spawned {
            child: spawned.child.clone(),
            stdin: spawned.stdin,
            stdout: spawned.stdout.clone(),
            stderr: spawned.stderr.clone(),
            drains: spawned.drains,
        },
        callback,
        encoding,
        true,
    );
    spawned.child
}

pub fn Node_UnsafeChildProcess_Unsafe_execImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|command| {
        let command = command.unwrap_string();
        let spawned = spawn_child("/bin/sh", &["-c".to_owned(), command], &crate::Value::Unit);
        spawn_box(spawned.child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execOptsImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|command, options| {
        let command = command.unwrap_string();
        let spawned = spawn_child("/bin/sh", &["-c".to_owned(), command], &options);
        spawn_box(spawned.child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execCbImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|command, callback| {
        let command = command.unwrap_string();
        let child = exec_async("/bin/sh", &["-c".to_owned(), command], &crate::Value::Unit, callback);
        spawn_box(child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execOptsCbImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|command, options, callback| {
        let command = command.unwrap_string();
        let child = exec_async("/bin/sh", &["-c".to_owned(), command], &options, callback);
        spawn_box(child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execFileImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|file, args| {
        let file = file.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        let spawned = spawn_child(&file, &args, &crate::Value::Unit);
        spawn_box(spawned.child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execFileOptsImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|file, args, options| {
        let file = file.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        let spawned = spawn_child(&file, &args, &options);
        spawn_box(spawned.child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execFileCbImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|file, args, callback| {
        let file = file.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        let child = exec_async(&file, &args, &crate::Value::Unit, callback);
        spawn_box(child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_execFileOptsCbImpl() -> crate::UnknownType {
    crate::Value::Func4(purust_core::Func4::Shared(Rc::new(
        |file, args, options, callback| {
            let file = file.unwrap_string();
            let args: Vec<String> = args
                .unwrap_array()
                .iter()
                .map(|value| value.unwrap_string())
                .collect();
            let child = exec_async(&file, &args, &options, callback);
            spawn_box(child)
        },
    )))
}

pub fn Node_UnsafeChildProcess_Unsafe_unsafeSOBToString(
    value: Rc<StringOrBuffer>,
) -> String {
    string_or_buffer_text(&value)
}

pub fn Node_UnsafeChildProcess_Unsafe_unsafeSOBToBuffer(
    value: Rc<StringOrBuffer>,
) -> Rc<Purs_Node_Buffer_Immutable::ImmutableBuffer> {
    match value.as_ref() {
        StringOrBuffer::Str(text) => Purs_Node_Buffer_Immutable::purust_buffer_from_bytes(
            purust_core::purust_string_to_utf8_lossy(text).into_bytes(),
        ),
        StringOrBuffer::Buffer(buffer) => buffer.clone(),
    }
}

pub fn spawn_box(child: Rc<EventEmitter>) -> crate::UnknownType {
    crate::Value::Class(Rc::new(child))
}

pub fn Node_UnsafeChildProcess_Unsafe_spawnImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|file, args| {
        let file = file.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        let spawned = spawn_child(&file, &args, &crate::Value::Unit);
        spawn_box(spawned.child)
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_spawnOptsImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|file, args, options| {
        let file = file.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        let spawned = spawn_child(&file, &args, &options);
        spawn_box(spawned.child)
    })))
}

fn fork_child(module_path: &str, args: &[String], options: &crate::UnknownType) -> Rc<EventEmitter> {
    // Without Node's IPC channel the child is spawned directly; `connected`
    // stays false and `send` returns false, like a disconnected child.
    let mut child_args = vec![module_path.to_owned()];
    child_args.extend(args.iter().cloned());
    let spawned = spawn_child("node", &child_args, options);
    spawned.child
}

pub fn Node_UnsafeChildProcess_Unsafe_forkImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|module_path, args| {
        let module_path = module_path.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        spawn_box(fork_child(&module_path, &args, &crate::Value::Unit))
    })))
}

pub fn Node_UnsafeChildProcess_Unsafe_forkOptsImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(|module_path, args, options| {
        let module_path = module_path.unwrap_string();
        let args: Vec<String> = args
            .unwrap_array()
            .iter()
            .map(|value| value.unwrap_string())
            .collect();
        spawn_box(fork_child(&module_path, &args, &options))
    })))
}

// ---------------------------------------------------------------------------
// IPC
// ---------------------------------------------------------------------------

fn send_unavailable(callback: Option<crate::UnknownType>) -> crate::UnknownType {
    if let Some(callback) = callback {
        callback.unwrap_func1()(class_nullable(None));
    }
    crate::mk_bool(false)
}

pub fn Node_UnsafeChildProcess_Unsafe_sendImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(
        |_child, _message, _handle| send_unavailable(None),
    )))
}

pub fn Node_UnsafeChildProcess_Unsafe_sendOptsImpl() -> crate::UnknownType {
    crate::Value::Func4(purust_core::Func4::Shared(Rc::new(
        |_child, _message, _handle, _options| send_unavailable(None),
    )))
}

pub fn Node_UnsafeChildProcess_Unsafe_sendCbImpl() -> crate::UnknownType {
    crate::Value::Func4(purust_core::Func4::Shared(Rc::new(
        |_child, _message, _handle, callback| send_unavailable(Some(callback)),
    )))
}

pub fn Node_UnsafeChildProcess_Unsafe_sendOptsCbImpl() -> crate::UnknownType {
    crate::Value::Func5(purust_core::Func5::Shared(Rc::new(
        |_child, _message, _handle, _options, callback| send_unavailable(Some(callback)),
    )))
}

pub fn Node_UnsafeChildProcess_Unsafe_unsafeChannelRefImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| crate::Value::Unit))
}

pub fn Node_UnsafeChildProcess_Unsafe_unsafeChannelUnrefImpl() -> crate::UnknownType {
    Node_UnsafeChildProcess_Unsafe_unsafeChannelRefImpl()
}
