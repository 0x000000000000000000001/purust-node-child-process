// Safe accessors over a child process. Everything reads the native state stored
// in the event emitter, mirroring Node's `ChildProcess` properties.
use std::rc::Rc;

use Purs_Node_ChildProcess_Types::{
    purust_child_state, purust_signal_number, UnsafeChildProcess,
};
use Purs_Node_EventEmitter::EventEmitter;

fn unbox_child(value: &crate::UnknownType) -> Rc<EventEmitter> {
    value.unwrap_class::<Rc<EventEmitter>>().clone()
}

fn nullable(value: Option<crate::UnknownType>) -> crate::UnknownType {
    let nullable = match value {
        Some(value) => Purs_Data_Nullable::Data_Nullable_notNull(value),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    };
    crate::Value::Class(Rc::new(nullable))
}

pub fn Node_UnsafeChildProcess_Safe_pidImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let child = unbox_child(&value);
        let pid = purust_child_state(&child).lock().unwrap().pid;
        nullable(if pid > 0 { Some(crate::mk_int(pid)) } else { None })
    })))
}

pub fn Node_UnsafeChildProcess_Safe_connectedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let child = unbox_child(&value);
        let connected = purust_child_state(&child).lock().unwrap().connected;
        crate::mk_bool(connected)
    })))
}

pub fn Node_UnsafeChildProcess_Safe_exitCodeImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let child = unbox_child(&value);
        let code = purust_child_state(&child).lock().unwrap().exit_code;
        nullable(code.map(crate::mk_int))
    })))
}

pub fn Node_UnsafeChildProcess_Safe_disconnectImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let child = unbox_child(&value);
        purust_child_state(&child).lock().unwrap().connected = false;
        crate::Value::Unit
    })))
}

fn send_signal(child: &Rc<EventEmitter>, signal: i32) -> bool {
    let (pid, exited) = {
        let state = purust_child_state(child);
        let state = state.lock().unwrap();
        (state.pid, state.exited)
    };
    if pid <= 0 || exited {
        return false;
    }
    let result = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if result == 0 {
        if signal != 0 {
            let state = purust_child_state(child);
            state.lock().unwrap().killed = true;
        }
        true
    } else {
        false
    }
}

pub fn Node_UnsafeChildProcess_Safe_killImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let child = unbox_child(&value);
        crate::mk_bool(send_signal(&child, libc::SIGTERM))
    })))
}

pub fn Node_UnsafeChildProcess_Safe_killStrImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, signal| {
        let child = unbox_child(&value);
        let signal = signal
            .unwrap_class::<Rc<Purs_Node_ChildProcess_Types::KillSignal>>()
            .clone();
        crate::mk_bool(send_signal(&child, purust_signal_number(&signal)))
    })))
}

pub fn Node_UnsafeChildProcess_Safe_killedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let child = unbox_child(&value);
        let killed = purust_child_state(&child).lock().unwrap().killed;
        crate::mk_bool(killed)
    })))
}

pub fn Node_UnsafeChildProcess_Safe_refImpl() -> crate::UnknownType {
    // Native processes are not referenced through the event loop: `ref` and
    // `unref` keep the same observable state.
    crate::Value::Func1(purust_core::Func1::Static(|_| crate::Value::Unit))
}

pub fn Node_UnsafeChildProcess_Safe_unrefImpl() -> crate::UnknownType {
    Node_UnsafeChildProcess_Safe_refImpl()
}

pub fn Node_UnsafeChildProcess_Safe_signalCodeImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let child = unbox_child(&value);
        let signal = purust_child_state(&child).lock().unwrap().signal_code.clone();
        nullable(signal.map(crate::Value::String))
    })))
}

pub fn Node_UnsafeChildProcess_Safe_spawnArgs(child: Rc<UnsafeChildProcess>) -> crate::UnknownType {
    let args = purust_child_state(&child).lock().unwrap().spawn_args.clone();
    let values = args
        .iter()
        .map(|argument| crate::Value::String(purust_core::purust_string_from_utf8(argument)))
        .collect();
    crate::mk_array(values)
}

pub fn Node_UnsafeChildProcess_Safe_spawnFile(child: Rc<UnsafeChildProcess>) -> String {
    let file = purust_child_state(&child).lock().unwrap().spawn_file.clone();
    purust_core::purust_string_to_utf8_lossy(&file)
}

pub fn Node_UnsafeChildProcess_Safe_stdio(child: Rc<UnsafeChildProcess>) -> crate::UnknownType {
    let stdio = purust_child_state(&child).lock().unwrap().stdio.clone();
    crate::mk_array(stdio)
}
