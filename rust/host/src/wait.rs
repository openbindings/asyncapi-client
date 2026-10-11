use dynamic_asyncapi_session::{RuntimeCode, RuntimeError};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    task::{Context, Waker},
};
use wasm_bindgen::prelude::*;

/// Cancellation is explicit and reusable. Cancellation of one receive wait
/// does not consume a queued message or close the session.
#[derive(Clone, Default)]
pub struct Cancellation {
    cancelled: Rc<Cell<bool>>,
    signal: Signal,
}
impl Cancellation {
    pub fn cancel(&self) {
        self.cancelled.set(true);
        self.signal.wake();
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.get()
    }
    pub(crate) fn register(&self) -> Registration {
        self.signal.register()
    }
}
#[derive(Clone, Default)]
pub(crate) struct Signal(Rc<RefCell<Vec<Option<Waker>>>>);
pub(crate) struct Registration {
    signal: Signal,
    slot: usize,
}
impl Signal {
    pub fn register(&self) -> Registration {
        // A slot with no waker can still belong to a live registration. Use a
        // placeholder until its future is polled, not None (which means free).
        let mut slots = self.0.borrow_mut();
        let slot = slots
            .iter()
            .position(Option::is_none)
            .unwrap_or(slots.len());
        if slot == slots.len() {
            slots.push(Some(Waker::noop().clone()));
        } else {
            slots[slot] = Some(Waker::noop().clone());
        }
        Registration {
            signal: self.clone(),
            slot,
        }
    }
    pub fn wake(&self) {
        let waiters: Vec<_> = self.0.borrow().iter().filter_map(Clone::clone).collect();
        for waiter in waiters {
            waiter.wake();
        }
    }
}
impl Registration {
    pub fn poll(&self, cx: &Context<'_>) {
        self.signal.0.borrow_mut()[self.slot] = Some(cx.waker().clone());
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.signal.0.borrow_mut()[self.slot] = None;
    }
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(catch, js_name = setTimeout)]
    fn set_timeout(callback: &js_sys::Function, delay: i32) -> Result<i32, JsValue>;
    #[wasm_bindgen(js_name = clearTimeout)]
    fn clear_timeout(id: i32);
}
pub(crate) struct Timer {
    id: i32,
    elapsed: Rc<Cell<bool>>,
    signal: Signal,
    _callback: Closure<dyn FnMut()>,
}
impl Timer {
    pub fn new(milliseconds: u32) -> Result<Self, RuntimeError> {
        let elapsed = Rc::new(Cell::new(false));
        let signal = Signal::default();
        let done = elapsed.clone();
        let wake = signal.clone();
        let callback = Closure::new(move || {
            done.set(true);
            wake.wake();
        });
        let id =
            set_timeout(callback.as_ref().unchecked_ref(), milliseconds as i32).map_err(|_| {
                RuntimeError::new(
                    RuntimeCode::Unsupported,
                    "host timer capability is unavailable",
                )
            })?;
        Ok(Self {
            id,
            elapsed,
            signal,
            _callback: callback,
        })
    }
    pub fn elapsed(&self) -> bool {
        self.elapsed.get()
    }
    pub fn register(&self) -> Registration {
        self.signal.register()
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        clear_timeout(self.id);
    }
}
