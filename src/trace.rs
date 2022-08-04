// NOTE/TODO: We don't yet know the penalty of tracing. Defer this integration.

use tracing::span::{self, Attributes, Record};
use tracing::{debug, field::Field, Event, Level, Metadata};

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::RwLock;

// metadata
//     span  - id, parent
//           - name, target, level, fields, call_site, module_path, file, line
//     event - name, target, level, fields, call_site, module_path, file, line
//
// span - id, parent

pub struct TraceRecorder {
    name: String,
    ids: RwLock<Vec<Id>>,
    next_span: AtomicU64,
}

struct Id {
    is_span: bool,
    name: String,
    target: String,
    level: Level,
    fields: Vec<Field>,
    file: Option<String>,
    line: Option<u32>,
    module_path: Option<String>,
}

impl Default for Id {
    fn default() -> Id {
        Id {
            is_span: bool::default(),
            name: String::default(),
            target: String::default(),
            level: Level::INFO,
            fields: Vec::default(),
            file: None,
            line: None,
            module_path: None,
        }
    }
}

impl<'a, 'b> From<&'b Metadata<'a>> for Id {
    fn from(val: &'b Metadata<'a>) -> Self {
        let val = Id {
            is_span: val.is_span(),
            name: val.name().to_string(),
            target: val.target().to_string(),
            level: val.level().clone(),
            fields: val.fields().iter().collect(),
            file: val.file().map(|s| s.to_string()),
            line: val.line(),
            module_path: val.module_path().map(|s| s.to_string()),
        };
        val
    }
}

impl Default for TraceRecorder {
    fn default() -> Self {
        let ids = vec![Id::default()];
        let val = TraceRecorder {
            name: "mqtt-recorder".to_string(),
            ids: RwLock::new(ids),
            next_span: AtomicU64::new(1),
        };
        println!("start tracing {:?} ...", val.name);
        val
    }
}

impl tracing::Subscriber for TraceRecorder {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> span::Id {
        let md = attrs.metadata();
        println!("new_span {}", md.name());
        if md.is_span() {
            debug!("enabling span for {:?}", md.name());
        } else if md.is_event() {
            debug!("enabling event for {:?}", md.name());
        } else {
            unreachable!()
        }
        let id = span::Id::from_u64(self.next_span.fetch_add(1, SeqCst));
        self.ids.write().unwrap().push(md.into());
        id
    }

    fn record(&self, id: &span::Id, _values: &Record<'_>) {
        println!("record {}", id.into_u64());
    }

    fn record_follows_from(&self, id: &span::Id, _follows: &span::Id) {
        println!("record_follows_from {}", id.into_u64());
    }

    fn enter(&self, id: &span::Id) {
        println!("enter {}", id.into_u64());
    }

    fn event(&self, _event: &Event<'_>) {
        println!("event");
    }

    fn exit(&self, id: &span::Id) {
        println!("exit {}", id.into_u64());
    }
}
