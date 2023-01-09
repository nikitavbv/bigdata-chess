use {
    std::sync::Arc,
    tracing::Level,
    tracing_subscriber::{
        prelude::*, 
        filter::filter_fn, 
        Layer, 
        fmt::format::JsonVisitor, 
        field::VisitOutput,
    },
    bigdata_chess_core::queue::Queue,
};

pub fn init_logging(queue: Option<Arc<Queue>>) {
    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish()
        .with(filter_fn(|metadata| {     
            if metadata.target().starts_with("sqlx::query") {
                metadata.level() > &Level::INFO
            } else {
                true
            }
        }))
        .with(LogsToKafkaTopicLayer::new(queue))
        .init();
}

struct LogsToKafkaTopicLayer {
    rt: tokio::runtime::Runtime,
    queue: Option<Arc<Queue>>,
}

impl LogsToKafkaTopicLayer {
    pub fn new(queue: Option<Arc<Queue>>) -> Self {
        Self {
            rt: tokio::runtime::Runtime::new().unwrap(),
            queue,
        }
    }
}

impl<S> Layer<S> for LogsToKafkaTopicLayer where S: tracing::Subscriber {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().level() <= &Level::WARN {
            let mut data = String::new();
            let visitor = JsonVisitor::new(&mut data);
            visitor.visit(event).unwrap();

            if let Some(queue) = self.queue.as_ref() {
                let queue = queue.clone();
                self.rt.spawn(async move {
                    queue.send_log_message(data.as_bytes().to_vec()).await
                });
            }
        }
    }
}