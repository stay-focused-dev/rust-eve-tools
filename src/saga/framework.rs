// saga/framework.rs - Generic saga framework
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Core trait that defines saga-specific behavior
pub trait SagaProcessor: Clone + Send + Sync + 'static {
    /// The type of work to be performed
    type WorkType: Debug + Clone + PartialEq + Eq + PartialOrd + Ord + Send + Sync;

    /// Unique key for tracking work completion
    type WorkKey: Debug + Clone + PartialEq + Eq + PartialOrd + Ord + Hash + Send + Sync;

    /// The result of processing work
    type WorkResult: Clone + Send + Sync;

    /// Error type for this processor
    type Error: std::error::Error + Send + Sync;

    /// Context type containing shared resources
    type Context: Send + Sync;

    /// Initial event type that starts the saga
    type InitialEvent: Send + Sync;

    /// Convert work type to resolution key
    fn to_resolution_key(work_type: &Self::WorkType) -> Self::WorkKey;

    /// Handle the initial event and return initial work items
    fn handle_initial_event(
        event: Self::InitialEvent,
    ) -> Result<Vec<Self::WorkType>, SagaError<Self::Error>>;

    /// Process a work item and return the result
    fn process(
        context: &Arc<Self::Context>,
        work_type: &Self::WorkType,
    ) -> impl std::future::Future<Output = Result<Self::WorkResult, Self::Error>> + Send;

    /// Handle work result and return new work items
    fn handle(
        context: &Arc<Self::Context>,
        work_result: Self::WorkResult,
    ) -> impl std::future::Future<Output = Result<Vec<Self::WorkType>, Self::Error>> + Send;
}

/// Generic work item wrapper
pub struct WorkItem<P: SagaProcessor> {
    pub work_type: P::WorkType,
    pub created_at: Instant,
    pub retry_count: u32,
    pub work_resolution_key: P::WorkKey,
}

impl<P: SagaProcessor> WorkItem<P> {
    pub fn new(work_type: P::WorkType) -> Self {
        Self {
            work_resolution_key: P::to_resolution_key(&work_type),
            work_type,
            created_at: Instant::now(),
            retry_count: 0,
        }
    }
}

impl<P: SagaProcessor> Clone for WorkItem<P> {
    fn clone(&self) -> Self {
        Self {
            work_type: self.work_type.clone(),
            created_at: self.created_at,
            retry_count: self.retry_count,
            work_resolution_key: self.work_resolution_key.clone(),
        }
    }
}

impl<P: SagaProcessor> Debug for WorkItem<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkItem")
            .field("work_type", &self.work_type)
            .field("created_at", &self.created_at)
            .field("retry_count", &self.retry_count)
            .field("work_resolution_key", &self.work_resolution_key)
            .finish()
    }
}

impl<P: SagaProcessor> PartialEq for WorkItem<P> {
    fn eq(&self, other: &Self) -> bool {
        self.work_resolution_key == other.work_resolution_key
    }
}

impl<P: SagaProcessor> Eq for WorkItem<P> {}

impl<P: SagaProcessor> PartialOrd for WorkItem<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P: SagaProcessor> Ord for WorkItem<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.work_resolution_key.cmp(&other.work_resolution_key)
    }
}

/// Work message sent between workers and saga
pub struct WorkMessage<P: SagaProcessor> {
    pub work_resolution_key: P::WorkKey,
    pub work_result: Result<Vec<WorkItem<P>>, P::Error>,
}

/// Generic saga orchestrator
pub struct Saga<P: SagaProcessor> {
    pub workflow_id: Uuid,
    pub status: SagaStatus,

    pub pending: BTreeSet<WorkItem<P>>,
    pub in_flight_work: HashMap<P::WorkKey, WorkItem<P>>,
    pub resolved: BTreeSet<P::WorkKey>,

    context: Arc<P::Context>,
    workers_count: usize,
    work_sender: mpsc::UnboundedSender<WorkItem<P>>,
    result_receiver: mpsc::UnboundedReceiver<WorkMessage<P>>,
    shared_work_receiver: Arc<Mutex<mpsc::UnboundedReceiver<WorkItem<P>>>>,
    result_sender: mpsc::UnboundedSender<WorkMessage<P>>,
    max_retries: u32,
}

const MAX_RETRIES: u32 = 3;

impl<P: SagaProcessor> Saga<P> {
    pub fn new(context: Arc<P::Context>, workers_count: usize) -> Self {
        Self::with_max_retries(context, workers_count, MAX_RETRIES)
    }

    pub fn with_max_retries(
        context: Arc<P::Context>,
        workers_count: usize,
        max_retries: u32,
    ) -> Self {
        let (work_sender, work_receiver) = mpsc::unbounded_channel();
        let (result_sender, result_receiver) = mpsc::unbounded_channel();
        let shared_work_receiver = Arc::new(Mutex::new(work_receiver));

        Self {
            workflow_id: Uuid::new_v4(),
            status: SagaStatus::Started,
            pending: BTreeSet::new(),
            in_flight_work: HashMap::new(),
            resolved: BTreeSet::new(),
            context,
            workers_count,
            work_sender,
            result_receiver,
            shared_work_receiver,
            result_sender,
            max_retries,
        }
    }

    pub fn print_pending_summary(&self, count: usize) {
        let first_pending: Vec<&WorkItem<P>> = self.pending.iter().take(count).collect();
        println!(
            "First {} / {} pending: {:?}",
            count.min(self.pending.len()),
            self.pending.len(),
            first_pending
        );
    }

    pub async fn start_with_event(
        mut self,
        initial_event: P::InitialEvent,
    ) -> Result<(), SagaError<P::Error>> {
        // Start workers
        let mut worker_handles: Vec<JoinHandle<()>> = vec![];

        for _ in 0..self.workers_count {
            let worker = Worker::<P>::new(
                self.context.clone(),
                self.shared_work_receiver.clone(),
                self.result_sender.clone(),
            );

            let handle = tokio::spawn(async move { worker.start().await });
            worker_handles.push(handle);
        }

        // Handle initial event
        let initial_work = P::handle_initial_event(initial_event)?;
        for work_type in initial_work {
            self.pending.insert(WorkItem::new(work_type));
        }

        self.status = SagaStatus::Processing;

        // Main processing loop
        loop {
            self.print_pending_summary(6);

            // Send work if available
            if let Some(work_item) = self.get_work() {
                if let Err(e) = self.work_sender.send(work_item) {
                    eprintln!("Unable to send work item: {}", e);
                }
            }

            // Receive results
            if let Some(message) = self.result_receiver.recv().await {
                let work_resolution_key = message.work_resolution_key;

                match message.work_result {
                    Ok(new_work_items) => {
                        self.handle_work_completed(work_resolution_key, new_work_items)?;
                    }
                    Err(e) => {
                        self.handle_work_failed(work_resolution_key, e)?;
                    }
                }

                if self.is_complete() {
                    println!("Saga completed successfully");
                    self.status = SagaStatus::Completed;
                    break;
                }
            } else {
                println!("Result channel closed");
                break;
            }
        }

        // Cleanup
        drop(self.work_sender);

        for handle in worker_handles {
            if let Err(e) = handle.await {
                eprintln!("Worker task failed: {}", e);
            }
        }

        Ok(())
    }

    fn handle_work_completed(
        &mut self,
        work_resolution_key: P::WorkKey,
        new_work_items: Vec<WorkItem<P>>,
    ) -> Result<(), SagaError<P::Error>> {
        println!(
            "Work completed: {:?}, new items: {}",
            work_resolution_key,
            new_work_items.len()
        );

        if let Some(work_item) = self.in_flight_work.remove(&work_resolution_key) {
            self.resolved.insert(work_item.work_resolution_key);

            for work_item in new_work_items {
                let key = work_item.work_resolution_key.clone();
                if !self.is_resolved(&key) {
                    self.pending.insert(work_item);
                }
            }
        } else {
            eprintln!(
                "Unable to find work item for key: {:?}",
                work_resolution_key
            );
        }

        Ok(())
    }

    fn handle_work_failed(
        &mut self,
        work_resolution_key: P::WorkKey,
        error: P::Error,
    ) -> Result<(), SagaError<P::Error>> {
        if let Some(mut work_item) = self.in_flight_work.remove(&work_resolution_key) {
            work_item.retry_count += 1;
            if work_item.retry_count < self.max_retries {
                println!(
                    "Retrying work item (attempt {}): {:?}",
                    work_item.retry_count + 1,
                    work_resolution_key
                );
                self.pending.insert(work_item);
            } else {
                eprintln!(
                    "Work item failed after {} retries: {:?}, error: {}",
                    self.max_retries, work_resolution_key, error
                );
                return Err(SagaError::ProcessingError(error));
            }
        }
        Ok(())
    }

    fn get_work(&mut self) -> Option<WorkItem<P>> {
        while let Some(work_item) = self.pending.pop_first() {
            if self.is_resolved(&work_item.work_resolution_key) {
                continue;
            }

            if self
                .in_flight_work
                .contains_key(&work_item.work_resolution_key)
            {
                continue;
            }

            self.in_flight_work
                .insert(work_item.work_resolution_key.clone(), work_item.clone());

            return Some(work_item);
        }

        None
    }

    fn is_complete(&self) -> bool {
        self.in_flight_work.is_empty() && self.pending.is_empty()
    }

    fn is_resolved(&self, key: &P::WorkKey) -> bool {
        self.in_flight_work.contains_key(key) || self.resolved.contains(key)
    }
}

/// Generic worker
struct Worker<P: SagaProcessor> {
    worker_id: Uuid,
    context: Arc<P::Context>,
    work_receiver: Arc<Mutex<mpsc::UnboundedReceiver<WorkItem<P>>>>,
    result_sender: mpsc::UnboundedSender<WorkMessage<P>>,
}

impl<P: SagaProcessor> Worker<P> {
    fn new(
        context: Arc<P::Context>,
        work_receiver: Arc<Mutex<mpsc::UnboundedReceiver<WorkItem<P>>>>,
        result_sender: mpsc::UnboundedSender<WorkMessage<P>>,
    ) -> Self {
        Self {
            worker_id: Uuid::new_v4(),
            context,
            work_receiver,
            result_sender,
        }
    }

    async fn start(&self) {
        loop {
            let maybe_work_item = {
                let mut receiver = self.work_receiver.lock().await;
                receiver.recv().await
            };

            if let Some(work_item) = maybe_work_item {
                println!(
                    "Worker {} processing: {:?}",
                    self.worker_id, work_item.work_type
                );

                let work_resolution_key = work_item.work_resolution_key.clone();

                let work_message = match P::process(&self.context, &work_item.work_type).await {
                    Ok(work_result) => match P::handle(&self.context, work_result).await {
                        Ok(new_work_types) => {
                            let new_items = new_work_types.into_iter().map(WorkItem::new).collect();
                            WorkMessage {
                                work_resolution_key,
                                work_result: Ok(new_items),
                            }
                        }
                        Err(e) => WorkMessage {
                            work_resolution_key,
                            work_result: Err(e),
                        },
                    },
                    Err(e) => WorkMessage {
                        work_resolution_key,
                        work_result: Err(e),
                    },
                };

                if let Err(e) = self.result_sender.send(work_message) {
                    eprintln!("Error sending work message: {}", e);
                }
            } else {
                println!("Worker {} shutting down", self.worker_id);
                break;
            }
        }
    }
}

#[derive(Debug)]
pub enum SagaStatus {
    Started,
    Processing,
    Completed,
}

#[derive(Debug, Error)]
pub enum SagaError<E: std::error::Error> {
    #[error("Invalid saga state")]
    InvalidState,
    #[error("Processing error: {0}")]
    ProcessingError(E),
}
