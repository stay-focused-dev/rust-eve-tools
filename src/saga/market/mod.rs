use crate::AppContext;
use crate::esi;
use crate::{MarketOrder, RegionId, TypeId};

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct WorkItem {
    pub id: Uuid,
    pub work_type: WorkType,
    pub priority: u8,
    pub created_at: Instant,
    pub retry_count: u32,
}

impl PartialEq for WorkItem {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for WorkItem {}

impl PartialOrd for WorkItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.work_type.cmp(&other.work_type)
    }
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub enum WorkType {
    MarketOrderSell {
        region_id: RegionId,
        type_id: TypeId,
        page: usize,
    },
    MarketOrderBuy {
        region_id: RegionId,
        type_id: TypeId,
        page: usize,
    },
}

#[derive(Debug)]
pub enum SagaStatus {
    Started,
    Processing,
    Completed,
}

pub enum SagaEvent {
    SagaStarted,
    WorkCompleted { work_id: Uuid, result: WorkResult },
    WorkFailed { work_id: Uuid, error: String },
}

pub struct MarketResolutionSaga {
    pub workflow_id: Uuid,
    pub context: Arc<AppContext>,
    pub status: SagaStatus,

    pub market_orders_sell_queue: BTreeSet<WorkItem>,
    pub market_orders_buy_queue: BTreeSet<WorkItem>,

    pub in_flight_work: HashMap<Uuid, WorkItem>,

    pub resolved_market_orders_sell: BTreeSet<(RegionId, TypeId, usize)>,
    pub resolved_market_orders_buy: BTreeSet<(RegionId, TypeId, usize)>,
}

impl MarketResolutionSaga {
    pub fn new(context: Arc<AppContext>) -> Self {
        MarketResolutionSaga {
            workflow_id: Uuid::new_v4(),
            status: SagaStatus::Started,
            context,
            market_orders_sell_queue: BTreeSet::new(),
            market_orders_buy_queue: BTreeSet::new(),
            in_flight_work: HashMap::new(),
            resolved_market_orders_sell: BTreeSet::new(),
            resolved_market_orders_buy: BTreeSet::new(),
        }
    }

    pub fn get_work(&mut self, worker_type: WorkerType) -> Option<WorkItem> {
        let work_item = match worker_type {
            WorkerType::MarketOrders => self
                .market_orders_sell_queue
                .pop_first()
                .or_else(|| self.market_orders_buy_queue.pop_first()),
        };

        if let Some(ref item) = work_item {
            self.in_flight_work.insert(item.id, item.clone());
        }

        work_item
    }

    pub async fn handle_event(&mut self, event: SagaEvent) -> Result<(), SagaError> {
        match event {
            SagaEvent::SagaStarted => {
                self.status = SagaStatus::Processing;

                // (region_id = 10000002, type_id = 44992, page = 1) - plex
                // (region_id = 10000002, type_id = 40520, page = 1) - LSI
                // (region_id = 10000002, type_id = 40519, page = 1) - Skill Extractor
                let data = vec![
                    (10000002, 44992, 1),
                    (10000002, 40520, 1),
                    (10000002, 40519, 1),
                ];

                for (region_id, type_id, page) in data {
                    let type_id = type_id.into();
                    
                    self.market_orders_buy_queue.insert(WorkItem {
                        id: Uuid::new_v4(),
                        work_type: WorkType::MarketOrderBuy {
                            region_id,
                            type_id,
                            page,
                        },
                        priority: 5,
                        created_at: Instant::now(),
                        retry_count: 0,
                    });

                    self.market_orders_sell_queue.insert(WorkItem {
                        id: Uuid::new_v4(),
                        work_type: WorkType::MarketOrderSell {
                            region_id,
                            type_id,
                            page,
                        },
                        priority: 5,
                        created_at: Instant::now(),
                        retry_count: 0,
                    });
                }
            }
            SagaEvent::WorkCompleted { work_id, result } => {
                if let Some(_work_item) = self.in_flight_work.remove(&work_id) {
                    match result {
                        WorkResult::MarketOrdersSell {
                            region_id,
                            type_id,
                            orders,
                            page,
                            total_pages,
                        } => {
                            self.resolved_market_orders_sell
                                .insert((region_id, type_id, page));

                            println!("market orders: {:?}", orders);

                            if page == 1 {
                                for page in 2..=total_pages {
                                    let work_item = WorkItem {
                                        id: Uuid::new_v4(),
                                        work_type: WorkType::MarketOrderSell {
                                            region_id,
                                            type_id,
                                            page,
                                        },
                                        priority: 5,
                                        created_at: Instant::now(),
                                        retry_count: 0,
                                    };
                                    self.market_orders_sell_queue.insert(work_item);
                                }
                            }
                        }
                        WorkResult::MarketOrdersBuy {
                            region_id,
                            type_id,
                            orders,
                            page,
                            total_pages,
                        } => {
                            self.resolved_market_orders_buy
                                .insert((region_id, type_id, page));

                            println!("market orders: {:?}", orders);

                            if page == 1 {
                                for page in 2..=total_pages {
                                    let work_item = WorkItem {
                                        id: Uuid::new_v4(),
                                        work_type: WorkType::MarketOrderBuy {
                                            region_id,
                                            type_id,
                                            page,
                                        },
                                        priority: 5,
                                        created_at: Instant::now(),
                                        retry_count: 0,
                                    };
                                    self.market_orders_buy_queue.insert(work_item);
                                }
                            }
                        }
                    }
                }
            }
            SagaEvent::WorkFailed { work_id, error } => {
                if let Some(mut work_item) = self.in_flight_work.remove(&work_id) {
                    work_item.retry_count += 1;

                    if work_item.retry_count < 3 {
                        match &work_item.work_type {
                            WorkType::MarketOrderSell { .. } => {
                                self.market_orders_sell_queue.insert(work_item);
                            }
                            WorkType::MarketOrderBuy { .. } => {
                                self.market_orders_buy_queue.insert(work_item);
                            }
                        }
                    } else {
                        eprintln!(
                            "Work item failed permanently: {:?}, error: {}",
                            work_item, error
                        );
                    }
                }
            }
        }

        if self.is_complete() {
            self.status = SagaStatus::Completed;
        }

        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.in_flight_work.is_empty()
            && self.market_orders_sell_queue.is_empty()
            && self.market_orders_buy_queue.is_empty()
    }
}
#[derive(Debug, Error)]
pub enum SagaError {
    #[error("Invalid saga state")]
    InvalidState,
    #[error("Processing error: {0}")]
    ProcessingError(String),
}

pub struct Worker {
    worker_id: Uuid,
    worker_type: WorkerType,
    saga: Arc<RwLock<MarketResolutionSaga>>,
    context: Arc<AppContext>,
}

#[derive(Clone)]
pub enum WorkerType {
    MarketOrders,
}

impl Worker {
    pub fn new(
        worker_type: WorkerType,
        saga: Arc<RwLock<MarketResolutionSaga>>,
        context: Arc<AppContext>,
    ) -> Self {
        let worker_id = Uuid::new_v4();
        Worker {
            worker_id,
            worker_type,
            saga,
            context,
        }
    }

    pub async fn start(&self) -> Result<(), SagaError> {
        loop {
            let work_item = {
                let mut saga = self.saga.write().await;
                saga.get_work(self.worker_type.clone())
            };

            if let Some(work_item) = work_item {
                match self.process_work_item(work_item.clone()).await {
                    Ok(result) => {
                        let mut saga = self.saga.write().await;
                        saga.handle_event(SagaEvent::WorkCompleted {
                            work_id: work_item.id,
                            result,
                        })
                        .await?;
                    }
                    Err(error) => {
                        let mut saga = self.saga.write().await;
                        saga.handle_event(SagaEvent::WorkFailed {
                            work_id: work_item.id,
                            error: error.to_string(),
                        })
                        .await?;
                    }
                }
            }

            {
                let saga = self.saga.read().await;
                if saga.is_complete() {
                    break;
                }
            }
        }

        println!("worker finished, id: {}", self.worker_id);

        Ok(())
    }

    async fn process_work_item(&self, work_item: WorkItem) -> Result<WorkResult, WorkerError> {
        let result = match work_item.work_type {
            WorkType::MarketOrderSell {
                region_id,
                type_id,
                page,
            } => {
                let (orders, total_pages) =
                    esi::get_sell_orders(&self.context.http_client, region_id, type_id, page)
                        .await
                        .map_err(|e| WorkerError::EsiError(e.to_string()))?;

                Ok(WorkResult::MarketOrdersSell {
                    region_id,
                    type_id,
                    orders,
                    page,
                    total_pages,
                })
            }
            WorkType::MarketOrderBuy {
                region_id,
                type_id,
                page,
            } => {
                let (orders, total_pages) =
                    esi::get_buy_orders(&self.context.http_client, region_id, type_id, page)
                        .await
                        .map_err(|e| WorkerError::EsiError(e.to_string()))?;

                Ok(WorkResult::MarketOrdersBuy {
                    region_id,
                    type_id,
                    orders,
                    page,
                    total_pages,
                })
            }
        };

        result
    }
}

pub enum WorkResult {
    MarketOrdersSell {
        region_id: RegionId,
        type_id: TypeId,
        orders: Vec<MarketOrder>,
        page: usize,
        total_pages: usize,
    },
    MarketOrdersBuy {
        region_id: RegionId,
        type_id: TypeId,
        orders: Vec<MarketOrder>,
        page: usize,
        total_pages: usize,
    },
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("ESI client error: {0}")]
    EsiError(String),
    #[error("Saga error: {0}")]
    SagaError(String),
}
