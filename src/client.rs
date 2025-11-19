use http::Error as HttpError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Error, RequestBuilder, Response};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::RatelimitGroup;

pub struct RatelimitedClient {
    inner: Client,
    ratelimit_group: Arc<Mutex<RatelimitGroup>>,
}

impl RatelimitedClient {
    pub fn new(ratelimit_group: RatelimitGroup) -> Self {
        RatelimitedClient {
            inner: Client::new(),
            ratelimit_group: Arc::new(Mutex::new(ratelimit_group)),
        }
    }

    pub fn with_client(client: Client, ratelimit_group: RatelimitGroup) -> Self {
        RatelimitedClient {
            inner: client,
            ratelimit_group: Arc::new(Mutex::new(ratelimit_group)),
        }
    }

    pub fn get(&self, url: impl AsRef<str>) -> RatelimitedRequestBuilder {
        RatelimitedRequestBuilder {
            builder: self.inner.get(url.as_ref()),
            ratelimit_group: Arc::clone(&self.ratelimit_group),
        }
    }

    pub fn post(&self, url: impl AsRef<str>) -> RatelimitedRequestBuilder {
        RatelimitedRequestBuilder {
            builder: self.inner.post(url.as_ref()),
            ratelimit_group: Arc::clone(&self.ratelimit_group),
        }
    }
}

pub struct RatelimitedRequestBuilder {
    builder: RequestBuilder,

    ratelimit_group: Arc<Mutex<RatelimitGroup>>,
}

impl RatelimitedRequestBuilder {
    pub fn header<K, V>(self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<HttpError>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<HttpError>,
    {
        RatelimitedRequestBuilder {
            builder: self.builder.header(key, value),
            ratelimit_group: self.ratelimit_group,
        }
    }

    pub fn headers(self, headers: HeaderMap) -> Self {
        RatelimitedRequestBuilder {
            builder: self.builder.headers(headers),
            ratelimit_group: self.ratelimit_group,
        }
    }

    pub fn json<T: serde::Serialize + ?Sized>(self, json: &T) -> Self {
        RatelimitedRequestBuilder {
            builder: self.builder.json(json),
            ratelimit_group: self.ratelimit_group,
        }
    }

    pub async fn send(self) -> Result<Response, Error> {
        let ratelimit_group = self.ratelimit_group;

        loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0));

            let mut ratelimit_group_guard = ratelimit_group.lock().await;
            if let Some(wait_time) = ratelimit_group_guard.hit_at(now) {
                drop(ratelimit_group_guard);

                sleep(wait_time).await;
                continue;
            }

            break;
        }
        self.builder.send().await
    }
}
