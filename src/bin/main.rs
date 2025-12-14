#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_must_use)]
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};

use eve::esi;
use eve::sde;
use eve::{
    AllAssetsDb, AssetItem, AssetName, CharacterAssetsDb, CharacterId, DogmaAttribute,
    DogmaAttributeConcise, DogmaAttributeId, DynamicItem, DynamicsDb, ItemId, ItemType,
    MarketGroup, MarketGroupId, Station, StationId, TypeId,
};
use eve::{Ratelimit, RatelimitGroup, RatelimitedClient};
use oauth2::{
    self, AuthUrl, AuthorizationCode, ClientId, CsrfToken, EndpointNotSet, EndpointSet,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenUrl,
};
use oauth2::{
    StandardDeviceAuthorizationResponse,
    basic::{BasicClient, BasicTokenResponse},
};
use pprof::ProfilerGuardBuilder;
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration as TokioDuration, interval};
use tower_sessions::{MemoryStore, Session, SessionManagerLayer};

// Import our processing modules
use eve::AppContext;
use eve::handlers;
use eve::saga::assets;
use eve::saga::market::{self, MarketResolutionSaga};
use eve::{CharacterClient, CharacterManager, OauthConfig};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let ratelimit_group = RatelimitGroup::new(vec![
        Ratelimit::new(Duration::from_secs(1), 2),
        Ratelimit::new(Duration::from_secs(60), 120),
    ]);

    let port = 8080;

    let http_client = Arc::new(RatelimitedClient::new(ratelimit_group));
    let oauth_config = OauthConfig {
        client_id: ClientId::new("49f3698f399f4870afaf1f632592abe0".to_string()),
        auth_url: AuthUrl::new("https://login.eveonline.com/v2/oauth/authorize".to_string())
            .context("invalid auth url")?,
        token_url: TokenUrl::new("https://login.eveonline.com/v2/oauth/token".to_string())
            .context("invalid token url")?,
        redirect_url: RedirectUrl::new(format!("http://localhost:{port}/auth/callback"))
            .context("invalid redirect url")?,
    };

    let oauth2_client = Arc::new(
        BasicClient::new(oauth_config.client_id)
            .set_auth_uri(oauth_config.auth_url)
            .set_token_uri(oauth_config.token_url)
            .set_redirect_uri(oauth_config.redirect_url),
    );

    let context = Arc::new(
        AppContext::with_client(
            http_client.clone(),
            oauth2_client.clone(),
            "/Users/maxim/data/eve/sqlite-latest.sqlite",
            "/Users/maxim/data/eve/",
        )
        .await?,
    );

    let dynamics_stats = {
        let dynamics_db = context.dynamics_db.read().await;
        (
            dynamics_db.len(),
            dynamics_db.last_stored_at,
            dynamics_db.last_updated_at,
        )
    };
    println!("📊 Loaded {} dynamics from storage", dynamics_stats.0);
    println!(
        "📅 Last stored: {}, Last updated: {}",
        dynamics_stats.1, dynamics_stats.2
    );

    // Start statistics logger
    let stats_context = context.clone();
    let stats_task = tokio::spawn(async move {
        let mut interval = interval(TokioDuration::from_secs(10)); // Log every 10 seconds

        loop {
            interval.tick().await;

            println!("tick");
        }
    });

    let context_clone = context.clone();
    tokio::spawn(async move {
        println!("starting market orders resolution");
        match start_market_orders_resolution_system(context_clone).await {
            Ok(_) => println!("market orders resolution completed"),
            Err(e) => println!("market orders resolution failed: {}", e),
        }
    });

    let server_task = start_http_server(context.clone(), port).await;

    server_task.await.expect("Server task failed");
    println!("HTTP server stopped");

    stats_task.abort();

    let mut dynamics_db_guard = context.dynamics_db.write().await;
    println!("🏁 Main cleanup - about to store dynamics");
    dynamics_db_guard.store();

    println!("Application stopped");
    Ok(())
}

pub async fn start_assets_resolution_system(
    context: Arc<AppContext>,
    character_id: CharacterId,
) -> Result<()> {
    let workers_count = 3;

    let saga = assets::run_assets_saga(context.clone(), character_id, workers_count).await?;

    context.character_assets_db.store();

    println!("assets resolution completed");
    Ok(())
}

pub async fn start_market_orders_resolution_system(context: Arc<AppContext>) -> Result<()> {
    let saga = Arc::new(RwLock::new(MarketResolutionSaga::new(context.clone())));

    let mut worker_handles = Vec::new();
    for _ in 0..3 {
        let worker = market::Worker::new(
            market::WorkerType::MarketOrders,
            saga.clone(),
            context.clone(),
        );

        let handle = tokio::spawn(async move { worker.start().await });
        worker_handles.push(handle);
    }

    {
        let mut saga = saga.write().await;
        saga.handle_event(market::SagaEvent::SagaStarted).await?;
    }

    for handle in worker_handles {
        handle.await.context("Failed to join worker task")?;
    }

    println!("market orders resolution completed");
    Ok(())
}

async fn dynamics_report_handler(State(state): State<AppState>) -> impl IntoResponse {
    let context = &state.context;

    let report = match handlers::dynamics::DynamicsReport::new(context).await {
        Ok(report) => report,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("content-type", "application/json")
                .body(
                    serde_json::json!({
                        "error": format!("Failed to generate dynamics report: {}", e),
                        "status": "error"
                    })
                    .to_string(),
                )
                .unwrap();
        }
    };

    match serde_json::to_string(&report) {
        Ok(report_json) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(report_json)
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("content-type", "application/json")
            .body(
                serde_json::json!({
                    "error": format!("Failed to serialize dynamics report: {}", e),
                    "status": "error"
                })
                .to_string(),
            )
            .unwrap(),
    }
}

async fn profile_dynamics_report_handler(State(state): State<AppState>) -> impl IntoResponse {
    println!("Starting profiling of dynamics report...");

    let guard = ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| {
            eprintln!("Failed to build profiler: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Profiler error: {}", e),
            )
        });

    let guard = match guard {
        Ok(g) => g,
        Err(err_response) => return err_response.into_response(),
    };

    let start = Instant::now();
    let report = handlers::dynamics::DynamicsReport::new(&state.context).await;
    let duration = start.elapsed();

    match guard.report().build() {
        Ok(report_result) => {
            let mut body = vec![];

            match report_result.flamegraph(&mut body) {
                Ok(_) => {
                    println!("Generated flamegraph with {} bytes", body.len());
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "image/svg+xml")
                        .header(
                            "content-disposition",
                            "inline; filename=\"dynamics-profile.svg\"",
                        )
                        .body(Body::from(body))
                        .unwrap()
                }
                Err(e) => {
                    eprintln!("Failed to generate flamegraph: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Flamegraph generation error: {}", e),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to build profiling report: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Report generation error: {}", e),
            )
                .into_response()
        }
    }
}

#[derive(Clone)]
struct AppState {
    context: Arc<AppContext>,
}

async fn auth_start(State(state): State<AppState>, session: Session) -> Result<String, String> {
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let csrf_token = CsrfToken::new_random();

    let auth_session = AuthSession {
        pkce_verifier: pkce_verifier.secret().to_string(),
        csrf_token: csrf_token.secret().to_string(),
    };

    println!("pkce_challenge: {:?}", pkce_challenge);
    println!("pkce_verifier: {:?}", pkce_verifier.secret());
    println!("csrf_token: {:?}", csrf_token.secret());

    session
        .insert("auth_data", &auth_session)
        .await
        .map_err(|e| format!("failed to store auth data: {e}"))?;

    let oauth2_client = &state.context.oauth2_client;

    let (auth_url, _) = oauth2_client
        .authorize_url(|| csrf_token)
        .add_scope(Scope::new("esi-assets.read_assets.v1".to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    println!("auth_url: {}", auth_url);
    Ok(format!("go to {auth_url}"))
}

async fn list_characters_handler(State(state): State<AppState>) -> Result<String, String> {
    let guard = state.context.characters.lock().await;
    let characters = guard.list();
    Ok(format!("Characters: {:?}", characters))
}

async fn auth_callback(
    State(state): State<AppState>,
    session: Session,
    Query(params): Query<CallbackParams>,
) -> Result<String, String> {
    let auth_data: AuthSession = session
        .get("auth_data")
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "No auth data found in session".to_string())?;

    if auth_data.csrf_token != params.state {
        return Err("Invalid CSRF token".to_string());
    }

    let pkce_verifier = PkceCodeVerifier::new(auth_data.pkce_verifier);
    session.remove::<AuthSession>("auth_data").await.ok();

    let oauth2_client = &state.context.oauth2_client;

    let oauth2_token = oauth2_client
        .exchange_code(AuthorizationCode::new(params.code))
        .set_pkce_verifier(pkce_verifier)
        .request_async(&reqwest::Client::new())
        .await
        .context("token exchange failed")
        .map_err(|e| e.to_string())?;

    let http_client = state.context.http_client.as_ref();
    let character_info = esi::get_character_info(http_client, &oauth2_token)
        .await
        .map_err(|e| e.to_string())?;

    {
        state.context.characters.lock().await.add(CharacterClient {
            character_id: character_info.character_id,
            character_name: character_info.character_name,
            oauth_token: oauth2_token,
        })
    }

    tokio::spawn(async move {
        println!(
            "starting asset resolution for character {}",
            character_info.character_id
        );
        match start_assets_resolution_system(state.context.clone(), character_info.character_id)
            .await
        {
            Ok(_) => println!(
                "asset resolution for character {} completed",
                character_info.character_id
            ),
            Err(e) => println!(
                "asset resolution for character {} failed: {}",
                character_info.character_id, e
            ),
        }
    });

    Ok("auth successful".to_string())
}

#[derive(Serialize, Deserialize)]
struct AuthSession {
    pkce_verifier: String,
    csrf_token: String,
}

async fn start_http_server(context: Arc<AppContext>, port: u16) -> tokio::task::JoinHandle<()> {
    let session_store = MemoryStore::default();
    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_same_site(tower_sessions::cookie::SameSite::Lax)
        .with_expiry(tower_sessions::Expiry::OnInactivity(
            tower_sessions::cookie::time::Duration::new(600, 0),
        ));

    let app = Router::new()
        .route("/auth/start", get(auth_start))
        .route("/auth/callback", get(auth_callback))
        .route("/characters", get(list_characters_handler))
        .route("/my/dynamics", get(dynamics_report_handler))
        .route("/profile/my/dynamics", get(profile_dynamics_report_handler))
        .with_state(AppState {
            context: context.clone(),
        })
        .layer(session_layer);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();
    })
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install ctrl-c handler");
}

#[derive(Deserialize, Debug, Clone)]
pub struct CallbackParams {
    code: String,
    state: String,
}
