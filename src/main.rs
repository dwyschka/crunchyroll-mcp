use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use anyhow::Result;
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};
use uuid::Uuid;

use crunchyroll_rs::{
    Crunchyroll, Locale, MediaCollection, MovieListing, Season, Series,
    auth::{CrunchyrollBuilder, DeviceIdentifier, middleware::MiddlewareContext},
    list::WatchlistOptions,
    search::{BrowseMediaType, BrowseOptions, BrowseSortType},
};

// ── Session persistence ──────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct PersistedSession {
    refresh_token: String,
    device_id: String,
    device_type: String,
}

fn session_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".crunchyroll-mcp")
        .join("session.json")
}

fn load_session() -> Option<PersistedSession> {
    let content = std::fs::read_to_string(session_path()).ok()?;
    serde_json::from_str(&content).ok()
}

fn save_session(session: &PersistedSession) {
    let path = session_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(content) = serde_json::to_string_pretty(session) {
        let _ = std::fs::write(path, content);
    }
}

fn clear_session() {
    let _ = std::fs::remove_file(session_path());
}

// ── Language settings ────────────────────────────────────────────────────────

/// Language for titles, descriptions and category names.
static LOCALE: LazyLock<Locale> =
    LazyLock::new(|| env_locale("CRUNCHYROLL_LOCALE", Locale::en_US));

/// Preferred audio language when a series or episode has several versions.
static AUDIO_LOCALE: LazyLock<Locale> =
    LazyLock::new(|| env_locale("CRUNCHYROLL_AUDIO_LOCALE", Locale::ja_JP));

/// Reads a locale like `de-DE` (or `de_DE`) from the environment.
fn env_locale(key: &str, default: Locale) -> Locale {
    let Some(value) = std::env::var(key).ok().filter(|v| !v.is_empty()) else {
        return default;
    };
    let locale = Locale::from(value.replace('_', "-"));
    if Locale::all().contains(&locale) {
        locale
    } else {
        eprintln!("Unknown locale in {key}: '{value}', falling back to {default}");
        default
    }
}

/// When `DEV_MODE=true`, every raw response from the Crunchyroll API is logged.
static DEV_MODE: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("DEV_MODE").is_ok_and(|v| v.eq_ignore_ascii_case("true") || v == "1")
});

// ── Dev logging ──────────────────────────────────────────────────────────────

/// Request body for the dev log; form fields are decoded and the password is masked.
fn request_params(request: &reqwest::Request) -> Option<String> {
    let body = request.body()?.as_bytes()?;
    let is_form = request
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/x-www-form-urlencoded"));
    if !is_form {
        return Some(String::from_utf8_lossy(body).into_owned());
    }

    let fields = form_urlencoded::parse(body)
        .map(|(key, value)| {
            if key == "password" {
                format!("  {key} = <{} Zeichen>", value.chars().count())
            } else {
                format!("  {key} = {value}")
            }
        })
        .collect::<Vec<_>>();
    Some(fields.join("\n"))
}

/// Middleware that logs each request and response body to stderr and passes it on unchanged.
struct ResponseLogger;

impl<'a> tower_service::Service<MiddlewareContext<'a>> for ResponseLogger {
    type Response = reqwest::Response;
    type Error = reqwest::Error;
    type Future = Pin<Box<dyn Future<Output = Result<reqwest::Response, reqwest::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, ctx: MiddlewareContext<'a>) -> Self::Future {
        let client = ctx.client.clone();
        let request = ctx.request;
        Box::pin(async move {
            use reqwest::ResponseBuilderExt;

            let method = request.method().clone();
            let url = request.url().clone();
            match request_params(&request) {
                Some(params) => eprintln!("[DEV] {method} {url}\n{params}"),
                None => eprintln!("[DEV] {method} {url}"),
            }
            let resp = client.execute(request).await?;
            let status = resp.status();
            let version = resp.version();
            let headers = resp.headers().clone();
            let body = resp.bytes().await?;
            eprintln!(
                "[DEV] {method} {url} -> {status}\n{}",
                String::from_utf8_lossy(&body)
            );

            let mut builder = http::Response::builder()
                .status(status)
                .version(version)
                .url(url);
            if let Some(h) = builder.headers_mut() {
                *h = headers;
            }
            Ok(builder
                .body(body)
                .expect("response parts taken from a valid response")
                .into())
        })
    }
}

/// Crunchyroll client builder with the configured locales (and the dev logger if enabled).
fn crunchyroll_builder() -> CrunchyrollBuilder {
    let builder = Crunchyroll::builder()
        .locale(LOCALE.clone())
        .preferred_audio_locale(AUDIO_LOCALE.clone());
    if *DEV_MODE {
        builder.middleware(ResponseLogger)
    } else {
        builder
    }
}

// ── Shared state ─────────────────────────────────────────────────────────────

struct AppState {
    crunchyroll: Option<Crunchyroll>,
    device_id: String,
    device_type: String,
}

type SharedState = Arc<Mutex<AppState>>;

fn make_device(device_id: &str, device_type: &str) -> DeviceIdentifier {
    DeviceIdentifier {
        device_id: device_id.to_string(),
        device_type: device_type.to_string(),
        device_name: None,
    }
}

// ── Login helpers ─────────────────────────────────────────────────────────────

async fn login_with_credentials(
    email: &str,
    password: &str,
    device_id: &str,
    device_type: &str,
) -> Result<Crunchyroll> {
    let cr = crunchyroll_builder()
        .login_with_credentials(email, password, make_device(device_id, device_type))
        .await?;
    Ok(cr)
}

async fn login_with_refresh(
    refresh_token: &str,
    device_id: &str,
    device_type: &str,
) -> Result<Crunchyroll> {
    let cr = crunchyroll_builder()
        .login_with_refresh_token(refresh_token, make_device(device_id, device_type))
        .await?;
    Ok(cr)
}

async fn extract_refresh_token(cr: &Crunchyroll) -> String {
    use crunchyroll_rs::auth::SessionToken;
    match cr.session_token().await {
        SessionToken::RefreshToken(token) => token,
        _ => String::new(),
    }
}

/// Saves the current refresh token if Crunchyroll rotated it during a token refresh.
async fn persist_refresh_token(state: &SharedState) {
    let (cr, device_id, device_type) = {
        let s = state.lock().await;
        let Some(cr) = s.crunchyroll.clone() else {
            return;
        };
        (cr, s.device_id.clone(), s.device_type.clone())
    };

    let refresh_token = extract_refresh_token(&cr).await;
    if refresh_token.is_empty()
        || load_session().is_some_and(|saved| saved.refresh_token == refresh_token)
    {
        return;
    }
    save_session(&PersistedSession {
        refresh_token,
        device_id,
        device_type,
    });
}

// ── Auto re-login ─────────────────────────────────────────────────────────────

fn env_credentials() -> Option<(String, String)> {
    let email = std::env::var("CRUNCHYROLL_EMAIL").ok().filter(|v| !v.is_empty())?;
    let password = std::env::var("CRUNCHYROLL_PASSWORD").ok().filter(|v| !v.is_empty())?;
    Some((email, password))
}

/// Logs in with the credentials from the environment and stores the new session.
async fn login_from_env(state: &SharedState) -> Result<Crunchyroll, String> {
    let (email, password) =
        env_credentials().ok_or("Not logged in. Use crunchyroll_login first.")?;
    let (device_id, device_type) = {
        let s = state.lock().await;
        (s.device_id.clone(), s.device_type.clone())
    };

    eprintln!("Logging in with credentials from environment...");
    let cr = login_with_credentials(&email, &password, &device_id, &device_type)
        .await
        .map_err(|e| format!("Not logged in. Auto-login from environment failed: {e}"))?;

    let refresh_token = extract_refresh_token(&cr).await;
    save_session(&PersistedSession {
        refresh_token,
        device_id,
        device_type,
    });
    eprintln!("Auto-login successful");

    state.lock().await.crunchyroll = Some(cr.clone());
    Ok(cr)
}

/// Returns the active session, logging in from the environment if there is none.
async fn current_session(state: &SharedState) -> Result<Crunchyroll, Value> {
    if let Some(cr) = state.lock().await.crunchyroll.clone() {
        return Ok(cr);
    }
    login_from_env(state).await.map_err(tool_err)
}

/// Whether a tool result failed because the session is expired or was revoked.
fn is_auth_error(result: &Value) -> bool {
    if result.get("isError").and_then(Value::as_bool) != Some(true) {
        return false;
    }
    let text = result
        .pointer("/content/0/text")
        .and_then(Value::as_str)
        .unwrap_or("");
    text.contains("(http 401)")
        || text.contains("authentication error")
        || text.contains("/auth/v1/token")
}

// ── MCP helpers ───────────────────────────────────────────────────────────────

fn tool_ok(text: impl Into<String>) -> Value {
    json!({
        "content": [{"type": "text", "text": text.into()}],
        "isError": false
    })
}

fn tool_err(text: impl Into<String>) -> Value {
    json!({
        "content": [{"type": "text", "text": text.into()}],
        "isError": true
    })
}

fn tool_json(value: impl Serialize) -> Value {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|e| e.to_string());
    tool_ok(text)
}

fn get_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key)?.as_str()
}

fn get_u64(args: &Value, key: &str) -> Option<u64> {
    args.get(key)?.as_u64()
}

// ── Tool definitions ──────────────────────────────────────────────────────────

fn tools_list() -> Value {
    json!([
        {
            "name": "crunchyroll_login",
            "description": "Login to Crunchyroll with email and password. Saves the session for future use.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "email": {"type": "string", "description": "Crunchyroll account email"},
                    "password": {"type": "string", "description": "Crunchyroll account password"}
                },
                "required": ["email", "password"]
            }
        },
        {
            "name": "crunchyroll_logout",
            "description": "Logout from Crunchyroll and clear the saved session.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "crunchyroll_status",
            "description": "Check current Crunchyroll login status.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "crunchyroll_search",
            "description": "Search the Crunchyroll catalog for anime, movies, and episodes.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query string"},
                    "limit": {"type": "number", "description": "Maximum number of results (default: 20)"}
                },
                "required": ["query"]
            }
        },
        {
            "name": "crunchyroll_browse",
            "description": "Browse the Crunchyroll catalog with optional filters.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sort": {
                        "type": "string",
                        "description": "Sort order: popularity, newly_added, alphabetical",
                        "enum": ["popularity", "newly_added", "alphabetical"]
                    },
                    "media_type": {
                        "type": "string",
                        "description": "Filter by type: series, movie",
                        "enum": ["series", "movie"]
                    },
                    "is_dubbed": {"type": "boolean", "description": "Filter for dubbed content"},
                    "is_subbed": {"type": "boolean", "description": "Filter for subbed content"},
                    "categories": {
                        "type": "string",
                        "description": "Comma-separated list of categories (e.g. 'action,comedy')"
                    },
                    "simulcast_season": {
                        "type": "string",
                        "description": "Simulcast season ID (from crunchyroll_get_simulcast_seasons)"
                    },
                    "limit": {"type": "number", "description": "Maximum number of results (default: 20)"}
                }
            }
        },
        {
            "name": "crunchyroll_get_categories",
            "description": "Get all available Crunchyroll anime categories/genres.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "crunchyroll_get_simulcast_seasons",
            "description": "Get all available simulcast seasons.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "crunchyroll_get_series",
            "description": "Get detailed information about an anime series by its ID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "series_id": {"type": "string", "description": "Crunchyroll series ID"}
                },
                "required": ["series_id"]
            }
        },
        {
            "name": "crunchyroll_get_seasons",
            "description": "Get all seasons for an anime series. Each season is returned in the preferred audio language; `versions` lists the season ID of every other audio language (dub).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "series_id": {"type": "string", "description": "Crunchyroll series ID"}
                },
                "required": ["series_id"]
            }
        },
        {
            "name": "crunchyroll_get_episodes",
            "description": "Get all episodes for a season. Use a season ID from `versions` to get the episodes of a specific dub.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "season_id": {"type": "string", "description": "Crunchyroll season ID"}
                },
                "required": ["season_id"]
            }
        },
        {
            "name": "crunchyroll_get_episode",
            "description": "Get detailed information about a specific episode, including the episode ID of every audio language in `versions`.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "episode_id": {"type": "string", "description": "Crunchyroll episode ID"}
                },
                "required": ["episode_id"]
            }
        },
        {
            "name": "crunchyroll_get_watchlist",
            "description": "Get the current user's watchlist.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "crunchyroll_add_to_watchlist",
            "description": "Add a series or movie to the watchlist.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content_id": {"type": "string", "description": "Series or movie listing ID to add"}
                },
                "required": ["content_id"]
            }
        },
        {
            "name": "crunchyroll_remove_from_watchlist",
            "description": "Remove a series or movie from the watchlist.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content_id": {"type": "string", "description": "Series or movie listing ID to remove"}
                },
                "required": ["content_id"]
            }
        },
        {
            "name": "crunchyroll_get_history",
            "description": "Get the watch history for the current user.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {"type": "number", "description": "Maximum number of entries (default: 20)"}
                }
            }
        },
        {
            "name": "crunchyroll_get_subscription",
            "description": "Get billing history and subscription information.",
            "inputSchema": {"type": "object", "properties": {}}
        }
    ])
}

// ── Tool implementations ──────────────────────────────────────────────────────

async fn tool_login(args: &Value, state: &SharedState) -> Value {
    let email = match get_str(args, "email") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: email"),
    };
    let password = match get_str(args, "password") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: password"),
    };

    let (device_id, device_type) = {
        let s = state.lock().await;
        (s.device_id.clone(), s.device_type.clone())
    };

    match login_with_credentials(&email, &password, &device_id, &device_type).await {
        Ok(cr) => {
            let refresh_token = extract_refresh_token(&cr).await;
            let is_premium = cr.premium().await;

            save_session(&PersistedSession {
                refresh_token,
                device_id,
                device_type,
            });

            let mut s = state.lock().await;
            s.crunchyroll = Some(cr);

            tool_ok(format!(
                "Logged in successfully as {email}. Premium: {is_premium}"
            ))
        }
        Err(e) => tool_err(format!("Login failed: {e}")),
    }
}

async fn tool_logout(state: &SharedState) -> Value {
    clear_session();
    let mut s = state.lock().await;
    s.crunchyroll = None;
    tool_ok("Logged out successfully")
}

async fn tool_status(state: &SharedState) -> Value {
    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    let is_premium = cr.premium().await;
    match cr.account().await {
        Ok(account) => tool_ok(format!(
            "Logged in as {} ({}). Premium: {}",
            account.email, account.account_id, is_premium
        )),
        Err(e) => tool_ok(format!("Logged in. Premium: {is_premium}. (Could not fetch account: {e})")),
    }
}

async fn tool_search(args: &Value, state: &SharedState) -> Value {
    let query = match get_str(args, "query") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: query"),
    };
    let limit = get_u64(args, "limit").unwrap_or(20) as usize;

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    let results_stream = cr.query(&query).top_results;
    match results_stream.take(limit).try_collect::<Vec<_>>().await {
        Ok(results) => {
            let simplified: Vec<Value> = results
                .iter()
                .map(|item| simplify_search_result(item))
                .collect();
            tool_json(json!({
                "query": query,
                "results": simplified,
                "count": simplified.len()
            }))
        }
        Err(e) => tool_err(format!("Search failed: {e}")),
    }
}

async fn tool_browse(args: &Value, state: &SharedState) -> Value {
    let limit = get_u64(args, "limit").unwrap_or(20) as usize;

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    let mut options = BrowseOptions::default();

    if let Some(sort) = get_str(args, "sort") {
        let sort_type = BrowseSortType::from(sort.to_string());
        options = options.sort(sort_type);
    }
    if let Some(media_type) = get_str(args, "media_type") {
        let mt = match media_type {
            "series" => BrowseMediaType::Series,
            "movie" => BrowseMediaType::Movie,
            other => BrowseMediaType::from(other.to_string()),
        };
        options = options.media_type(mt);
    }
    if let Some(v) = args.get("is_dubbed").and_then(|v| v.as_bool()) {
        options = options.is_dubbed(v);
    }
    if let Some(v) = args.get("is_subbed").and_then(|v| v.as_bool()) {
        options = options.is_subbed(v);
    }
    if let Some(season) = get_str(args, "simulcast_season") {
        options = options.simulcast_season(season.to_string());
    }
    if let Some(cats_str) = get_str(args, "categories") {
        use crunchyroll_rs::categories::Category;
        let cats: Vec<Category> = cats_str
            .split(',')
            .map(|c| Category::from(c.trim().to_lowercase()))
            .collect();
        options = options.categories(cats);
    }

    match cr
        .browse(options)
        .take(limit)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(results) => {
            let simplified: Vec<Value> = results
                .iter()
                .map(|item| simplify_search_result(item))
                .collect();
            tool_json(json!({
                "results": simplified,
                "count": simplified.len()
            }))
        }
        Err(e) => tool_err(format!("Browse failed: {e}")),
    }
}

async fn tool_get_categories(state: &SharedState) -> Value {
    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr.categories().await {
        Ok(categories) => {
            let simplified: Vec<Value> = categories
                .iter()
                .map(|c| {
                    json!({
                        "id": c.category.to_string(),
                        "slug": c.slug,
                        "title": c.localization.title,
                        "description": c.localization.description
                    })
                })
                .collect();
            tool_json(json!({"categories": simplified}))
        }
        Err(e) => tool_err(format!("Failed to get categories: {e}")),
    }
}

async fn tool_get_simulcast_seasons(state: &SharedState) -> Value {
    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr.simulcast_seasons(LOCALE.clone()).await {
        Ok(seasons) => {
            let simplified: Vec<Value> = seasons
                .iter()
                .map(|s| {
                    json!({
                        "id": s.id,
                        "title": s.localization.title,
                        "description": s.localization.description
                    })
                })
                .collect();
            tool_json(json!({"simulcast_seasons": simplified}))
        }
        Err(e) => tool_err(format!("Failed to get simulcast seasons: {e}")),
    }
}

async fn tool_get_series(args: &Value, state: &SharedState) -> Value {
    let series_id = match get_str(args, "series_id") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: series_id"),
    };

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr.media_from_id::<Series>(&series_id).await {
        Ok(series) => tool_json(json!({
            "id": series.id,
            "title": series.title,
            "description": series.description,
            "slug_title": series.slug_title,
            "episode_count": series.episode_count,
            "season_count": series.season_count,
            "is_subbed": series.is_subbed,
            "is_dubbed": series.is_dubbed,
            "is_simulcast": series.is_simulcast,
            "audio_locales": series.audio_locales.iter().map(|l| l.to_string()).collect::<Vec<_>>(),
            "maturity_ratings": series.maturity_ratings,
            "series_launch_year": series.series_launch_year,
            "categories": series.categories.as_ref().map(|cats| {
                cats.iter().map(|c| c.to_string()).collect::<Vec<_>>()
            })
        })),
        Err(e) => tool_err(format!("Failed to get series: {e}")),
    }
}

async fn tool_get_seasons(args: &Value, state: &SharedState) -> Value {
    let series_id = match get_str(args, "series_id") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: series_id"),
    };

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr.media_from_id::<Series>(&series_id).await {
        Ok(series) => match series.seasons().await {
            Ok(seasons) => {
                let simplified: Vec<Value> = seasons
                    .iter()
                    .map(|s| {
                        json!({
                            "id": s.id,
                            "title": s.title,
                            "season_number": s.season_number,
                            "number_of_episodes": s.number_of_episodes,
                            "is_subbed": s.is_subbed,
                            "is_dubbed": s.is_dubbed,
                            "is_simulcast": s.is_simulcast,
                            "audio_locales": s.audio_locales.iter().map(|l| l.to_string()).collect::<Vec<_>>(),
                            "versions": season_versions(s)
                        })
                    })
                    .collect();
                tool_json(json!({"series_id": series_id, "seasons": simplified}))
            }
            Err(e) => tool_err(format!("Failed to get seasons: {e}")),
        },
        Err(e) => tool_err(format!("Failed to get series: {e}")),
    }
}

async fn tool_get_episodes(args: &Value, state: &SharedState) -> Value {
    let season_id = match get_str(args, "season_id") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: season_id"),
    };

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr.media_from_id::<Season>(&season_id).await {
        Ok(season) => match season.episodes().await {
            Ok(episodes) => {
                let simplified: Vec<Value> = episodes
                    .iter()
                    .map(|ep| {
                        json!({
                            "id": ep.id,
                            "title": ep.title,
                            "description": ep.description,
                            "episode_number": ep.episode_number,
                            "sequence_number": ep.sequence_number,
                            "season_number": ep.season_number,
                            "series_id": ep.series_id,
                            "series_title": ep.series_title,
                            "season_id": ep.season_id,
                            "audio_locale": ep.audio_locale.to_string(),
                            "duration_seconds": ep.duration.num_seconds(),
                            "episode_air_date": ep.episode_air_date.to_rfc3339(),
                            "is_premium_only": ep.is_premium_only,
                            "versions": episode_versions(ep)
                        })
                    })
                    .collect();
                tool_json(json!({"season_id": season_id, "episodes": simplified}))
            }
            Err(e) => tool_err(format!("Failed to get episodes: {e}")),
        },
        Err(e) => tool_err(format!("Failed to get season: {e}")),
    }
}

async fn tool_get_episode(args: &Value, state: &SharedState) -> Value {
    let episode_id = match get_str(args, "episode_id") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: episode_id"),
    };

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    use crunchyroll_rs::Episode;
    match cr.media_from_id::<Episode>(&episode_id).await {
        Ok(ep) => tool_json(json!({
            "id": ep.id,
            "title": ep.title,
            "description": ep.description,
            "episode_number": ep.episode_number,
            "sequence_number": ep.sequence_number,
            "season_number": ep.season_number,
            "series_id": ep.series_id,
            "series_title": ep.series_title,
            "season_id": ep.season_id,
            "season_title": ep.season_title,
            "audio_locale": ep.audio_locale.to_string(),
            "subtitle_locales": ep.subtitle_locales.iter().map(|l| l.to_string()).collect::<Vec<_>>(),
            "duration_seconds": ep.duration.num_seconds(),
            "episode_air_date": ep.episode_air_date.to_rfc3339(),
            "is_premium_only": ep.is_premium_only,
            "versions": episode_versions(&ep)
        })),
        Err(e) => tool_err(format!("Failed to get episode: {e}")),
    }
}

async fn tool_get_watchlist(state: &SharedState) -> Value {
    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr.watchlist(WatchlistOptions::default()).await {
        Ok(entries) => {
            let simplified: Vec<Value> = entries
                .iter()
                .map(|e| {
                    let (id, title, media_type) = match &e.panel {
                        MediaCollection::Series(s) => {
                            (s.id.clone(), s.title.clone(), "series")
                        }
                        MediaCollection::MovieListing(m) => {
                            (m.id.clone(), m.title.clone(), "movie")
                        }
                        _ => ("".to_string(), "".to_string(), "unknown"),
                    };
                    json!({
                        "id": id,
                        "title": title,
                        "type": media_type,
                        "is_favorite": e.is_favorite,
                        "never_watched": e.never_watched,
                        "fully_watched": e.fully_watched,
                        "playhead": e.playhead
                    })
                })
                .collect();
            tool_json(json!({"watchlist": simplified, "count": simplified.len()}))
        }
        Err(e) => tool_err(format!("Failed to get watchlist: {e}")),
    }
}

async fn tool_add_to_watchlist(args: &Value, state: &SharedState) -> Value {
    let content_id = match get_str(args, "content_id") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: content_id"),
    };

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    // Try as Series first, then MovieListing
    match cr.media_from_id::<Series>(&content_id).await {
        Ok(series) => match series.add_to_watchlist().await {
            Ok(()) => return tool_ok(format!("Added '{}' to watchlist", series.title)),
            Err(e) => return tool_err(format!("Failed to add to watchlist: {e}")),
        },
        Err(_) => {}
    }

    match cr.media_from_id::<MovieListing>(&content_id).await {
        Ok(movie) => match movie.add_to_watchlist().await {
            Ok(()) => tool_ok(format!("Added '{}' to watchlist", movie.title)),
            Err(e) => tool_err(format!("Failed to add to watchlist: {e}")),
        },
        Err(e) => tool_err(format!("Content not found (tried series and movie): {e}")),
    }
}

async fn tool_remove_from_watchlist(args: &Value, state: &SharedState) -> Value {
    let content_id = match get_str(args, "content_id") {
        Some(v) => v.to_string(),
        None => return tool_err("Missing required parameter: content_id"),
    };

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    // Try as Series first
    if let Ok(series) = cr.media_from_id::<Series>(&content_id).await {
        match series.into_watchlist_entry().await {
            Ok(Some(entry)) => match entry.remove().await {
                Ok(()) => return tool_ok("Removed from watchlist"),
                Err(e) => return tool_err(format!("Failed to remove: {e}")),
            },
            Ok(None) => return tool_err("Content is not in watchlist"),
            Err(e) => return tool_err(format!("Failed to check watchlist: {e}")),
        }
    }

    // Try as MovieListing
    if let Ok(movie) = cr.media_from_id::<MovieListing>(&content_id).await {
        match movie.into_watchlist_entry().await {
            Ok(Some(entry)) => match entry.remove().await {
                Ok(()) => return tool_ok("Removed from watchlist"),
                Err(e) => return tool_err(format!("Failed to remove: {e}")),
            },
            Ok(None) => return tool_err("Content is not in watchlist"),
            Err(e) => return tool_err(format!("Failed to check watchlist: {e}")),
        }
    }

    tool_err("Content not found. Provide a valid series or movie ID.")
}

async fn tool_get_history(args: &Value, state: &SharedState) -> Value {
    let limit = get_u64(args, "limit").unwrap_or(20) as usize;

    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr
        .watch_history()
        .take(limit)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(entries) => {
            let simplified: Vec<Value> = entries
                .iter()
                .map(|e| {
                    json!({
                        "id": e.id,
                        "parent_id": e.parent_id,
                        "parent_type": e.parent_type,
                        "date_played": e.date_played.to_rfc3339(),
                        "playhead": e.playhead,
                        "fully_watched": e.fully_watched
                    })
                })
                .collect();
            tool_json(json!({"history": simplified, "count": simplified.len()}))
        }
        Err(e) => tool_err(format!("Failed to get watch history: {e}")),
    }
}

async fn tool_get_subscription(state: &SharedState) -> Value {
    let cr = match current_session(state).await {
        Ok(cr) => cr,
        Err(e) => return e,
    };

    match cr.billing_history().await {
        Ok(billing) => {
            let invoices: Vec<Value> = billing
                .invoices
                .iter()
                .map(|inv| {
                    json!({
                        "invoice_id": inv.invoice_id,
                        "date": inv.invoice_date.to_rfc3339(),
                        "amount": inv.amount,
                        "currency": inv.currency_code,
                        "status": inv.status,
                        "plan": inv.description,
                        "payment_method": inv.payment_info.payment_method_name
                    })
                })
                .collect();
            let is_premium = cr.premium().await;
            tool_json(json!({
                "is_premium": is_premium,
                "billing_history": invoices
            }))
        }
        Err(e) => tool_err(format!("Failed to get subscription info: {e}")),
    }
}

// ── Language versions ─────────────────────────────────────────────────────────

/// The same season in every audio language, each with its own season ID.
fn season_versions(season: &Season) -> Vec<Value> {
    season
        .versions
        .iter()
        .map(|v| {
            json!({
                "id": v.id,
                "audio_locale": v.audio_locale.to_string(),
                "original": v.original
            })
        })
        .collect()
}

/// The same episode in every audio language, each with its own episode ID.
fn episode_versions(episode: &crunchyroll_rs::Episode) -> Vec<Value> {
    episode
        .versions
        .iter()
        .map(|v| {
            json!({
                "id": v.id,
                "season_id": v.season_id,
                "audio_locale": v.audio_locale.to_string(),
                "original": v.original,
                "is_premium_only": v.is_premium_only
            })
        })
        .collect()
}

// ── Search result simplifier ──────────────────────────────────────────────────

fn simplify_search_result(item: &crunchyroll_rs::search::SearchMediaCollection) -> Value {
    use crunchyroll_rs::search::SearchMediaCollection;
    match item {
        SearchMediaCollection::Series(s) => json!({
            "type": "series",
            "id": s.id,
            "title": s.title,
            "description": s.description,
            "episode_count": s.episode_count,
            "season_count": s.season_count,
            "is_dubbed": s.is_dubbed,
            "is_subbed": s.is_subbed,
            "audio_locales": s.audio_locales.iter().map(|l| l.to_string()).collect::<Vec<_>>()
        }),
        SearchMediaCollection::Episode(e) => json!({
            "type": "episode",
            "id": e.id,
            "title": e.title,
            "description": e.description,
            "series_title": e.series_title,
            "season_number": e.season_number,
            "episode_number": e.episode_number
        }),
        SearchMediaCollection::MovieListing(m) => json!({
            "type": "movie",
            "id": m.id,
            "title": m.title,
            "description": m.description
        }),
        SearchMediaCollection::MusicVideo(v) => json!({
            "type": "music_video",
            "id": v.id,
            "title": v.title
        }),
        SearchMediaCollection::Concert(c) => json!({
            "type": "concert",
            "id": c.id,
            "title": c.title
        }),
    }
}

// ── MCP protocol dispatch ─────────────────────────────────────────────────────

async fn dispatch_tool(name: &str, args: &Value, state: &SharedState) -> Value {
    let had_session = state.lock().await.crunchyroll.is_some();
    let result = run_tool(name, args, state).await;

    // Session expired or was revoked: log in again from the environment and retry once.
    let is_session_tool = matches!(name, "crunchyroll_login" | "crunchyroll_logout");
    if had_session && !is_session_tool && is_auth_error(&result) && env_credentials().is_some() {
        eprintln!("Session no longer valid, re-authenticating...");
        state.lock().await.crunchyroll = None;
        if let Err(e) = login_from_env(state).await {
            return tool_err(e);
        }
        let result = run_tool(name, args, state).await;
        persist_refresh_token(state).await;
        return result;
    }

    persist_refresh_token(state).await;
    result
}

async fn run_tool(name: &str, args: &Value, state: &SharedState) -> Value {
    match name {
        "crunchyroll_login" => tool_login(args, state).await,
        "crunchyroll_logout" => tool_logout(state).await,
        "crunchyroll_status" => tool_status(state).await,
        "crunchyroll_search" => tool_search(args, state).await,
        "crunchyroll_browse" => tool_browse(args, state).await,
        "crunchyroll_get_categories" => tool_get_categories(state).await,
        "crunchyroll_get_simulcast_seasons" => tool_get_simulcast_seasons(state).await,
        "crunchyroll_get_series" => tool_get_series(args, state).await,
        "crunchyroll_get_seasons" => tool_get_seasons(args, state).await,
        "crunchyroll_get_episodes" => tool_get_episodes(args, state).await,
        "crunchyroll_get_episode" => tool_get_episode(args, state).await,
        "crunchyroll_get_watchlist" => tool_get_watchlist(state).await,
        "crunchyroll_add_to_watchlist" => tool_add_to_watchlist(args, state).await,
        "crunchyroll_remove_from_watchlist" => tool_remove_from_watchlist(args, state).await,
        "crunchyroll_get_history" => tool_get_history(args, state).await,
        "crunchyroll_get_subscription" => tool_get_subscription(state).await,
        unknown => tool_err(format!("Unknown tool: {unknown}")),
    }
}

async fn handle_request(msg: &Value, state: &SharedState) -> Option<Value> {
    let id = msg.get("id").cloned();
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");

    // Notifications have no id → no response
    if id.is_none() {
        return None;
    }
    let id = id.unwrap();

    let result = match method {
        "initialize" => json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "crunchyroll-mcp", "version": "0.1.0"}
        }),

        "tools/list" => json!({"tools": tools_list()}),

        "tools/call" => {
            let params = msg.get("params").cloned().unwrap_or(json!({}));
            let tool_name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            dispatch_tool(tool_name, &args, state).await
        }

        "ping" => json!({}),

        unknown => {
            return Some(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": format!("Method not found: {unknown}")
                }
            }));
        }
    };

    Some(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    }))
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let device_id = Uuid::new_v4().to_string();
    let device_type = "ANDROIDTV".to_string();

    let mut state_inner = AppState {
        crunchyroll: None,
        device_id: device_id.clone(),
        device_type: device_type.clone(),
    };

    // Without env credentials, fall back to the saved session
    if env_credentials().is_none() {
        if let Some(session) = load_session() {
            eprintln!("Attempting login from saved session...");
            match login_with_refresh(&session.refresh_token, &session.device_id, &session.device_type)
                .await
            {
                Ok(cr) => {
                    // Update device_id/type to match session
                    state_inner.device_id = session.device_id.clone();
                    state_inner.device_type = session.device_type.clone();

                    let new_refresh = extract_refresh_token(&cr).await;
                    save_session(&PersistedSession {
                        refresh_token: new_refresh,
                        device_id: session.device_id,
                        device_type: session.device_type,
                    });
                    eprintln!("Session login successful");
                    state_inner.crunchyroll = Some(cr);
                }
                Err(e) => eprintln!("Session login failed: {e}"),
            }
        }
    }

    let state: SharedState = Arc::new(Mutex::new(state_inner));

    // Env credentials take precedence (useful for Docker deployments). If this fails,
    // the next tool call retries the login.
    if env_credentials().is_some() {
        if let Err(e) = login_from_env(&state).await {
            eprintln!("{e}");
        }
    }

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut writer = tokio::io::BufWriter::new(stdout);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading stdin: {e}");
                break;
            }
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Invalid JSON: {e}");
                let error_response = json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": -32700, "message": "Parse error"}
                });
                let mut output = serde_json::to_string(&error_response).unwrap();
                output.push('\n');
                let _ = writer.write_all(output.as_bytes()).await;
                let _ = writer.flush().await;
                continue;
            }
        };

        if let Some(response) = handle_request(&msg, &state).await {
            let mut output = serde_json::to_string(&response).unwrap();
            output.push('\n');
            if let Err(e) = writer.write_all(output.as_bytes()).await {
                eprintln!("Error writing response: {e}");
                break;
            }
            if let Err(e) = writer.flush().await {
                eprintln!("Error flushing output: {e}");
                break;
            }
        }
    }

    Ok(())
}
