//! TDX TCP 连接的生命周期管理。
//!
//! `tdxrs` 的客户端是「一条常驻 socket + 连接池」，`connect_to_any()` 在
//! `connected == true` 时直接返回；而它的自动重试是在**同一条 socket** 上再试 4 次
//! （每次等满 5s 读超时），失败也不会把 `connected` 清零，于是
//! `reconnect_if_needed()` 会短路 —— 跨天/休眠唤醒后表现为「每轮请求都失败，
//! 但永远不重连」。
//!
//! 恢复逻辑全部收口在这里：
//! - 每次请求前检查上次成功时间：CST 跨天或空闲超过 [`IDLE_RECONNECT`] 就换新客户端；
//! - 请求失败时换新客户端，200ms 后重试一次；
//! - 关闭 tdxrs 的内部自动重试，避免在死 socket 上累计 4 × 5s 读超时；
//! - 建连用独立锁串行化：`connect_internal()` 会 `close_all()`，与借出的连接
//!   并发时会打乱连接池计数。
//!
//! 换掉整个客户端（而不是 `disconnect()`）是刻意的：旧客户端可能还有并发请求
//! 持有它的 `Arc`，`disconnect()` 的 `close_all()` 会关掉空闲连接并把 active
//! 计数清零；等那个请求归还连接时计数就下溢了。换新客户端让旧连接自然结束。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use tdxrs::error::TdxError;
use tdxrs::error_codes::ErrorCode;
use tdxrs::net::client::TdxHqClient;

/// 建立连接/单次读写的超时。`connect_to_any()` 只对新连接生效。
const CONNECT_TIMEOUT_SECS: f64 = 5.0;

/// 超过这个空闲时长就认为 socket 可能已被对端回收，下次请求前换新连接。
///
/// 盘中每 2s、休市每 30s 都会请求，正常轮询不会触发；只有系统休眠、
/// 长时间切到别的数据源、或整夜没有请求时才会命中。
const IDLE_RECONNECT: Duration = Duration::from_secs(120);

/// 失败后换新连接重试前等待的时间（给对端/本机网络栈一点缓冲）。
const RETRY_DELAY: Duration = Duration::from_millis(200);

/// 一次操作最多尝试的次数（首次 + 一次重试）。
const MAX_ATTEMPTS: u32 = 2;

struct Slot {
    client: Arc<TdxHqClient>,
    last_success: Option<Instant>,
    /// 上次成功请求所在的 CST 日期。跨天后即使刚请求过也换一条连接。
    last_success_day: Option<NaiveDate>,
}

/// 共享的 TDX 连接状态。适配器持有 `Arc<TdxSession>`，克隆适配器共享同一份。
pub(crate) struct TdxSession {
    slot: Mutex<Slot>,
    /// 只保护「建立连接」这一步；具体请求仍可并行。
    connect_lock: Mutex<()>,
}

impl TdxSession {
    pub fn new() -> Self {
        Self {
            slot: Mutex::new(Slot {
                client: Arc::new(new_client()),
                last_success: None,
                last_success_day: None,
            }),
            connect_lock: Mutex::new(()),
        }
    }

    /// 在阻塞线程里执行一次 TDX 操作：建连 → 执行 → 失败换连接重试一次。
    ///
    /// 调用方负责放进 `spawn_blocking`（tdxrs 是阻塞 socket API）。
    pub fn run<T, F>(&self, op: F) -> Result<T, TdxError>
    where
        F: Fn(&TdxHqClient) -> Result<T, TdxError>,
    {
        for attempt in 0..MAX_ATTEMPTS {
            let client = self.acquire();

            let connect = {
                let _guard = self
                    .connect_lock
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                client.connect_to_any(Some(CONNECT_TIMEOUT_SECS))
            };
            if let Err(error) = connect {
                // `connect_to_any()` 自己已经遍历过全部候选服务器；
                // 在同一次调用里再扫一遍只会把离线时的等待翻倍。
                self.rotate(&client);
                return Err(error);
            }

            match op(&client) {
                Ok(value) => {
                    self.mark_success(&client);
                    return Ok(value);
                }
                // 只有传输层失败才值得换连接重试。服务端返回的业务错误
                // （无效代码、限流等）重连也不会变好，反而会让每个轮询周期
                // 都重建一次连接。
                Err(error) if attempt + 1 < MAX_ATTEMPTS && is_connection_error(&error) => {
                    log::warn!(
                        "TDX request failed (attempt {}): {} — discarding connection and retrying",
                        attempt + 1,
                        error.format_coded()
                    );
                    self.rotate(&client);
                    std::thread::sleep(RETRY_DELAY);
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("MAX_ATTEMPTS 至少为 1")
    }

    /// 取当前客户端；跨天或空闲过久时先换一条新连接。
    fn acquire(&self) -> Arc<TdxHqClient> {
        let (client, reset) = {
            let mut slot = self
                .slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let now = Instant::now();
            let today = cst_today();
            let mut reset = None;
            if let Some(last_success) = slot.last_success {
                let idle = now.saturating_duration_since(last_success);
                let day_changed = slot.last_success_day.map_or(false, |day| day != today);
                if idle >= IDLE_RECONNECT || day_changed {
                    reset = Some((idle, day_changed));
                    slot.client = Arc::new(new_client());
                    slot.last_success = None;
                    slot.last_success_day = None;
                }
            }
            (Arc::clone(&slot.client), reset)
        };
        // 日志后端可能做轮转文件 IO，放在锁外。
        if let Some((idle, day_changed)) = reset {
            log::info!(
                "TDX connection reset (idle {:?}, day changed: {})",
                idle,
                day_changed
            );
        }
        client
    }

    fn mark_success(&self, client: &Arc<TdxHqClient>) {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // 另一个线程可能已经换过客户端；旧连接的成功不应覆盖新连接的时钟。
        if Arc::ptr_eq(&slot.client, client) {
            slot.last_success = Some(Instant::now());
            slot.last_success_day = Some(cst_today());
        }
    }

    fn rotate(&self, expected: &Arc<TdxHqClient>) {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if Arc::ptr_eq(&slot.client, expected) {
            slot.client = Arc::new(new_client());
            slot.last_success = None;
            slot.last_success_day = None;
        }
    }
}

fn new_client() -> TdxHqClient {
    let client = TdxHqClient::new();
    // tdxrs 的内部重试不会重建连接（死 socket 下 `connected` 仍是 true），
    // 只会在同一 fd 上反复等 5s 超时。恢复交给上面的「换客户端 + 重试一次」。
    client.set_auto_retry(false);
    client
}

/// 传输层失败（socket 已死/读超时/连接池异常）才换连接重试。
fn is_connection_error(error: &TdxError) -> bool {
    if matches!(
        error,
        TdxError::Io(_)
            | TdxError::Connection(_)
            | TdxError::ConnectionTimeout
            | TdxError::Disconnected
            | TdxError::RetryExhausted(_)
    ) {
        return true;
    }
    matches!(
        error.error_code(),
        Some(code)
            if code == ErrorCode::CONNECTION_FAILED
                || code == ErrorCode::CONNECTION_TIMEOUT
                || code == ErrorCode::DISCONNECTED
                || code == ErrorCode::RETRY_EXHAUSTED
                || code == ErrorCode::POOL_EXHAUSTED
    )
}

fn cst_today() -> NaiveDate {
    cst_date_at(Utc::now())
}

fn cst_date_at(now: DateTime<Utc>) -> NaiveDate {
    now.with_timezone(&fixed_cst()).date_naive()
}

fn fixed_cst() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("UTC+8 is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yesterday() -> NaiveDate {
        cst_today().pred_opt().expect("CST date has a predecessor")
    }

    fn force_success_age(session: &TdxSession, age: Duration, day: NaiveDate) {
        let mut slot = session
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slot.last_success = Instant::now().checked_sub(age);
        slot.last_success_day = Some(day);
    }

    #[test]
    fn reuses_client_while_requests_are_recent() {
        let session = TdxSession::new();
        let first = session.acquire();
        session.mark_success(&first);

        assert!(Arc::ptr_eq(&first, &session.acquire()));
    }

    #[test]
    fn discards_client_after_long_idle_gap() {
        let session = TdxSession::new();
        let first = session.acquire();
        force_success_age(&session, IDLE_RECONNECT + Duration::from_secs(1), cst_today());

        assert!(!Arc::ptr_eq(&first, &session.acquire()));
    }

    #[test]
    fn discards_client_when_cst_day_changes() {
        let session = TdxSession::new();
        let first = session.acquire();
        session.mark_success(&first);
        // 刚成功过，但成功日期是昨天 —— 跨天后必须换连接。
        force_success_age(&session, Duration::from_secs(1), yesterday());

        assert!(!Arc::ptr_eq(&first, &session.acquire()));
    }

    #[test]
    fn stale_handle_cannot_overwrite_newer_connection_state() {
        let session = TdxSession::new();
        let old = session.acquire();
        session.rotate(&old);

        let current = session.acquire();
        session.mark_success(&old);
        session.rotate(&old);

        assert!(
            Arc::ptr_eq(&current, &session.acquire()),
            "旧句柄不应覆盖或再次轮换新连接"
        );
        let slot = session
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(slot.last_success.is_none(), "旧连接的成功必须被忽略");
    }

    #[test]
    fn only_transport_failures_trigger_reconnect() {
        assert!(is_connection_error(&TdxError::Disconnected));
        assert!(is_connection_error(&TdxError::Connection(
            "recv failed: broken pipe".to_string()
        )));
        assert!(is_connection_error(&TdxError::ConnectionTimeout));
        assert!(is_connection_error(&ErrorCode::DISCONNECTED.err("empty body")));

        // 业务/协议错误重连也不会变好，不该触发换连接。
        assert!(!is_connection_error(&TdxError::InvalidData(
            "bad code".to_string()
        )));
        assert!(!is_connection_error(&TdxError::Parse("bad".to_string())));
        assert!(!is_connection_error(
            &ErrorCode::RATE_LIMIT_EXCEEDED.err("slow down")
        ));
    }

    #[test]
    fn cst_day_boundary_is_measured_at_utc_plus_8() {
        let before = DateTime::parse_from_rfc3339("2026-11-24T15:59:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let after = DateTime::parse_from_rfc3339("2026-11-24T16:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        assert_eq!(
            cst_date_at(before),
            NaiveDate::from_ymd_opt(2026, 11, 24).expect("valid date")
        );
        assert_eq!(
            cst_date_at(after),
            NaiveDate::from_ymd_opt(2026, 11, 25).expect("valid date")
        );
    }
}
