//! Combined market data source.
//!
//! Mainland China symbols are fetched from the TongdaXin protocol through
//! `tdxrs`. Hong Kong and US symbols use Yahoo Finance's public chart/search
//! endpoints. Keeping both adapters behind one `DataSource` makes the source
//! selectable from the existing settings UI without requiring Python.

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use reqwest::Client;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;

use crate::datasource::{DataSource, INDEX_CODES, INDEX_POOL_NAMES};
use crate::domain::{AppError, Depth, IndexQuote, KLineData, Level, MinuteData, Quote, StockBrief};

const YAHOO_CHART_URL: &str = "https://query1.finance.yahoo.com/v8/finance/chart";
const YAHOO_SEARCH_URL: &str = "https://query1.finance.yahoo.com/v1/finance/search";
const MAX_YAHOO_SEARCH_RESULTS: usize = 20;
const TDX_QUOTE_BATCH_SIZE: usize = 60;
const TDX_KLINE_PAGE_SIZE: u16 = 800;
const MAX_TDX_KLINE_PAGES: usize = 20;
const YAHOO_QUOTE_BATCH_SIZE: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarketProvider {
    Tdx,
    Yahoo,
}

struct YahooChartData {
    quote: Quote,
    bars: Vec<KLineData>,
    minutes: Vec<MinuteData>,
}

/// A selectable source that routes CN/HK/US symbols to the appropriate backend.
#[derive(Clone)]
pub struct YahooTdxAdapter {
    yahoo: Client,
    tdx: Arc<tdxrs::net::client::TdxHqClient>,
}

impl YahooTdxAdapter {
    pub fn new() -> Self {
        // Keep reqwest's environment proxy support so HTTPS_PROXY/HTTP_PROXY
        // can be supplied when the desktop app is started from a shell.
        let yahoo = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent(concat!(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
                "AppleWebKit/537.36 (KHTML, like Gecko) ",
                "Chrome/131.0.0.0 Safari/537.36"
            ))
            .build()
            .expect("failed to build Yahoo Finance HTTP client");
        Self {
            yahoo,
            tdx: Arc::new(tdxrs::net::client::TdxHqClient::new()),
        }
    }
}

fn market_provider(market: &str) -> Option<MarketProvider> {
    match market.trim().to_ascii_uppercase().as_str() {
        "CN" => Some(MarketProvider::Tdx),
        "HK" | "US" => Some(MarketProvider::Yahoo),
        _ => None,
    }
}

fn tdx_symbol(code: &str) -> Option<(u8, String, String)> {
    let normalized = code.trim().strip_prefix("s_").unwrap_or(code.trim());
    let (market, digits) = if let Some(digits) = normalized.strip_prefix("sh") {
        (tdxrs::protocol::constants::MARKET_SH, digits)
    } else if let Some(digits) = normalized.strip_prefix("sz") {
        (tdxrs::protocol::constants::MARKET_SZ, digits)
    } else if let Some(digits) = normalized.strip_prefix("bj") {
        (tdxrs::protocol::constants::MARKET_BJ, digits)
    } else if normalized.len() == 6 && normalized.bytes().all(|b| b.is_ascii_digit()) {
        match normalized.as_bytes()[0] {
            b'5' | b'6' | b'9' => (tdxrs::protocol::constants::MARKET_SH, normalized),
            b'4' | b'8' => (tdxrs::protocol::constants::MARKET_BJ, normalized),
            _ => (tdxrs::protocol::constants::MARKET_SZ, normalized),
        }
    } else {
        return None;
    };
    if digits.len() != 6 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let prefix = match market {
        tdxrs::protocol::constants::MARKET_SH => "sh",
        tdxrs::protocol::constants::MARKET_SZ => "sz",
        tdxrs::protocol::constants::MARKET_BJ => "bj",
        _ => return None,
    };
    Some((market, digits.to_string(), format!("{}{}", prefix, digits)))
}

fn yahoo_symbol(code: &str, market: &str) -> Option<String> {
    let code = code.trim().to_ascii_uppercase();
    match market.trim().to_ascii_uppercase().as_str() {
        "HK" => {
            if code.ends_with(".HK") {
                Some(code)
            } else {
                let digits = code.strip_prefix("HK").unwrap_or(&code);
                if digits.chars().all(|c| c.is_ascii_digit()) {
                    Some(format!("{:0>4}.HK", digits))
                } else {
                    Some(format!("{}.HK", digits))
                }
            }
        }
        "US" => Some(code.strip_suffix(".US").unwrap_or(&code).to_string()),
        _ => None,
    }
}

fn local_timezone(meta: &Value, market: &str) -> Tz {
    let fallback = if market.eq_ignore_ascii_case("HK") {
        "Asia/Hong_Kong"
    } else {
        "America/New_York"
    };
    meta.get("exchangeTimezoneName")
        .and_then(Value::as_str)
        .and_then(|name| Tz::from_str(name).ok())
        .or_else(|| Tz::from_str(fallback).ok())
        .unwrap_or(chrono_tz::UTC)
}

fn number_at(row: &Value, field: &str, index: usize) -> Option<f64> {
    row.get(field)
        .and_then(Value::as_array)
        .and_then(|values| values.get(index))
        .and_then(Value::as_f64)
}

fn meta_number(meta: &Value, field: &str) -> Option<f64> {
    meta.get(field).and_then(Value::as_f64)
}

fn parse_yahoo_chart(
    response: &Value,
    symbol: &str,
    market: &str,
    intraday: bool,
) -> Result<YahooChartData, AppError> {
    let chart = response
        .get("chart")
        .ok_or_else(|| AppError::parse("yahoo", "响应中缺少 chart"))?;
    if !chart.get("error").unwrap_or(&Value::Null).is_null() {
        return Err(AppError::network("yahoo", chart["error"].to_string()));
    }
    let result = chart
        .get("result")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or_else(|| AppError::parse("yahoo", "没有可用行情结果"))?;
    let meta = result
        .get("meta")
        .ok_or_else(|| AppError::parse("yahoo", "响应中缺少 meta"))?;
    let quote_rows = result
        .pointer("/indicators/quote/0")
        .ok_or_else(|| AppError::parse("yahoo", "响应中缺少 quote 指标"))?;
    let timestamps = result
        .get("timestamp")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let timezone = local_timezone(meta, market);
    let mut bars = Vec::new();
    let mut minutes = Vec::new();

    for (index, timestamp) in timestamps.iter().enumerate() {
        let Some(seconds) = timestamp.as_i64() else {
            continue;
        };
        let Some(utc) = DateTime::<Utc>::from_timestamp(seconds, 0) else {
            continue;
        };
        let local = utc.with_timezone(&timezone);
        let Some(close) = number_at(quote_rows, "close", index) else {
            continue;
        };
        let open = number_at(quote_rows, "open", index).unwrap_or(close);
        let high = number_at(quote_rows, "high", index).unwrap_or(close);
        let low = number_at(quote_rows, "low", index).unwrap_or(close);
        let volume = number_at(quote_rows, "volume", index)
            .unwrap_or(0.0)
            .max(0.0) as u64;
        let timestamp_text = if intraday {
            local.format("%Y-%m-%d %H:%M").to_string()
        } else {
            local.format("%Y-%m-%d").to_string()
        };
        bars.push(KLineData {
            date: timestamp_text.clone(),
            open,
            high,
            low,
            close,
            volume,
            turnover: close * volume as f64,
        });
        minutes.push(MinuteData {
            time: local.format("%Y-%m-%d %H:%M").to_string(),
            price: close,
            open,
            high,
            low,
            volume,
            avg_price: close,
        });
    }

    let last_bar = bars.last();
    let price = meta_number(meta, "regularMarketPrice")
        .or_else(|| last_bar.map(|bar| bar.close))
        .unwrap_or(0.0);
    let previous_close = meta_number(meta, "chartPreviousClose")
        .or_else(|| meta_number(meta, "previousClose"))
        .unwrap_or(0.0);
    let volume = meta
        .get("regularMarketVolume")
        .and_then(Value::as_u64)
        .or_else(|| last_bar.map(|bar| bar.volume))
        .unwrap_or(0);
    let name = meta
        .get("longName")
        .or_else(|| meta.get("shortName"))
        .and_then(Value::as_str)
        .unwrap_or(symbol)
        .to_string();
    let timestamp = meta
        .get("regularMarketTime")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| Utc::now().timestamp());
    let change = price - previous_close;
    let change_pct = if previous_close > 0.0 {
        change / previous_close * 100.0
    } else {
        0.0
    };

    Ok(YahooChartData {
        quote: Quote {
            code: symbol.to_string(),
            market: market.to_ascii_uppercase(),
            name,
            price,
            change: (change * 1_000_000.0).round() / 1_000_000.0,
            change_pct: (change_pct * 1_000_000.0).round() / 1_000_000.0,
            open: meta_number(meta, "regularMarketOpen")
                .or_else(|| last_bar.map(|bar| bar.open))
                .unwrap_or(0.0),
            high: meta_number(meta, "regularMarketDayHigh")
                .or_else(|| last_bar.map(|bar| bar.high))
                .unwrap_or(0.0),
            low: meta_number(meta, "regularMarketDayLow")
                .or_else(|| last_bar.map(|bar| bar.low))
                .unwrap_or(0.0),
            volume,
            // Yahoo's chart response has no turnover field. This estimate is a
            // useful display value and preserves the project's unit (currency).
            turnover: price * volume as f64,
            turnover_rate: None,
            timestamp,
        },
        bars,
        minutes,
    })
}

fn index_name(code: &str) -> String {
    INDEX_POOL_NAMES
        .iter()
        .find(|(candidate, _)| *candidate == code)
        .map(|(_, name)| (*name).to_string())
        .unwrap_or_else(|| code.to_string())
}

fn quote_code(market: u8, code: &str) -> String {
    match market {
        tdxrs::protocol::constants::MARKET_SH => format!("sh{}", code),
        tdxrs::protocol::constants::MARKET_SZ => format!("sz{}", code),
        tdxrs::protocol::constants::MARKET_BJ => format!("bj{}", code),
        _ => code.to_string(),
    }
}

fn tdx_quote_to_domain(q: &tdxrs::protocol::types::SecurityQuote) -> Quote {
    let previous_close = q.last_close;
    let change = if q.price > 0.0 && previous_close > 0.0 {
        q.price - previous_close
    } else {
        0.0
    };
    Quote {
        code: quote_code(q.market, &q.code),
        market: "CN".to_string(),
        name: quote_code(q.market, &q.code),
        price: q.price,
        change,
        change_pct: if previous_close > 0.0 {
            change / previous_close * 100.0
        } else {
            0.0
        },
        open: q.open,
        high: q.high,
        low: q.low,
        // TDX real-time volume and depth quantities are lots.
        volume: (q.vol.max(0.0) as u64).saturating_mul(100),
        turnover: q.amount.max(0.0),
        turnover_rate: None,
        timestamp: Utc::now().timestamp(),
    }
}

fn tdx_depth(q: &tdxrs::protocol::types::SecurityQuote) -> Depth {
    let bids = [
        (q.bid1, q.bid_vol1),
        (q.bid2, q.bid_vol2),
        (q.bid3, q.bid_vol3),
        (q.bid4, q.bid_vol4),
        (q.bid5, q.bid_vol5),
    ]
    .into_iter()
    .filter(|(price, volume)| *price > 0.0 && *volume > 0.0)
    .map(|(price, volume)| Level {
        price,
        volume: (volume as u64).saturating_mul(100),
    })
    .collect();
    let asks = [
        (q.ask1, q.ask_vol1),
        (q.ask2, q.ask_vol2),
        (q.ask3, q.ask_vol3),
        (q.ask4, q.ask_vol4),
        (q.ask5, q.ask_vol5),
    ]
    .into_iter()
    .filter(|(price, volume)| *price > 0.0 && *volume > 0.0)
    .map(|(price, volume)| Level {
        price,
        volume: (volume as u64).saturating_mul(100),
    })
    .collect();
    Depth {
        code: quote_code(q.market, &q.code),
        bids,
        asks,
    }
}

fn tdx_category(period: &str) -> Option<u8> {
    use tdxrs::protocol::constants::*;
    match period {
        "1min" => Some(KLINE_1MIN),
        "5min" => Some(KLINE_5MIN),
        "15min" => Some(KLINE_15MIN),
        "30min" => Some(KLINE_30MIN),
        "60min" => Some(KLINE_1HOUR),
        "daily" => Some(KLINE_DAILY),
        "weekly" => Some(KLINE_WEEKLY),
        "monthly" => Some(KLINE_MONTHLY),
        _ => None,
    }
}

fn tdx_bar_to_domain(bar: &tdxrs::protocol::types::SecurityBar) -> KLineData {
    KLineData {
        date: bar.datetime.clone(),
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.vol.max(0.0) as u64,
        turnover: bar.amount.max(0.0),
    }
}

fn compact_tdx_date(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_digit())
        .collect()
}

fn normalize_tdx_end_date(value: &str) -> Option<String> {
    let compact = compact_tdx_date(value);
    if compact.len() < 8 {
        return None;
    }
    if compact.len() >= 12 {
        Some(compact[..12].to_string())
    } else {
        Some(format!("{}235959", &compact[..8]))
    }
}

fn bar_is_at_or_before(bar: &KLineData, end_date: &str) -> bool {
    compact_tdx_date(&bar.date).as_str() <= end_date
}

fn fixed_cst() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("UTC+8 is valid")
}

fn date_prefix_for_tdx() -> String {
    Utc::now()
        .with_timezone(&fixed_cst())
        .format("%Y-%m-%d")
        .to_string()
}

impl YahooTdxAdapter {
    async fn yahoo_chart(
        &self,
        symbol: &str,
        range: Option<&str>,
        interval: &str,
        period1: Option<i64>,
        period2: Option<i64>,
    ) -> Result<Value, AppError> {
        let mut request = self
            .yahoo
            .get(format!("{}/{}", YAHOO_CHART_URL, symbol))
            .query(&[
                ("interval", interval),
                ("includePrePost", "false"),
                ("events", "div,splits"),
            ]);
        if let Some(range) = range {
            request = request.query(&[("range", range)]);
        }
        if let Some(value) = period1 {
            request = request.query(&[("period1", value)]);
        }
        if let Some(value) = period2 {
            request = request.query(&[("period2", value)]);
        }
        let response = request
            .send()
            .await
            .map_err(|e| AppError::network("yahoo", format!("请求失败: {e}")))?;
        if !response.status().is_success() {
            return Err(AppError::network(
                "yahoo",
                format!("HTTP {}", response.status()),
            ));
        }
        response
            .json()
            .await
            .map_err(|e| AppError::parse("yahoo", format!("JSON 解析失败: {e}")))
    }

    async fn fetch_yahoo_realtime(&self, code: &str, market: &str) -> Result<Quote, AppError> {
        let symbol = yahoo_symbol(code, market)
            .ok_or_else(|| AppError::Unsupported(format!("不支持的市场: {market}")))?;
        let response = self
            .yahoo_chart(&symbol, Some("1d"), "1m", None, None)
            .await?;
        let mut quote = parse_yahoo_chart(&response, &symbol, market, true)?.quote;
        quote.code = code.to_string();
        Ok(quote)
    }

    async fn fetch_tdx_realtime(&self, codes: &[String]) -> Result<Vec<Quote>, AppError> {
        let symbols: Vec<(u8, String)> = codes
            .iter()
            .filter_map(|code| tdx_symbol(code).map(|(market, digits, _)| (market, digits)))
            .collect();
        if symbols.is_empty() {
            return Ok(vec![]);
        }
        let client = self.tdx.clone();
        let quotes = tokio::task::spawn_blocking(move || {
            client
                .connect_to_any(Some(5.0))
                .map_err(|e| format!("TDX 连接失败: {e}"))?;
            let mut quotes = Vec::with_capacity(symbols.len());
            for batch in symbols.chunks(TDX_QUOTE_BATCH_SIZE) {
                let requests: Vec<(u8, &str)> = batch
                    .iter()
                    .map(|(market, code)| (*market, code.as_str()))
                    .collect();
                let mut batch_quotes = client
                    .get_security_quotes(&requests)
                    .map_err(|e| format!("TDX 行情请求失败: {e}"))?;
                quotes.append(&mut batch_quotes);
            }
            Ok::<_, String>(quotes)
        })
        .await
        .map_err(|e| AppError::network("tdx", format!("阻塞任务失败: {e}")))?
        .map_err(|e| AppError::network("tdx", e))?;
        Ok(quotes.iter().map(tdx_quote_to_domain).collect())
    }

    async fn fetch_tdx_indices(&self) -> Result<Vec<IndexQuote>, AppError> {
        let symbols: Vec<(u8, String, String)> =
            INDEX_CODES.split(',').filter_map(tdx_symbol).collect();
        let client = self.tdx.clone();
        let quotes = tokio::task::spawn_blocking(move || {
            client
                .connect_to_any(Some(5.0))
                .map_err(|e| format!("TDX 连接失败: {e}"))?;
            let requests: Vec<(u8, &str)> = symbols
                .iter()
                .map(|(market, digits, _)| (*market, digits.as_str()))
                .collect();
            let quotes = client
                .get_security_quotes(&requests)
                .map_err(|e| format!("TDX 指数请求失败: {e}"))?;
            Ok::<_, String>((symbols, quotes))
        })
        .await
        .map_err(|e| AppError::network("tdx", format!("阻塞任务失败: {e}")))?
        .map_err(|e| AppError::network("tdx", e))?;

        let (symbols, quotes) = quotes;
        let mut indices = Vec::with_capacity(quotes.len());
        for q in quotes {
            let canonical = quote_code(q.market, &q.code);
            let code = format!("s_{}", canonical);
            let previous = q.last_close;
            let change = if q.price > 0.0 && previous > 0.0 {
                q.price - previous
            } else {
                0.0
            };
            indices.push(IndexQuote {
                code: code.clone(),
                name: index_name(&code),
                price: q.price,
                change,
                change_pct: if previous > 0.0 {
                    change / previous * 100.0
                } else {
                    0.0
                },
                volume: (q.vol.max(0.0) as u64).saturating_mul(100),
                turnover: q.amount.max(0.0),
            });
        }
        indices.sort_by_key(|index| {
            symbols
                .iter()
                .position(|(_, _, code)| format!("s_{}", code) == index.code)
                .unwrap_or(usize::MAX)
        });
        Ok(indices)
    }

    async fn fetch_tdx_minute(&self, code: &str) -> Result<Vec<MinuteData>, AppError> {
        let (market, digits, _) = tdx_symbol(code)
            .ok_or_else(|| AppError::Unsupported(format!("无效的 A 股代码: {code}")))?;
        let client = self.tdx.clone();
        let data = tokio::task::spawn_blocking(move || {
            client
                .connect_to_any(Some(5.0))
                .map_err(|e| format!("TDX 连接失败: {e}"))?;
            client
                .get_minute_time_data(market, &digits)
                .map_err(|e| format!("TDX 分时请求失败: {e}"))
        })
        .await
        .map_err(|e| AppError::network("tdx", format!("阻塞任务失败: {e}")))?
        .map_err(|e| AppError::network("tdx", e))?;

        let date = date_prefix_for_tdx();
        let mut result: Vec<MinuteData> = data
            .into_iter()
            .map(|point| MinuteData {
                time: format!("{} {}", date, point.time),
                price: point.price,
                open: point.price,
                high: point.price,
                low: point.price,
                volume: (point.vol.max(0.0) as u64).saturating_mul(100),
                avg_price: point.avg_price,
            })
            .collect();
        result.reverse();
        Ok(result)
    }

    async fn fetch_tdx_kline(
        &self,
        code: &str,
        period: &str,
        end_date: Option<&str>,
        count: Option<u32>,
    ) -> Result<Vec<KLineData>, AppError> {
        let (market, digits, _) = tdx_symbol(code)
            .ok_or_else(|| AppError::Unsupported(format!("无效的 A 股代码: {code}")))?;
        let category = tdx_category(period)
            .ok_or_else(|| AppError::Unsupported(format!("不支持的周期: {period}")))?;
        let requested = count.unwrap_or(200).clamp(1, 800) as u16;
        let end_date_owned = end_date.and_then(normalize_tdx_end_date);
        let client = self.tdx.clone();
        let bars = tokio::task::spawn_blocking(move || {
            client
                .connect_to_any(Some(5.0))
                .map_err(|e| format!("TDX 连接失败: {e}"))?;
            if end_date_owned.is_none() {
                return client
                    .get_security_bars(category, market, &digits, 0, requested, 1)
                    .map_err(|e| format!("TDX K 线请求失败: {e}"));
            }

            let end_date = end_date_owned.unwrap_or_default();
            let mut offset = 0_u32;
            let mut selected = Vec::new();
            for _ in 0..MAX_TDX_KLINE_PAGES {
                let page = client
                    .get_security_bars(category, market, &digits, offset, TDX_KLINE_PAGE_SIZE, 1)
                    .map_err(|e| format!("TDX K 线请求失败: {e}"))?;
                if page.is_empty() {
                    break;
                }

                let page_len = page.len();
                let reached_end = page
                    .first()
                    .map(|bar| compact_tdx_date(&bar.datetime).as_str() <= end_date.as_str())
                    .unwrap_or(false);
                let older_bars: Vec<_> = page
                    .into_iter()
                    .filter(|bar| compact_tdx_date(&bar.datetime).as_str() <= end_date.as_str())
                    .collect();
                selected.splice(0..0, older_bars);

                if (reached_end && selected.len() >= requested as usize)
                    || page_len < TDX_KLINE_PAGE_SIZE as usize
                {
                    break;
                }
                offset = offset.saturating_add(page_len as u32);
            }
            Ok(selected)
        })
        .await
        .map_err(|e| AppError::network("tdx", format!("阻塞任务失败: {e}")))?
        .map_err(|e| AppError::network("tdx", e))?;

        let mut result: Vec<KLineData> = bars.iter().map(tdx_bar_to_domain).collect();
        if let Some(end_date) = end_date.and_then(normalize_tdx_end_date) {
            result.retain(|bar| bar_is_at_or_before(bar, &end_date));
        }
        if result.len() > requested as usize {
            result = result[result.len() - requested as usize..].to_vec();
        }
        Ok(result)
    }

    async fn fetch_yahoo_kline(
        &self,
        code: &str,
        market: &str,
        period: &str,
        end_date: Option<&str>,
        count: Option<u32>,
    ) -> Result<Vec<KLineData>, AppError> {
        let symbol = yahoo_symbol(code, market)
            .ok_or_else(|| AppError::Unsupported(format!("不支持的市场: {market}")))?;
        let (interval, range, intraday, days_per_bar) = match period {
            "1min" => ("1m", Some("5d"), true, 0),
            "5min" => ("5m", Some("1mo"), true, 0),
            "15min" => ("15m", Some("1mo"), true, 0),
            "30min" => ("30m", Some("1mo"), true, 0),
            "60min" => ("60m", Some("3mo"), true, 0),
            "daily" => ("1d", None, false, 3),
            "weekly" => ("1wk", None, false, 14),
            "monthly" => ("1mo", None, false, 62),
            other => return Err(AppError::Unsupported(format!("Yahoo 不支持周期: {other}"))),
        };
        let amount = count.unwrap_or(200).clamp(1, 2000) as i64;
        let end = end_date
            .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
            .and_then(|date| date.and_hms_opt(23, 59, 59))
            .and_then(|date| {
                local_timezone(&Value::Null, market)
                    .from_local_datetime(&date)
                    .single()
            })
            .map(|date| date.timestamp());
        let period2 = end.unwrap_or_else(|| Utc::now().timestamp());
        let (period1, period2) = if intraday {
            (None, None)
        } else {
            let period1 = period2 - amount * days_per_bar * 86_400;
            (Some(period1), Some(period2))
        };
        let response = self
            .yahoo_chart(&symbol, range, interval, period1, period2)
            .await?;
        let mut bars = parse_yahoo_chart(&response, &symbol, market, intraday)?.bars;
        if let Some(end_date) = end_date {
            bars.retain(|bar| bar.date.get(..10).unwrap_or(&bar.date) <= end_date);
        }
        if bars.len() > amount as usize {
            bars = bars[bars.len() - amount as usize..].to_vec();
        }
        Ok(bars)
    }

    async fn yahoo_search(&self, keyword: &str, market: &str) -> Result<Vec<StockBrief>, AppError> {
        let response = self
            .yahoo
            .get(YAHOO_SEARCH_URL)
            .query(&[
                ("q", keyword.trim()),
                ("quotesCount", "20"),
                ("newsCount", "0"),
            ])
            .send()
            .await
            .map_err(|e| AppError::network("yahoo", format!("搜索请求失败: {e}")))?;
        if !response.status().is_success() {
            return Err(AppError::network(
                "yahoo",
                format!("搜索 HTTP {}", response.status()),
            ));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| AppError::parse("yahoo", format!("搜索 JSON 解析失败: {e}")))?;
        let wanted = market.to_ascii_uppercase();
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for item in body
            .get("quotes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if result.len() >= MAX_YAHOO_SEARCH_RESULTS {
                break;
            }
            let Some(symbol) = item.get("symbol").and_then(Value::as_str) else {
                continue;
            };
            let upper = symbol.to_ascii_uppercase();
            let item_market = if upper.ends_with(".HK") { "HK" } else { "US" };
            if wanted != "ALL" && wanted != item_market {
                continue;
            }
            let quote_type = item.get("quoteType").and_then(Value::as_str).unwrap_or("");
            if !matches!(quote_type, "EQUITY" | "ETF" | "INDEX") {
                continue;
            }
            if !seen.insert(upper.clone()) {
                continue;
            }
            let name = item
                .get("longname")
                .or_else(|| item.get("shortname"))
                .and_then(Value::as_str)
                .unwrap_or(symbol)
                .to_string();
            result.push(StockBrief {
                code: upper,
                market: item_market.to_string(),
                name,
                category: if quote_type == "ETF" {
                    "ETF".to_string()
                } else if quote_type == "INDEX" {
                    "ZS".to_string()
                } else {
                    "GP-A".to_string()
                },
            });
        }
        Ok(result)
    }
}

#[async_trait]
impl DataSource for YahooTdxAdapter {
    fn name(&self) -> &str {
        "yahoo_tdx"
    }

    fn display_name(&self) -> &str {
        "通达信 + Yahoo Finance"
    }

    fn supports_market(&self, market: &str) -> bool {
        market_provider(market).is_some()
    }

    async fn fetch_realtime(&self, codes: &[String], market: &str) -> Result<Vec<Quote>, AppError> {
        match market_provider(market) {
            Some(MarketProvider::Tdx) => self.fetch_tdx_realtime(codes).await,
            Some(MarketProvider::Yahoo) => {
                let mut quotes = Vec::with_capacity(codes.len());
                for batch in codes.chunks(YAHOO_QUOTE_BATCH_SIZE) {
                    let mut requests = tokio::task::JoinSet::new();
                    for code in batch {
                        let adapter = self.clone();
                        let code = code.clone();
                        let market = market.to_string();
                        requests.spawn(async move {
                            let result = adapter.fetch_yahoo_realtime(&code, &market).await;
                            (code, market, result)
                        });
                    }
                    while let Some(result) = requests.join_next().await {
                        match result {
                            Ok((_code, _market, Ok(quote))) => quotes.push(quote),
                            Ok((code, market, Err(error))) => {
                                log::warn!("Yahoo quote {}:{} failed: {}", market, code, error)
                            }
                            Err(error) => {
                                log::warn!("Yahoo quote task failed: {}", error)
                            }
                        }
                    }
                }
                Ok(quotes)
            }
            None => Err(AppError::Unsupported(format!("不支持的市场: {market}"))),
        }
    }

    async fn fetch_indices(&self) -> Result<Vec<IndexQuote>, AppError> {
        self.fetch_tdx_indices().await
    }

    async fn search(&self, keyword: &str, market: &str) -> Result<Vec<StockBrief>, AppError> {
        if market.eq_ignore_ascii_case("CN") {
            // TDX quote data does not include the security name; reuse the
            // existing CN exact-code lookup when the suggest APIs are down.
            return crate::datasource::tencent::TencentAdapter::new()
                .search(keyword, market)
                .await;
        }
        if market.eq_ignore_ascii_case("HK")
            || market.eq_ignore_ascii_case("US")
            || market.eq_ignore_ascii_case("ALL")
        {
            return self.yahoo_search(keyword, market).await;
        }
        Ok(vec![])
    }

    async fn fetch_depth(&self, code: &str, market: &str) -> Result<Depth, AppError> {
        if !market.eq_ignore_ascii_case("CN") {
            return Ok(Depth {
                code: code.to_string(),
                bids: vec![],
                asks: vec![],
            });
        }
        let (tdx_market, digits, _) = tdx_symbol(code)
            .ok_or_else(|| AppError::Unsupported(format!("无效的 A 股代码: {code}")))?;
        let client = self.tdx.clone();
        let depth = tokio::task::spawn_blocking(move || {
            client
                .connect_to_any(Some(5.0))
                .map_err(|e| format!("TDX 连接失败: {e}"))?;
            let refs = [(tdx_market, digits.as_str())];
            let quotes = client
                .get_security_quotes(&refs)
                .map_err(|e| format!("TDX 深度请求失败: {e}"))?;
            quotes
                .first()
                .map(tdx_depth)
                .ok_or_else(|| "TDX 未返回盘口数据".to_string())
        })
        .await
        .map_err(|e| AppError::network("tdx", format!("阻塞任务失败: {e}")))?
        .map_err(|e| AppError::network("tdx", e))?;
        Ok(depth)
    }

    async fn fetch_minute_data(
        &self,
        code: &str,
        market: &str,
    ) -> Result<Vec<MinuteData>, AppError> {
        match market_provider(market) {
            Some(MarketProvider::Tdx) => self.fetch_tdx_minute(code).await,
            Some(MarketProvider::Yahoo) => {
                let symbol = yahoo_symbol(code, market)
                    .ok_or_else(|| AppError::Unsupported(format!("不支持的市场: {market}")))?;
                let response = self
                    .yahoo_chart(&symbol, Some("5d"), "1m", None, None)
                    .await?;
                Ok(parse_yahoo_chart(&response, &symbol, market, true)?.minutes)
            }
            None => Err(AppError::Unsupported(format!("不支持的市场: {market}"))),
        }
    }

    async fn fetch_kline(
        &self,
        code: &str,
        market: &str,
        period: &str,
        end_date: Option<&str>,
        count: Option<u32>,
    ) -> Result<Vec<KLineData>, AppError> {
        match market_provider(market) {
            Some(MarketProvider::Tdx) => self.fetch_tdx_kline(code, period, end_date, count).await,
            Some(MarketProvider::Yahoo) => {
                self.fetch_yahoo_kline(code, market, period, end_date, count)
                    .await
            }
            None => Err(AppError::Unsupported(format!("不支持的市场: {market}"))),
        }
    }

    async fn health_check(&self) -> Result<bool, AppError> {
        let client = self.tdx.clone();
        tokio::task::spawn_blocking(move || {
            client
                .connect_to_any(Some(5.0))
                .map_err(|e| AppError::network("tdx", format!("健康检查失败: {e}")))
        })
        .await
        .map_err(|e| AppError::network("tdx", format!("阻塞任务失败: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bar_is_at_or_before, market_provider, normalize_tdx_end_date, parse_yahoo_chart,
        tdx_symbol, yahoo_symbol, MarketProvider,
    };
    use serde_json::json;
    use tdxrs::protocol::constants::{MARKET_SH, MARKET_SZ};

    #[test]
    fn routes_cn_to_tdx_and_hk_us_to_yahoo() {
        assert_eq!(market_provider("CN"), Some(MarketProvider::Tdx));
        assert_eq!(market_provider("HK"), Some(MarketProvider::Yahoo));
        assert_eq!(market_provider("US"), Some(MarketProvider::Yahoo));
        assert_eq!(market_provider("OTHER"), None);
    }

    #[test]
    fn normalizes_symbols_for_both_protocols() {
        assert_eq!(tdx_symbol("sh600519").unwrap().0, MARKET_SH);
        assert_eq!(tdx_symbol("000001").unwrap().0, MARKET_SZ);
        assert_eq!(tdx_symbol("s_sh000001").unwrap().2, "sh000001");
        assert_eq!(yahoo_symbol("700", "HK"), Some("0700.HK".into()));
        assert_eq!(yahoo_symbol("AAPL", "US"), Some("AAPL".into()));
    }

    #[test]
    fn tdx_end_date_supports_daily_and_minute_bars() {
        assert_eq!(
            normalize_tdx_end_date("2026-11-12"),
            Some("20261112235959".to_string())
        );
        assert_eq!(
            normalize_tdx_end_date("202611120935"),
            Some("202611120935".to_string())
        );
        assert!(bar_is_at_or_before(
            &crate::domain::KLineData {
                date: "2026-11-12 09:30".to_string(),
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 0,
                turnover: 0.0,
            },
            "202611120935"
        ));
    }

    #[test]
    fn parses_yahoo_chart_into_project_quote_and_bars() {
        let response = json!({
            "chart": { "result": [{
                "meta": {
                    "symbol": "AAPL",
                    "currency": "USD",
                    "longName": "Apple Inc.",
                    "exchangeTimezoneName": "America/New_York",
                    "chartPreviousClose": 172.0,
                    "regularMarketPrice": 176.32,
                    "regularMarketVolume": 1000,
                    "regularMarketTime": 1700000000
                },
                "timestamp": [1700000000],
                "indicators": { "quote": [{
                    "open": [175.0],
                    "high": [177.0],
                    "low": [174.0],
                    "close": [176.32],
                    "volume": [1000]
                }] }
            }], "error": null }
        });
        let parsed = parse_yahoo_chart(&response, "AAPL", "US", false).unwrap();
        assert_eq!(parsed.quote.name, "Apple Inc.");
        assert_eq!(parsed.quote.market, "US");
        assert_eq!(parsed.quote.price, 176.32);
        assert_eq!(parsed.quote.change, 4.32);
        assert_eq!(parsed.quote.volume, 1000);
        assert_eq!(parsed.bars[0].date, "2023-11-14");
    }
}
