import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import patch
from urllib.parse import quote

import requests

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

# =========================================================
# Polymarket signal bot v2 — Gamma API + Telegram
# Docs: https://docs.polymarket.com/developers/gamma-markets-api/get-events
# Telegram: https://core.telegram.org/bots/api#sendmessage
# Does not place trades; scans public Gamma data only.
# =========================================================

VERSION = "0.2.4"
# MSK = UTC+3 year-round (no DST); avoids ZoneInfo/tzdata on minimal Windows installs
MSK_TZ = timezone(timedelta(hours=3), name="MSK")

# ----- env-backed settings (see .env.example) -----


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(key: str, default: int) -> int:
    try:
        val = os.getenv(key, str(default))
        if val is None or str(val).strip() == "":
            return default
        return int(str(val).strip())
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        val = os.getenv(key, str(default))
        if val is None or str(val).strip() == "":
            return default
        return float(str(val).strip())
    except ValueError:
        return default


def _proxy_dict_for_prefix(prefix: str) -> Optional[Dict[str, str]]:
    """
    optional HTTP(S) or SOCKS proxy for requests.
    either PREFIX_PROXY_URL (one line) or PREFIX_PROXY_HOST + PORT + USER + PASSWORD.
    passwords with special chars: use split vars or percent-encode in URL.
    socks5:// needs PySocks (see requirements).
    """
    url = (os.getenv(f"{prefix}_PROXY_URL") or "").strip()
    if url:
        return {"http": url, "https": url}
    host = (os.getenv(f"{prefix}_PROXY_HOST") or "").strip()
    if not host:
        return None
    port = _env_int(f"{prefix}_PROXY_PORT", 0)
    if port <= 0:
        return None
    user = (os.getenv(f"{prefix}_PROXY_USER") or "").strip()
    password = os.getenv(f"{prefix}_PROXY_PASSWORD") or ""
    if user:
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    else:
        auth = ""
    full = f"http://{auth}{host}:{port}"
    return {"http": full, "https": full}


def build_requests_session(proxy_dict: Optional[Dict[str, str]] = None) -> requests.Session:
    s = requests.Session()
    if proxy_dict:
        s.proxies.update(proxy_dict)
    return s


def telegram_token() -> str:
    return (os.getenv("TELEGRAM_TOKEN") or "").strip()


def chat_id() -> str:
    return (os.getenv("CHAT_ID") or "").strip()


REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 20)
POLL_INTERVAL_SECONDS = _env_int("POLL_INTERVAL_SECONDS", 60)
MAX_LOOPS = _env_int("MAX_LOOPS", 0)
MODE = (os.getenv("MODE") or "bot").strip().lower()

# Gamma GET /events — same host as docs: https://gamma-api.polymarket.com
GAMMA_EVENTS_URL = (os.getenv("GAMMA_EVENTS_URL") or "https://gamma-api.polymarket.com/events").strip()
# live API expects e.g. volume24hr; volume_24hr returns 422 "order fields are not valid"
GAMMA_EVENTS_ORDER = (os.getenv("GAMMA_EVENTS_ORDER") or "volume24hr").strip()
MAX_EVENTS_PER_REQUEST = _env_int("MAX_EVENTS_PER_REQUEST", 100)
ONLY_ACTIVE = _env_bool("ONLY_ACTIVE", True)
ONLY_OPEN = _env_bool("ONLY_OPEN", True)

MIN_EVENT_VOLUME_24H = _env_float("MIN_EVENT_VOLUME_24H", 1000.0)
MIN_EVENT_LIQUIDITY = _env_float("MIN_EVENT_LIQUIDITY", 5000.0)

# signal thresholds (tunable in code; avoids huge .env)
OVERPRICED_THRESHOLD = 0.80
MID_LOW = 0.40
MID_HIGH = 0.60
LOW_QUALITY_PRICE_FLOOR = 0.05
TARGET_MOVE = 0.15
STOP_MOVE = 0.08

# optional keyword focus. empty INCLUDE_KEYWORDS = scan any title that passes exclude
INCLUDE_KEYWORDS = [
    "fed",
    "rate",
    "inflation",
    "cpi",
    "recession",
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "crypto",
    "solana",
    "election",
    "president",
    "senate",
    "house",
    "etf",
    "sec",
    "approval",
    "token",
    "fdv",
    "launch",
]
EXCLUDE_KEYWORDS = [
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "soccer",
    "tennis",
    "match",
    "game",
]


class _MSKFormatter(logging.Formatter):
    """log timestamps in Europe/Moscow (per project convention)."""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=MSK_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + " MSK"


_logger: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger
    log = logging.getLogger("polymarket_bot_v2")
    log.setLevel(getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").strip().upper(), logging.INFO))
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_MSKFormatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(handler)
    log.propagate = False
    _logger = log
    return log


def log_startup_banner() -> None:
    log = get_logger()
    log.info("polymarket signal bot v2 starting | version=%s | mode=%s", VERSION, MODE)


def has_telegram_credentials() -> bool:
    return bool(telegram_token() and chat_id())


def get_runtime_mode() -> str:
    if _env_bool("DRY_RUN", False):
        return "dry-run"
    if not has_telegram_credentials():
        return "dry-run"
    return "live"


def print_startup_help() -> None:
    log = get_logger()
    mode = get_runtime_mode()
    log.info("runtime=%s", mode)
    if mode == "dry-run":
        log.info("Telegram not configured or DRY_RUN=1 — signals go to console only.")
        if not has_telegram_credentials():
            log.info("set TELEGRAM_TOKEN and CHAT_ID in .env (see .env.example) for live Telegram.")


def normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def contains_any_keyword(text: str, keywords: List[str]) -> bool:
    lowered = normalize_text(text)
    return any(keyword in lowered for keyword in keywords)


def is_interesting_title(title: str) -> bool:
    lowered = normalize_text(title)
    if not lowered:
        return False
    if EXCLUDE_KEYWORDS and contains_any_keyword(lowered, EXCLUDE_KEYWORDS):
        return False
    if not INCLUDE_KEYWORDS:
        return True
    return contains_any_keyword(lowered, INCLUDE_KEYWORDS)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fetch_active_events(session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """
    Gamma API: GET /events with active, closed, order, ascending, limit.
    Docs mention volume_24hr; actual API validates order field — use volume24hr (see GAMMA_EVENTS_ORDER).
    """
    http = session or requests
    params = {
        "active": str(ONLY_ACTIVE).lower(),
        "closed": str(not ONLY_OPEN).lower(),
        "order": GAMMA_EVENTS_ORDER,
        "ascending": "false",
        "limit": MAX_EVENTS_PER_REQUEST,
    }
    try:
        response = http.get(GAMMA_EVENTS_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        get_logger().warning("unexpected Gamma events response type: %s", type(data).__name__)
        return []
    except requests.RequestException as exc:
        get_logger().error("Gamma /events request failed: %s", exc)
        return []
    except Exception as exc:
        get_logger().error("unexpected error fetching events: %s", exc)
        return []


def extract_markets_from_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """nested markets; skip inactive/closed rows to match tradable Gamma state."""
    markets = event.get("markets", [])
    if not isinstance(markets, list):
        return []
    out: List[Dict[str, Any]] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        if market.get("closed") is True:
            continue
        if market.get("active") is False:
            continue
        out.append(market)
    return out


def _parse_json_list(raw: Any) -> List[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def extract_outcomes(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Gamma returns outcomes + outcomePrices as JSON strings, e.g.
    outcomes: "[\"Yes\", \"No\"]", outcomePrices: "[\"0.12\", \"0.88\"]"
    or legacy list[dict] with name/price — both supported.
    """
    raw = market.get("outcomes")
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return [item for item in raw if isinstance(item, dict)]

    names_any = _parse_json_list(raw)
    names = [str(x) for x in names_any]

    prices_raw = market.get("outcomePrices")
    prices_any = _parse_json_list(prices_raw)

    if names and prices_any and len(names) == len(prices_any):
        return [{"name": names[i], "price": prices_any[i]} for i in range(len(names))]
    return []


def passes_event_filters(event: Dict[str, Any]) -> bool:
    title = str(event.get("title") or event.get("slug") or "")
    volume_24hr = safe_float(event.get("volume24hr", event.get("volume_24hr", 0.0)))
    liquidity = safe_float(event.get("liquidity", 0.0))

    if volume_24hr < MIN_EVENT_VOLUME_24H:
        return False
    if liquidity < MIN_EVENT_LIQUIDITY:
        return False
    if not is_interesting_title(title):
        return False
    return True


def classify_signal(price: float, title: str) -> Optional[Tuple[str, str, float, float, int]]:
    lowered = normalize_text(title)

    if price < LOW_QUALITY_PRICE_FLOOR or price > 0.95:
        return None

    is_hype = any(keyword in lowered for keyword in ["fdv", "launch", "token", "airdrop", "approval", "etf"])
    is_macro = any(keyword in lowered for keyword in ["fed", "rate", "inflation", "cpi", "recession"])
    is_politics = any(keyword in lowered for keyword in ["election", "president", "senate", "house"])

    if price > OVERPRICED_THRESHOLD:
        score = 6
        if is_hype:
            score += 2
        if is_macro:
            score -= 1
        action = "NO / FADE HYPE"
        exit_price = max(price - TARGET_MOVE, 0.01)
        stop_price = min(price + STOP_MOVE, 0.99)
        return "SHORT", action, round(exit_price, 2), round(stop_price, 2), score

    if MID_LOW < price < MID_HIGH:
        score = 5
        if is_macro or is_politics:
            score += 2
        action = "WATCH / VALUE ZONE"
        exit_price = min(price + TARGET_MOVE, 0.99)
        stop_price = max(price - STOP_MOVE, 0.01)
        return "MID", action, round(exit_price, 2), round(stop_price, 2), score

    return None


def analyze_market(event: Dict[str, Any], market: Dict[str, Any]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []

    event_title = str(event.get("title") or event.get("slug") or "Untitled event").strip()
    market_title = str(market.get("question") or market.get("title") or event_title).strip()
    combined_title = market_title if market_title else event_title

    outcomes = extract_outcomes(market)
    if not outcomes:
        return []

    for outcome in outcomes:
        outcome_name = str(outcome.get("name") or outcome.get("outcome") or "").strip()
        price = safe_float(outcome.get("price", 0))

        if price <= 0 or price >= 1:
            continue

        classified = classify_signal(price, combined_title)
        if not classified:
            continue

        signal_type, action, exit_price, stop_price, score = classified

        reason_bits = []
        if price > OVERPRICED_THRESHOLD:
            reason_bits.append("price > 0.80")
        if MID_LOW < price < MID_HIGH:
            reason_bits.append("price in 0.40-0.60 range")
        if any(k in normalize_text(combined_title) for k in ["fdv", "launch", "token", "airdrop", "approval", "etf"]):
            reason_bits.append("hype-sensitive title")
        if any(k in normalize_text(combined_title) for k in ["fed", "rate", "inflation", "cpi", "recession"]):
            reason_bits.append("macro-sensitive title")
        if any(k in normalize_text(combined_title) for k in ["election", "president", "senate", "house"]):
            reason_bits.append("politics-sensitive title")

        signals.append(
            {
                "event_title": event_title,
                "market_title": combined_title,
                "outcome_name": outcome_name or "Unknown",
                "type": signal_type,
                "action": action,
                "entry": round(price, 2),
                "exit": exit_price,
                "stop": stop_price,
                "score": score,
                "reason": ", ".join(reason_bits) if reason_bits else "pattern match",
                "event_volume_24hr": round(safe_float(event.get("volume24hr", event.get("volume_24hr", 0.0))), 2),
                "event_liquidity": round(safe_float(event.get("liquidity", 0.0)), 2),
                "event_slug": str(event.get("slug") or "").strip(),
            }
        )

    return signals


def format_signal(signal: Dict[str, Any]) -> str:
    event_link = ""
    if signal.get("event_slug"):
        event_link = f"\nLink: https://polymarket.com/event/{signal['event_slug']}"

    return (
        "📢 SIGNAL V2\n\n"
        f"Event: {signal['event_title']}\n"
        f"Market: {signal['market_title']}\n"
        f"Outcome: {signal['outcome_name']}\n"
        f"Signal: {signal['type']}\n"
        f"Action: {signal['action']}\n"
        f"Score: {signal['score']}/10\n\n"
        f"Entry: {signal['entry']}\n"
        f"Exit: {signal['exit']}\n"
        f"Stop: {signal['stop']}\n\n"
        f"Reason: {signal['reason']}\n"
        f"24h Volume: {signal['event_volume_24hr']}\n"
        f"Liquidity: {signal['event_liquidity']}"
        f"{event_link}"
    )


def send_telegram_message(text: str, session: Optional[requests.Session] = None) -> bool:
    if get_runtime_mode() == "dry-run":
        print("\n--- DRY RUN MESSAGE ---")
        print(text)
        print("--- END MESSAGE ---\n")
        return True

    http = session or requests
    # Telegram Bot API sendMessage
    url = f"https://api.telegram.org/bot{telegram_token()}/sendMessage"
    payload = {
        "chat_id": chat_id(),
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        response = http.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok", False):
            # avoid logging full payload if it ever includes chat text; ok flag false is enough
            get_logger().warning("Telegram sendMessage not ok (description may be in API response)")
            return False

        return True
    except requests.RequestException as exc:
        get_logger().error("Telegram HTTP error: %s", exc)
        return False
    except Exception as exc:
        get_logger().error("Telegram unexpected error: %s", exc)
        return False


def send_signal(signal: Dict[str, Any], session: Optional[requests.Session] = None) -> bool:
    return send_telegram_message(format_signal(signal), session=session)


def send_test_message() -> None:
    log = get_logger()
    message = "🚀 Polymarket Signal Bot V2 connected and running"
    log.info("sending Telegram test message")
    tg_session = build_requests_session(_proxy_dict_for_prefix("TELEGRAM"))
    success = send_telegram_message(message, session=tg_session)
    log.info("test message sent." if success else "test message failed.")


def build_signal_key(signal: Dict[str, Any]) -> str:
    return f"{signal['event_title']}|{signal['market_title']}|{signal['outcome_name']}|{signal['type']}|{signal['entry']}"


def scan_once(session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    events = fetch_active_events(session=session)
    signals: List[Dict[str, Any]] = []

    if not events:
        get_logger().warning("no events returned from Gamma /events")
        return []

    for event in events:
        if not passes_event_filters(event):
            continue

        markets = extract_markets_from_event(event)
        for market in markets:
            signals.extend(analyze_market(event, market))

    signals.sort(key=lambda item: (-item["score"], -item["event_volume_24hr"], item["entry"]))
    return signals


def run_bot(poll_interval_seconds: int = POLL_INTERVAL_SECONDS) -> None:
    print_startup_help()
    seen: Set[str] = set()
    loop_count = 0
    log = get_logger()

    tg_proxy = _proxy_dict_for_prefix("TELEGRAM")
    gamma_proxy = _proxy_dict_for_prefix("GAMMA")
    if tg_proxy:
        log.info("telegram requests use proxy (url not logged)")
    if gamma_proxy:
        log.info("gamma requests use proxy (url not logged)")

    telegram_session = build_requests_session(tg_proxy)
    gamma_session = build_requests_session(gamma_proxy)

    while True:
        loop_count += 1
        log.info("scan cycle %s | fetching Gamma events", loop_count)
        signals = scan_once(session=gamma_session)

        if not signals:
            log.info("no qualifying signals this cycle")

        for signal in signals:
            key = build_signal_key(signal)
            if key in seen:
                continue
            was_sent = send_signal(signal, session=telegram_session)
            if was_sent:
                seen.add(key)

        if MAX_LOOPS > 0 and loop_count >= MAX_LOOPS:
            log.info("stopping after %s loop(s) (MAX_LOOPS=%s)", loop_count, MAX_LOOPS)
            break

        time.sleep(poll_interval_seconds)


# =========================
# tests
# =========================


def test_has_telegram_credentials_false_when_env_empty() -> None:
    with patch.dict(os.environ, {"TELEGRAM_TOKEN": "", "CHAT_ID": ""}, clear=False):
        assert has_telegram_credentials() is False


def test_extract_markets_from_event() -> None:
    event = {"markets": [{"question": "Q1", "active": True, "closed": False}, {"question": "Q2", "active": True, "closed": False}]}
    markets = extract_markets_from_event(event)
    assert len(markets) == 2
    assert markets[0]["question"] == "Q1"


def test_extract_markets_skips_closed() -> None:
    event = {"markets": [{"question": "Open", "active": True, "closed": False}, {"question": "Closed", "active": True, "closed": True}]}
    markets = extract_markets_from_event(event)
    assert len(markets) == 1
    assert markets[0]["question"] == "Open"


def test_extract_outcomes_gamma_json_strings() -> None:
    market = {
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '["0.82", "0.18"]',
    }
    out = extract_outcomes(market)
    assert len(out) == 2
    assert out[0]["name"] == "Yes"
    assert safe_float(out[0]["price"]) == 0.82


def test_analyze_market_short_signal() -> None:
    event = {
        "title": "Token launch FDV test",
        "slug": "token-launch-fdv-test",
        "volume24hr": 50000,
        "liquidity": 100000,
    }
    market = {
        "question": "Will token FDV exceed $300M?",
        "active": True,
        "closed": False,
        "outcomes": [{"name": "Yes", "price": "0.82"}],
    }
    signals = analyze_market(event, market)
    assert len(signals) == 1
    assert signals[0]["type"] == "SHORT"
    assert signals[0]["entry"] == 0.82
    assert signals[0]["exit"] == 0.67
    assert signals[0]["score"] >= 6


def test_analyze_market_mid_signal() -> None:
    event = {
        "title": "Fed decision test",
        "slug": "fed-decision-test",
        "volume24hr": 60000,
        "liquidity": 120000,
    }
    market = {
        "question": "Will the Fed cut rates in June?",
        "active": True,
        "closed": False,
        "outcomes": [{"name": "Yes", "price": "0.50"}],
    }
    signals = analyze_market(event, market)
    assert len(signals) == 1
    assert signals[0]["type"] == "MID"
    assert signals[0]["entry"] == 0.5
    assert signals[0]["exit"] == 0.65
    assert signals[0]["score"] >= 7


def test_analyze_market_bad_prices() -> None:
    event = {"title": "Bad market test", "slug": "bad-market-test", "volume24hr": 10000, "liquidity": 10000}
    market = {
        "question": "Bad market",
        "active": True,
        "closed": False,
        "outcomes": [
            {"price": "abc"},
            {"price": 0},
            {"price": 1},
            {"price": -0.2},
        ],
    }
    signals = analyze_market(event, market)
    assert signals == []


def test_passes_event_filters_true() -> None:
    event = {
        "title": "Bitcoin ETF approval odds",
        "volume24hr": 5000,
        "liquidity": 10000,
    }
    assert passes_event_filters(event) is True


def test_passes_event_filters_false_for_sports() -> None:
    event = {
        "title": "NBA Finals game 1",
        "volume24hr": 5000,
        "liquidity": 10000,
    }
    assert passes_event_filters(event) is False


def test_proxy_dict_url_takes_precedence() -> None:
    with patch.dict(
        os.environ,
        {
            "TELEGRAM_PROXY_URL": "http://user:secret@198.51.100.1:8080",
            "TELEGRAM_PROXY_HOST": "ignored",
            "TELEGRAM_PROXY_PORT": "1",
        },
        clear=False,
    ):
        p = _proxy_dict_for_prefix("TELEGRAM")
        assert p == {"http": "http://user:secret@198.51.100.1:8080", "https": "http://user:secret@198.51.100.1:8080"}


def test_proxy_dict_from_host_port_user_password() -> None:
    with patch.dict(
        os.environ,
        {
            "TELEGRAM_PROXY_URL": "",
            "TELEGRAM_PROXY_HOST": "198.51.100.2",
            "TELEGRAM_PROXY_PORT": "8888",
            "TELEGRAM_PROXY_USER": "u",
            "TELEGRAM_PROXY_PASSWORD": "p@x",
        },
        clear=False,
    ):
        p = _proxy_dict_for_prefix("TELEGRAM")
        assert p is not None
        assert "198.51.100.2:8888" in p["https"]
        assert "u:" in p["https"]


def test_format_signal() -> None:
    signal = {
        "event_title": "Sample event",
        "market_title": "Sample market",
        "outcome_name": "Yes",
        "type": "SHORT",
        "action": "NO / FADE HYPE",
        "entry": 0.82,
        "exit": 0.67,
        "stop": 0.9,
        "score": 8,
        "reason": "price > 0.80, hype-sensitive title",
        "event_volume_24hr": 50000,
        "event_liquidity": 100000,
        "event_slug": "sample-event",
    }
    text = format_signal(signal)
    assert "📢 SIGNAL V2" in text
    assert "Sample event" in text
    assert "Score: 8/10" in text
    assert "https://polymarket.com/event/sample-event" in text


def run_tests() -> None:
    test_has_telegram_credentials_false_when_env_empty()
    test_extract_markets_from_event()
    test_extract_markets_skips_closed()
    test_extract_outcomes_gamma_json_strings()
    test_analyze_market_short_signal()
    test_analyze_market_mid_signal()
    test_analyze_market_bad_prices()
    test_passes_event_filters_true()
    test_passes_event_filters_false_for_sports()
    test_proxy_dict_url_takes_precedence()
    test_proxy_dict_from_host_port_user_password()
    test_format_signal()
    get_logger().info("all tests passed | version=%s", VERSION)


if __name__ == "__main__":
    log_startup_banner()
    try:
        if MODE == "run_tests":
            run_tests()
        elif MODE == "test_message":
            send_test_message()
        else:
            run_bot()
    except KeyboardInterrupt:
        get_logger().info("bot stopped by user | version=%s", VERSION)
        sys.exit(0)
