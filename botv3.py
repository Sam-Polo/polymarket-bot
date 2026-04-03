import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, quote_plus
from xml.etree import ElementTree as ET

import requests

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

# =========================================================
# POLYMARKET SIGNAL BOT V3 (NEWS-DRIVEN, PyCharm-friendly)
# =========================================================
# What this version does:
# 1) Pulls fresh headlines from configurable RSS feeds / Google News search feeds
# 2) Classifies the headline into action tags (approve / reject / delay / cut / hike / win / lose ...)
# 3) Fetches active Polymarket events from the Gamma API
# 4) Matches a headline to the most relevant market(s)
# 5) Checks current price + recent price history + order book using public CLOB endpoints
# 6) Sends a signal ONLY if the headline and market price look mismatched
#
# Notes:
# - This bot DOES NOT place trades.
# - Config: .env (see .env.example), same as bot.py + optional NEWS_* proxy for RSS.
# - Deps: pip install -r requirements.txt (requests, python-dotenv, PySocks for socks5 proxy).
# =========================================================

VERSION = "0.3.0"
MSK_TZ = timezone(timedelta(hours=3), name="MSK")


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


MODE = (os.getenv("MODE") or "bot").strip().lower()
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 20)
POLL_INTERVAL_SECONDS = _env_int("POLL_INTERVAL_SECONDS", 60)
MAX_LOOPS = _env_int("MAX_LOOPS", 0)

# news tuning (optional .env overrides)
NEWS_MAX_AGE_MINUTES = _env_int("NEWS_MAX_AGE_MINUTES", 15)
NEWS_LOOKBACK_MINUTES = _env_int("NEWS_LOOKBACK_MINUTES", 30)
TOP_NEWS_PER_CYCLE = _env_int("TOP_NEWS_PER_CYCLE", 25)
MIN_NEWS_STRENGTH = _env_int("MIN_NEWS_STRENGTH", 2)

GAMMA_EVENTS_URL = (os.getenv("GAMMA_EVENTS_URL") or "https://gamma-api.polymarket.com/events").strip()
GAMMA_EVENTS_ORDER = (os.getenv("GAMMA_EVENTS_ORDER") or "volume24hr").strip()
CLOB_BOOK_API = (os.getenv("CLOB_BOOK_API") or "https://clob.polymarket.com/book").strip()
CLOB_HISTORY_API = (os.getenv("CLOB_HISTORY_API") or "https://clob.polymarket.com/prices-history").strip()
MAX_EVENTS_PER_REQUEST = _env_int("MAX_EVENTS_PER_REQUEST", 100)
ONLY_ACTIVE = _env_bool("ONLY_ACTIVE", True)
ONLY_OPEN = _env_bool("ONLY_OPEN", True)
MIN_EVENT_VOLUME_24H = _env_float("MIN_EVENT_VOLUME_24H", 1000.0)
MIN_EVENT_LIQUIDITY = _env_float("MIN_EVENT_LIQUIDITY", 5000.0)
HISTORY_LOOKBACK_MINUTES = _env_int("HISTORY_LOOKBACK_MINUTES", 15)

# mismatch thresholds (optional .env)
MIN_ABSOLUTE_MISMATCH = _env_float("MIN_ABSOLUTE_MISMATCH", 0.08)
MIN_EXPECTED_MOVE = _env_float("MIN_EXPECTED_MOVE", 0.06)
MAX_MOVE_ALREADY_REACTED = _env_float("MAX_MOVE_ALREADY_REACTED", 0.07)

# Default public news feeds.
# You can add or remove queries here.
# Google News RSS search feeds are practical defaults that do not require an API key.
GOOGLE_NEWS_QUERIES = [
    "SEC ETF approval",
    "Bitcoin ETF approval",
    "Ethereum ETF approval",
    "Federal Reserve rates",
    "Fed cut rates",
    "Fed hike rates",
    "US CPI inflation",
    "US recession",
    "US Senate election",
    "US House election",
    "Trump election",
    "Biden election",
    "Solana ETF",
    "token launch crypto"
]
DIRECT_RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]

# =========================
# TEXT / NLP CONFIG
# =========================
STOPWORDS = {
    "the", "a", "an", "and", "or", "to", "of", "for", "in", "on", "by", "with", "at", "from", "is",
    "are", "was", "were", "be", "will", "would", "can", "could", "should", "this", "that", "these",
    "those", "as", "about", "after", "before", "into", "than", "then", "it", "its", "their", "they",
    "them", "he", "she", "his", "her", "you", "your", "we", "our", "has", "have", "had", "says",
    "say", "said", "amid", "amidst", "new", "latest", "more", "less", "over", "under", "up", "down",
    "us", "u.s", "u.s.", "news", "report", "reports", "reuters", "update", "live"
}

ACTION_SYNONYMS = {
    "approve": {"approve", "approved", "approval", "approves", "pass", "passed", "passes", "greenlights", "greenlight"},
    "reject": {"reject", "rejected", "rejects", "deny", "denied", "denies", "block", "blocked", "blocks", "ban", "banned", "bans"},
    "delay": {"delay", "delayed", "delays", "postpone", "postponed", "postpones", "extend", "extended", "extends"},
    "cut": {"cut", "cuts", "reduce", "reduced", "reduces", "easing", "ease"},
    "hike": {"hike", "hikes", "raise", "raises", "raised", "increase", "increased", "increases", "tightening", "tighten"},
    "win": {"win", "wins", "won", "victory", "victories", "elected", "reelected", "beats", "beat"},
    "lose": {"lose", "loses", "lost", "defeat", "defeated", "falls", "fell", "drop", "drops", "dropped"},
    "launch": {"launch", "launches", "launched", "debut", "debuts", "debuted", "list", "listed", "lists"},
    "recession": {"recession"},
    "inflation_up": {"inflation", "hotter", "surges", "surge", "accelerates", "accelerated"},
    "inflation_down": {"cooling", "cools", "cooled", "disinflation", "softer"},
}

OPPOSITE_ACTIONS = {
    "approve": {"reject", "delay"},
    "reject": {"approve"},
    "delay": {"approve", "launch"},
    "cut": {"hike"},
    "hike": {"cut"},
    "win": {"lose"},
    "lose": {"win"},
    "launch": {"delay", "reject"},
    "recession": set(),
    "inflation_up": {"inflation_down"},
    "inflation_down": {"inflation_up"},
}

TOPIC_KEYWORDS = {
    "crypto": {"bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crypto", "token", "etf", "sec", "airdrop", "fdv"},
    "macro": {"fed", "federal", "reserve", "rates", "rate", "inflation", "cpi", "recession", "payrolls", "gdp"},
    "politics": {"election", "president", "senate", "house", "trump", "biden", "campaign", "vote", "voter"},
}

STRONG_HEADLINE_WORDS = {
    "approved", "rejected", "won", "passed", "launched", "elected", "delayed", "cuts", "hikes", "raised", "reduced", "blocked"
}


class _MSKFormatter(logging.Formatter):
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
    log = logging.getLogger("polymarket_bot_v3")
    log.setLevel(getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").strip().upper(), logging.INFO))
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_MSKFormatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(handler)
    log.propagate = False
    _logger = log
    return log


def log_startup_banner() -> None:
    get_logger().info("polymarket signal bot v3 starting | version=%s | mode=%s", VERSION, MODE)


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
            log.info("set TELEGRAM_TOKEN and CHAT_ID in .env (see .env.example).")



def now_utc() -> datetime:
    return datetime.now(timezone.utc)



def build_google_news_rss_url(query: str) -> str:
    encoded = quote_plus(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"



def build_all_feed_urls() -> List[str]:
    urls = [build_google_news_rss_url(query) for query in GOOGLE_NEWS_QUERIES]
    urls.extend(DIRECT_RSS_FEEDS)
    return urls



def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default



def parse_jsonish_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            loaded = json.loads(text)
            return loaded if isinstance(loaded, list) else []
        except json.JSONDecodeError:
            return []
    return []



def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()



def tokenize(text: str) -> List[str]:
    words = re.findall(r"[a-zA-Z0-9$]+", normalize_text(text))
    return [word for word in words if len(word) >= 3 and word not in STOPWORDS]



def keyword_topics(text: str) -> Set[str]:
    lowered = normalize_text(text)
    topics: Set[str] = set()
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            topics.add(topic)
    return topics



def detect_action_tags(text: str) -> Set[str]:
    lowered = normalize_text(text)
    tags: Set[str] = set()
    for action, synonyms in ACTION_SYNONYMS.items():
        if any(re.search(rf"\b{re.escape(word)}\b", lowered) for word in synonyms):
            tags.add(action)
    return tags



def headline_strength(text: str) -> int:
    tokens = set(tokenize(text))
    strength = 1
    if any(word in tokens for word in STRONG_HEADLINE_WORDS):
        strength += 1
    if any(word in tokens for word in ["approved", "rejected", "won", "elected", "launched", "blocked"]):
        strength += 1
    return min(strength, 3)



def parse_pub_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None



def fetch_rss_feed(url: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    http = session or requests
    try:
        response = http.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except Exception as error:
        get_logger().warning("RSS fetch failed | url=%s | err=%s", url, error)
        return []

    items: List[Dict[str, Any]] = []

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        description = (item.findtext("description") or "").strip()
        pub_date = parse_pub_date(item.findtext("pubDate"))
        source = (item.findtext("source") or "").strip()

        if title:
            items.append({
                "title": title,
                "link": link,
                "description": description,
                "published_at": pub_date,
                "source": source,
                "feed_url": url,
            })

    return items



def fetch_recent_news(session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    threshold = now_utc() - timedelta(minutes=NEWS_LOOKBACK_MINUTES)
    all_items: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for url in build_all_feed_urls():
        for item in fetch_rss_feed(url, session=session):
            dedupe_key = f"{item['title']}|{item['link']}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            published_at = item.get("published_at")
            if published_at is not None and published_at < threshold:
                continue

            classification = classify_news_item(item)
            if classification["strength"] < MIN_NEWS_STRENGTH:
                continue

            merged = dict(item)
            merged.update(classification)
            all_items.append(merged)

    all_items.sort(key=lambda x: x.get("published_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return all_items[:TOP_NEWS_PER_CYCLE]



def classify_news_item(item: Dict[str, Any]) -> Dict[str, Any]:
    text = f"{item.get('title', '')} {item.get('description', '')}"
    actions = detect_action_tags(text)
    topics = keyword_topics(text)
    strength = headline_strength(text)

    age_minutes: Optional[float] = None
    published_at = item.get("published_at")
    if isinstance(published_at, datetime):
        age_minutes = (now_utc() - published_at).total_seconds() / 60.0

    fresh_enough = age_minutes is None or age_minutes <= NEWS_MAX_AGE_MINUTES
    return {
        "actions": actions,
        "topics": topics,
        "strength": strength,
        "age_minutes": age_minutes,
        "fresh_enough": fresh_enough,
    }



def fetch_active_events(session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
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
    except requests.RequestException as error:
        get_logger().error("Gamma /events request failed: %s", error)
        return []
    except Exception as error:
        get_logger().error("unexpected error fetching events: %s", error)
        return []



def extract_markets_from_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
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



def passes_event_filters(event: Dict[str, Any]) -> bool:
    volume_24hr = safe_float(event.get("volume24hr", event.get("volume_24hr", 0.0)))
    liquidity = safe_float(event.get("liquidity", 0.0))
    return volume_24hr >= MIN_EVENT_VOLUME_24H and liquidity >= MIN_EVENT_LIQUIDITY



def extract_market_snapshot(market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    names_raw = parse_jsonish_list(market.get("outcomes"))
    prices_raw = parse_jsonish_list(market.get("outcomePrices"))
    token_ids_raw = parse_jsonish_list(market.get("clobTokenIds"))

    # Some responses may already contain structured outcomes.
    if not names_raw and isinstance(market.get("outcomes"), list) and market.get("outcomes"):
        for item in market.get("outcomes", []):
            if isinstance(item, dict):
                names_raw.append(item.get("name") or item.get("outcome") or "")
                prices_raw.append(item.get("price") or item.get("outcomePrice") or 0)
                token_ids_raw.append(item.get("token_id") or item.get("tokenId") or "")

    names = [str(x).strip() for x in names_raw]
    prices = [safe_float(x) for x in prices_raw]
    token_ids = [str(x).strip() for x in token_ids_raw]

    outcomes: List[Dict[str, Any]] = []
    max_len = max(len(names), len(prices), len(token_ids))
    if max_len == 0:
        return None

    for i in range(max_len):
        outcomes.append({
            "name": names[i] if i < len(names) else f"Outcome {i + 1}",
            "price": prices[i] if i < len(prices) else 0.0,
            "token_id": token_ids[i] if i < len(token_ids) else "",
        })

    yes_outcome = None
    no_outcome = None
    for outcome in outcomes:
        lower_name = normalize_text(outcome["name"])
        if lower_name == "yes":
            yes_outcome = outcome
        elif lower_name == "no":
            no_outcome = outcome

    if yes_outcome is None and len(outcomes) >= 1:
        yes_outcome = outcomes[0]
    if no_outcome is None and len(outcomes) >= 2:
        no_outcome = outcomes[1]

    return {
        "question": str(market.get("question") or market.get("title") or "").strip(),
        "slug": str(market.get("slug") or "").strip(),
        "outcomes": outcomes,
        "yes": yes_outcome,
        "no": no_outcome,
    }



def fetch_order_book(token_id: str, session: Optional[requests.Session] = None) -> Optional[Dict[str, Any]]:
    if not token_id:
        return None
    http = session or requests
    try:
        response = http.get(CLOB_BOOK_API, params={"token_id": token_id}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data
        return None
    except Exception:
        return None



def fetch_price_history(token_id: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    if not token_id:
        return []
    http = session or requests
    end_ts = int(time.time())
    start_ts = end_ts - (HISTORY_LOOKBACK_MINUTES * 60)
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "interval": "1m",
        "fidelity": 1,
    }
    try:
        response = http.get(CLOB_HISTORY_API, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        history = data.get("history", []) if isinstance(data, dict) else []
        return history if isinstance(history, list) else []
    except Exception:
        return []



def best_current_yes_price(yes_outcome: Dict[str, Any], book: Optional[Dict[str, Any]], history: List[Dict[str, Any]]) -> float:
    fallback = safe_float(yes_outcome.get("price", 0.0))

    if isinstance(book, dict):
        bids = book.get("bids", []) if isinstance(book.get("bids"), list) else []
        asks = book.get("asks", []) if isinstance(book.get("asks"), list) else []
        best_bid = safe_float(bids[0].get("price")) if bids else 0.0
        best_ask = safe_float(asks[0].get("price")) if asks else 0.0
        last_trade = safe_float(book.get("last_trade_price", 0.0))

        if best_bid > 0 and best_ask > 0:
            return round((best_bid + best_ask) / 2.0, 4)
        if last_trade > 0:
            return round(last_trade, 4)
        if best_bid > 0:
            return round(best_bid, 4)
        if best_ask > 0:
            return round(best_ask, 4)

    if history:
        last_point = history[-1]
        hist_price = safe_float(last_point.get("p", 0.0))
        if hist_price > 0:
            return round(hist_price, 4)

    return round(fallback, 4)



def history_oldest_price(history: List[Dict[str, Any]], fallback: float) -> float:
    if not history:
        return fallback
    oldest = history[0]
    value = safe_float(oldest.get("p", fallback), fallback)
    return round(value, 4)



def direction_relation(news_actions: Set[str], market_actions: Set[str]) -> str:
    if not news_actions or not market_actions:
        return "none"
    if news_actions & market_actions:
        return "same"
    for action in news_actions:
        opposites = OPPOSITE_ACTIONS.get(action, set())
        if market_actions & opposites:
            return "opposite"
    return "none"



def overlap_score(news_text: str, market_text: str) -> int:
    news_tokens = set(tokenize(news_text))
    market_tokens = set(tokenize(market_text))
    return len(news_tokens & market_tokens)



def topic_overlap(news_topics: Set[str], market_text: str) -> int:
    market_topics = keyword_topics(market_text)
    return len(news_topics & market_topics)



def score_market_match(news_item: Dict[str, Any], event_title: str, market_question: str) -> Tuple[int, str]:
    news_text = f"{news_item.get('title', '')} {news_item.get('description', '')}"
    combined_market_text = f"{event_title} {market_question}"
    relation = direction_relation(news_item.get("actions", set()), detect_action_tags(combined_market_text))

    score = 0
    score += overlap_score(news_text, combined_market_text) * 2
    score += topic_overlap(news_item.get("topics", set()), combined_market_text) * 3

    if relation == "same":
        score += 4
    elif relation == "opposite":
        score += 3

    return score, relation



def expected_yes_zone(relation: str, strength: int) -> Tuple[float, float]:
    if relation == "same":
        if strength >= 3:
            return 0.72, 0.92
        if strength == 2:
            return 0.65, 0.85
        return 0.58, 0.78

    if relation == "opposite":
        if strength >= 3:
            return 0.08, 0.28
        if strength == 2:
            return 0.15, 0.35
        return 0.22, 0.42

    return 0.0, 1.0



def compute_trade_levels(trade_side: str, current_yes_price: float, zone_low: float, zone_high: float) -> Tuple[float, float, float]:
    if trade_side == "BUY YES":
        entry = current_yes_price
        take_profit = min(max(zone_low, current_yes_price + 0.08), 0.95)
        stop = max(current_yes_price - 0.05, 0.01)
        return round(entry, 2), round(take_profit, 2), round(stop, 2)

    # BUY NO, using NO-side prices
    current_no_price = 1.0 - current_yes_price
    no_zone_low = 1.0 - zone_high
    no_zone_high = 1.0 - zone_low
    entry = current_no_price
    take_profit = min(max(no_zone_low, current_no_price + 0.08), 0.95)
    stop = max(current_no_price - 0.05, 0.01)
    return round(entry, 2), round(take_profit, 2), round(stop, 2)



def evaluate_news_market_mismatch(news_item: Dict[str, Any], event: Dict[str, Any], market: Dict[str, Any], session: Optional[requests.Session] = None) -> Optional[Dict[str, Any]]:
    event_title = str(event.get("title") or event.get("slug") or "Untitled event").strip()
    event_slug = str(event.get("slug") or "").strip()

    snapshot = extract_market_snapshot(market)
    if snapshot is None or snapshot.get("yes") is None:
        return None

    market_question = snapshot.get("question", "")
    match_score, relation = score_market_match(news_item, event_title, market_question)
    if match_score < 6 or relation == "none":
        return None

    if not news_item.get("fresh_enough", False):
        return None

    strength = int(news_item.get("strength", 1))
    zone_low, zone_high = expected_yes_zone(relation, strength)

    yes_outcome = snapshot["yes"]
    token_id = str(yes_outcome.get("token_id") or "")
    book = fetch_order_book(token_id, session=session)
    history = fetch_price_history(token_id, session=session)

    current_yes_price = best_current_yes_price(yes_outcome, book, history)
    past_yes_price = history_oldest_price(history, fallback=safe_float(yes_outcome.get("price", 0.0)))
    recent_move = round(current_yes_price - past_yes_price, 4)

    mismatch = 0.0
    trade_side = ""
    expected_move = 0.0

    if current_yes_price < zone_low:
        mismatch = zone_low - current_yes_price
        trade_side = "BUY YES"
        expected_move = max(zone_low - current_yes_price, 0.0)
    elif current_yes_price > zone_high:
        mismatch = current_yes_price - zone_high
        trade_side = "BUY NO"
        expected_move = max(current_yes_price - zone_high, 0.0)
    else:
        return None

    if mismatch < MIN_ABSOLUTE_MISMATCH:
        return None

    if expected_move < MIN_EXPECTED_MOVE:
        return None

    # If the price already moved aggressively in the correct direction, skip it as probably priced in.
    if trade_side == "BUY YES" and recent_move >= MAX_MOVE_ALREADY_REACTED:
        return None
    if trade_side == "BUY NO" and recent_move <= -MAX_MOVE_ALREADY_REACTED:
        return None

    entry, take_profit, stop = compute_trade_levels(trade_side, current_yes_price, zone_low, zone_high)

    age_minutes = news_item.get("age_minutes")
    age_text = "unknown"
    if isinstance(age_minutes, (int, float)):
        age_text = f"{age_minutes:.1f}m"

    reasons = [
        f"news-market relation: {relation}",
        f"headline strength: {strength}/3",
        f"match score: {match_score}",
        f"current YES {current_yes_price:.2f} vs expected YES zone {zone_low:.2f}-{zone_high:.2f}",
        f"recent YES move over ~{HISTORY_LOOKBACK_MINUTES}m: {recent_move:+.2f}",
    ]

    return {
        "headline": news_item.get("title", ""),
        "headline_link": news_item.get("link", ""),
        "headline_source": news_item.get("source", "") or "Unknown source",
        "headline_age": age_text,
        "event_title": event_title,
        "event_slug": event_slug,
        "market_question": market_question,
        "trade_side": trade_side,
        "entry": entry,
        "take_profit": take_profit,
        "stop": stop,
        "current_yes": round(current_yes_price, 2),
        "expected_yes_low": round(zone_low, 2),
        "expected_yes_high": round(zone_high, 2),
        "mismatch": round(mismatch, 2),
        "match_score": match_score,
        "news_strength": strength,
        "reason": "; ".join(reasons),
    }



def find_news_driven_signals(
    polymarket_session: Optional[requests.Session] = None,
    news_session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    news_items = fetch_recent_news(session=news_session)
    if not news_items:
        get_logger().info("no fresh headlines this cycle")
        return []

    events = fetch_active_events(session=polymarket_session)
    if not events:
        get_logger().warning("no Polymarket events fetched this cycle")
        return []

    signals: List[Dict[str, Any]] = []

    for news_item in news_items:
        for event in events:
            if not passes_event_filters(event):
                continue

            event_title = str(event.get("title") or event.get("slug") or "")
            markets = extract_markets_from_event(event)
            if not markets:
                continue

            # Cheap event-level prefilter before deeper evaluation.
            event_level_score = overlap_score(news_item.get("title", ""), event_title) + topic_overlap(news_item.get("topics", set()), event_title)
            if event_level_score < 1:
                continue

            for market in markets:
                signal = evaluate_news_market_mismatch(news_item, event, market, session=polymarket_session)
                if signal is not None:
                    signals.append(signal)

    signals.sort(key=lambda item: (-item["mismatch"], -item["match_score"], item["headline_age"]))
    return signals



def format_signal(signal: Dict[str, Any]) -> str:
    link_line = ""
    if signal.get("event_slug"):
        link_line = f"\nPolymarket: https://polymarket.com/event/{signal['event_slug']}"

    news_line = ""
    if signal.get("headline_link"):
        news_line = f"\nNews: {signal['headline_link']}"

    return (
        "📰 NEWS-LAG SIGNAL\n\n"
        f"Headline: {signal['headline']}\n"
        f"Source: {signal['headline_source']}\n"
        f"Age: {signal['headline_age']}\n\n"
        f"Event: {signal['event_title']}\n"
        f"Market: {signal['market_question']}\n\n"
        f"Trade side: {signal['trade_side']}\n"
        f"Entry: {signal['entry']}\n"
        f"Take profit: {signal['take_profit']}\n"
        f"Stop: {signal['stop']}\n\n"
        f"Current YES: {signal['current_yes']}\n"
        f"Expected YES zone: {signal['expected_yes_low']}-{signal['expected_yes_high']}\n"
        f"Mismatch: {signal['mismatch']}\n"
        f"Match score: {signal['match_score']}\n"
        f"News strength: {signal['news_strength']}/3\n\n"
        f"Why: {signal['reason']}"
        f"{link_line}"
        f"{news_line}"
    )



def send_telegram_message(text: str, session: Optional[requests.Session] = None) -> bool:
    if get_runtime_mode() == "dry-run":
        print("\n--- DRY RUN MESSAGE ---")
        print(text)
        print("--- END MESSAGE ---\n")
        return True

    http = session or requests
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
            get_logger().warning("Telegram sendMessage not ok (details in API response)")
            return False
        return True
    except requests.RequestException as error:
        get_logger().error("Telegram HTTP error: %s", error)
        return False
    except Exception as error:
        get_logger().error("Telegram unexpected error: %s", error)
        return False



def send_test_message() -> None:
    log = get_logger()
    message = "🚀 Polymarket Signal Bot V3 connected. News-driven mode is ready."
    log.info("sending Telegram test message")
    tg = build_requests_session(_proxy_dict_for_prefix("TELEGRAM"))
    success = send_telegram_message(message, session=tg)
    log.info("test message sent." if success else "test message failed.")



def build_signal_key(signal: Dict[str, Any]) -> str:
    return f"{signal['headline']}|{signal['event_title']}|{signal['market_question']}|{signal['trade_side']}|{signal['entry']}"



def run_bot() -> None:
    print_startup_help()
    seen: Set[str] = set()
    loop_count = 0
    log = get_logger()

    tg_proxy = _proxy_dict_for_prefix("TELEGRAM")
    pm_proxy = _proxy_dict_for_prefix("GAMMA")
    news_proxy = _proxy_dict_for_prefix("NEWS")
    if tg_proxy:
        log.info("telegram requests use proxy (url not logged)")
    if pm_proxy:
        log.info("polymarket (gamma+clob) requests use proxy (url not logged)")
    if news_proxy:
        log.info("RSS/news requests use proxy (url not logged)")

    telegram_session = build_requests_session(tg_proxy)
    polymarket_session = build_requests_session(pm_proxy)
    news_session = build_requests_session(news_proxy)

    while True:
        loop_count += 1
        log.info("scan cycle %s | news + polymarket", loop_count)
        signals = find_news_driven_signals(
            polymarket_session=polymarket_session,
            news_session=news_session,
        )

        if not signals:
            log.info("no news-lag signals this cycle")

        for signal in signals:
            key = build_signal_key(signal)
            if key in seen:
                continue
            sent = send_telegram_message(format_signal(signal), session=telegram_session)
            if sent:
                seen.add(key)

        if MAX_LOOPS > 0 and loop_count >= MAX_LOOPS:
            log.info("stopping after %s loop(s) (MAX_LOOPS=%s)", loop_count, MAX_LOOPS)
            break

        time.sleep(POLL_INTERVAL_SECONDS)


# =========================
# TESTS (network-free)
# =========================

def test_parse_jsonish_list() -> None:
    assert parse_jsonish_list('["Yes", "No"]') == ["Yes", "No"]
    assert parse_jsonish_list("") == []
    assert parse_jsonish_list(None) == []



def test_detect_action_tags() -> None:
    tags = detect_action_tags("SEC approved Ethereum ETF after delay")
    assert "approve" in tags
    assert "delay" in tags



def test_direction_relation_same() -> None:
    assert direction_relation({"approve"}, {"approve"}) == "same"



def test_direction_relation_opposite() -> None:
    assert direction_relation({"approve"}, {"reject"}) == "opposite"



def test_extract_market_snapshot_from_json_strings() -> None:
    market = {
        "question": "Will the SEC approve ETH ETF by June?",
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '[0.42, 0.58]',
        "clobTokenIds": '["yes_token", "no_token"]',
    }
    snapshot = extract_market_snapshot(market)
    assert snapshot is not None
    assert snapshot["yes"]["name"] == "Yes"
    assert snapshot["yes"]["price"] == 0.42
    assert snapshot["yes"]["token_id"] == "yes_token"



def test_score_market_match_is_meaningful() -> None:
    news = {
        "title": "SEC approved Ethereum ETF filing",
        "description": "",
        "actions": {"approve"},
        "topics": {"crypto"},
    }
    score, relation = score_market_match(news, "Ethereum ETF", "Will SEC approve Ethereum ETF by June?")
    assert relation == "same"
    assert score >= 6



def test_evaluate_news_market_mismatch_buy_yes() -> None:
    news = {
        "title": "SEC approved Ethereum ETF filing",
        "description": "",
        "link": "https://example.com/news",
        "source": "Example",
        "actions": {"approve"},
        "topics": {"crypto"},
        "strength": 3,
        "fresh_enough": True,
        "age_minutes": 2.0,
    }
    event = {
        "title": "Ethereum ETF",
        "slug": "ethereum-etf",
        "volume24hr": 10000,
        "liquidity": 20000,
    }
    market = {
        "question": "Will SEC approve Ethereum ETF by June?",
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '[0.41, 0.59]',
        "clobTokenIds": '["yes_token", "no_token"]',
    }

    class DummySession:
        def get(self, url, params=None, timeout=None):
            class Resp:
                def __init__(self, payload):
                    self._payload = payload
                def raise_for_status(self):
                    return None
                def json(self):
                    return self._payload
            if "book" in url:
                return Resp({"last_trade_price": "0.41", "bids": [], "asks": []})
            return Resp({"history": [{"t": 1, "p": 0.40}, {"t": 2, "p": 0.41}]})

    signal = evaluate_news_market_mismatch(news, event, market, session=DummySession())
    assert signal is not None
    assert signal["trade_side"] == "BUY YES"
    assert signal["current_yes"] == 0.41



def test_evaluate_news_market_mismatch_buy_no() -> None:
    news = {
        "title": "SEC rejected Ethereum ETF filing",
        "description": "",
        "link": "https://example.com/news",
        "source": "Example",
        "actions": {"reject"},
        "topics": {"crypto"},
        "strength": 3,
        "fresh_enough": True,
        "age_minutes": 2.0,
    }
    event = {
        "title": "Ethereum ETF",
        "slug": "ethereum-etf",
        "volume24hr": 10000,
        "liquidity": 20000,
    }
    market = {
        "question": "Will SEC approve Ethereum ETF by June?",
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '[0.70, 0.30]',
        "clobTokenIds": '["yes_token", "no_token"]',
    }

    class DummySession:
        def get(self, url, params=None, timeout=None):
            class Resp:
                def __init__(self, payload):
                    self._payload = payload
                def raise_for_status(self):
                    return None
                def json(self):
                    return self._payload
            if "book" in url:
                return Resp({"last_trade_price": "0.70", "bids": [], "asks": []})
            return Resp({"history": [{"t": 1, "p": 0.70}, {"t": 2, "p": 0.70}]})

    signal = evaluate_news_market_mismatch(news, event, market, session=DummySession())
    assert signal is not None
    assert signal["trade_side"] == "BUY NO"
    assert signal["current_yes"] == 0.7



def test_format_signal() -> None:
    signal = {
        "headline": "SEC approved Ethereum ETF filing",
        "headline_link": "https://example.com/news",
        "headline_source": "Example",
        "headline_age": "2.0m",
        "event_title": "Ethereum ETF",
        "event_slug": "ethereum-etf",
        "market_question": "Will SEC approve Ethereum ETF by June?",
        "trade_side": "BUY YES",
        "entry": 0.41,
        "take_profit": 0.52,
        "stop": 0.36,
        "current_yes": 0.41,
        "expected_yes_low": 0.72,
        "expected_yes_high": 0.92,
        "mismatch": 0.31,
        "match_score": 12,
        "news_strength": 3,
        "reason": "example reason",
    }
    text = format_signal(signal)
    assert "NEWS-LAG SIGNAL" in text
    assert "BUY YES" in text
    assert "ethereum-etf" in text



def run_tests() -> None:
    test_parse_jsonish_list()
    test_detect_action_tags()
    test_direction_relation_same()
    test_direction_relation_opposite()
    test_extract_market_snapshot_from_json_strings()
    test_score_market_match_is_meaningful()
    test_evaluate_news_market_mismatch_buy_yes()
    test_evaluate_news_market_mismatch_buy_no()
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
