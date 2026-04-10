import json
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote, quote_plus
from xml.etree import ElementTree as ET

import requests
try:
    from curl_cffi import requests as curl_requests
    _CURL_CFFI_AVAILABLE = True
except ImportError:
    _CURL_CFFI_AVAILABLE = False

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

# =========================================================
# POLYMARKET SIGNAL BOT V5
# =========================================================
# Config: .env + .env.example (Telegram, proxy, TheNewsAPI/NewsAPI, GDELT, Gamma).
# pip install -r requirements.txt
# Modes: MODE=run_tests | test_message | bot | report
# =========================================================

VERSION = "0.5.3"
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


def _env_multiline_list(key: str, default: List[str]) -> List[str]:
    raw = os.getenv(key)
    if not raw or not str(raw).strip():
        return default
    out: List[str] = []
    for line in str(raw).replace("|||", "\n").splitlines():
        s = line.strip()
        if s:
            out.append(s)
    return out


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


def newsapi_api_key() -> str:
    return (os.getenv("NEWSAPI_API_KEY") or "").strip()


MODE = (os.getenv("MODE") or "bot").strip().lower()
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 20)
MAX_LOOPS = _env_int("MAX_LOOPS", 0)
POLL_INTERVAL_SECONDS = _env_int("POLL_INTERVAL_SECONDS", 60)

DB_PATH = (os.getenv("DB_PATH") or "polymarket_bot_v5.sqlite3").strip()
SNAPSHOT_MILESTONES_MIN = [5, 15, 30, 60]
MARKET_COOLDOWN_MIN = _env_int("MARKET_COOLDOWN_MIN", 20)
CLUSTER_COOLDOWN_MIN = _env_int("CLUSTER_COOLDOWN_MIN", 15)

ENABLE_GOOGLE_RSS = _env_bool("ENABLE_GOOGLE_RSS", True)
# Google News RSS: run every N cycles + pause between requests to avoid connection resets
# 20 queries/cycle × every 5 cycles × 60s = ~5760 req/day — acceptable
GOOGLE_RSS_EVERY_N_CYCLES = _env_int("GOOGLE_RSS_EVERY_N_CYCLES", 5)
GOOGLE_RSS_INTER_REQUEST_DELAY = _env_float("GOOGLE_RSS_INTER_REQUEST_DELAY", 2.0)
ENABLE_DIRECT_RSS = _env_bool("ENABLE_DIRECT_RSS", True)
# бесплатный NewsAPI плох для низкой задержки — по умолчанию выкл.; живой поток: RSS + TheNewsAPI + GDELT
ENABLE_NEWSAPI = _env_bool("ENABLE_NEWSAPI", False)
ENABLE_THENEWSAPI = _env_bool("ENABLE_THENEWSAPI", True)
ENABLE_GDELT = _env_bool("ENABLE_GDELT", True)
NEWSAPI_ONLY_REUTERS = _env_bool("NEWSAPI_ONLY_REUTERS", False)

NEWS_LOOKBACK_MINUTES = _env_int("NEWS_LOOKBACK_MINUTES", 45)
NEWS_MAX_AGE_MINUTES = _env_int("NEWS_MAX_AGE_MINUTES", 12)
TOP_NEWS_PER_CYCLE = _env_int("TOP_NEWS_PER_CYCLE", 40)
MIN_NEWS_CONFIDENCE = _env_int("MIN_NEWS_CONFIDENCE", 55)

GOOGLE_NEWS_QUERIES = [
    "SEC ETF approval",
    "Bitcoin ETF approval",
    "Ethereum ETF approval",
    "Federal Reserve rates",
    "Fed cut rates",
    "Fed hike rates",
    "US CPI inflation",
    "US recession",
    "Trump election",
    "Biden election",
    "Solana ETF",
    "token launch crypto",
]
REUTERS_GOOGLE_NEWS_QUERIES = [
    "site:reuters.com SEC ETF approval",
    "site:reuters.com Bitcoin ETF approval",
    "site:reuters.com Ethereum ETF approval",
    "site:reuters.com Federal Reserve rates",
    "site:reuters.com US CPI inflation",
    "site:reuters.com US recession",
    "site:reuters.com Trump election",
    "site:reuters.com Biden election",
]
# feeds.reuters.com часто не резолвится / www.reuters.com/rss — 401 без подписки; Reuters через Google RSS + TheNewsAPI
_DIRECT_RSS_FEEDS_DEFAULT = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.cnbc.com/id/10000664/device/rss/rss.html",
]
# браузероподобный запрос: иначе CNBC и др. режут «python-requests» (403)
_RSS_UA_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
RSS_HTTP_HEADERS = {
    "User-Agent": (os.getenv("RSS_USER_AGENT") or _RSS_UA_DEFAULT).strip(),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
_NEWSAPI_EVERYTHING_QUERIES_DEFAULT = [
    "(SEC OR Fed OR CPI) AND (ETF OR bitcoin OR rates OR inflation OR recession)",
    "(Trump OR Biden OR election) AND (policy OR tariff OR election)",
    "(Solana OR ethereum OR crypto) AND (ETF OR launch OR regulation)",
]
# короткий список запросов к GDELT (меньше 429); расширить можно через GDELT_QUERIES_ENV
GDELT_QUERIES = [
    '"SEC" AND (ETF OR bitcoin OR ethereum)',
    '"Federal Reserve" AND (rates OR inflation OR recession)',
]
GDELT_MAX_RECORDS = _env_int("GDELT_MAX_RECORDS", 10)
# pause between GDELT calls to reduce 429 Too Many Requests; при 429 — пауза и повтор (см. fetch_gdelt)
GDELT_INTER_REQUEST_DELAY = _env_float("GDELT_INTER_REQUEST_DELAY", 6.0)
GDELT_429_RETRY_DELAY = _env_float("GDELT_429_RETRY_DELAY", 30.0)
# после серии 429 не дергать провайдер N минут
GDELT_BACKOFF_MINUTES = _env_int("GDELT_BACKOFF_MINUTES", 30)
# каждые N циклов бота (см. BOT_CYCLE_COUNTER в run_bot); алиас env: GDELT_FETCH_EVERY_N_CYCLES
_gdelt_every_raw = (os.getenv("GDELT_FETCH_EVERY_N_CYCLES") or os.getenv("GDELT_EVERY_N_CYCLES") or "5").strip()
try:
    GDELT_EVERY_N_CYCLES = int(_gdelt_every_raw)
except ValueError:
    GDELT_EVERY_N_CYCLES = 5

# NewsAPI /everything — override q list, domains, from/to, etc. via .env (see .env.example)
NEWSAPI_BASE_URL = (os.getenv("NEWSAPI_BASE_URL") or "https://newsapi.org/v2/everything").strip()
NEWSAPI_DOMAINS_DEFAULT = "reuters.com,bloomberg.com,cnbc.com,coindesk.com,theblock.co"
NEWSAPI_DOMAINS = (os.getenv("NEWSAPI_DOMAINS") or NEWSAPI_DOMAINS_DEFAULT).strip()
NEWSAPI_SOURCES = (os.getenv("NEWSAPI_SOURCES") or "").strip()
NEWSAPI_FROM = (os.getenv("NEWSAPI_FROM") or "").strip()
NEWSAPI_TO = (os.getenv("NEWSAPI_TO") or "").strip()
NEWSAPI_LANGUAGE = (os.getenv("NEWSAPI_LANGUAGE") or "en").strip()
NEWSAPI_SORT_BY = (os.getenv("NEWSAPI_SORT_BY") or "publishedAt").strip()
NEWSAPI_SEARCH_IN = (os.getenv("NEWSAPI_SEARCH_IN") or "title,description").strip()
NEWSAPI_PAGE_SIZE = _env_int("NEWSAPI_PAGE_SIZE", 25)
# NewsAPI: только если ENABLE_NEWSAPI=1; раз в N циклов (BOT_CYCLE_COUNTER % N == 0)
NEWSAPI_EVERY_N_CYCLES = _env_int("NEWSAPI_EVERY_N_CYCLES", 10)

# TheNewsAPI (api.thenewsapi.com)
THENEWSAPI_BASE_URL = (os.getenv("THENEWSAPI_BASE_URL") or "https://api.thenewsapi.com/v1/news/all").strip()
THENEWSAPI_LIMIT = _env_int("THENEWSAPI_LIMIT", 3)
THENEWSAPI_DOMAINS_DEFAULT = "reuters.com,bloomberg.com,cnbc.com,coindesk.com,theblock.co"
THENEWSAPI_DOMAINS = (os.getenv("THENEWSAPI_DOMAINS") or THENEWSAPI_DOMAINS_DEFAULT).strip()
THENEWSAPI_EVERY_N_CYCLES = _env_int("THENEWSAPI_EVERY_N_CYCLES", 2)
THENEWSAPI_BACKOFF_MINUTES = _env_int("THENEWSAPI_BACKOFF_MINUTES", 15)
_THENEWSAPI_QUERIES_DEFAULT = [
    "SEC ETF approval",
    "Bitcoin ETF approval",
    "Ethereum ETF approval",
    "Federal Reserve rates",
    "Fed cut rates",
    "Fed hike rates",
    "US CPI inflation",
    "US recession",
    "Trump election",
    "Biden election",
    "Solana ETF",
    "token launch crypto",
]

GAMMA_EVENTS_URL = (os.getenv("GAMMA_EVENTS_URL") or "https://gamma-api.polymarket.com/events").strip()
GAMMA_EVENTS_ORDER = (os.getenv("GAMMA_EVENTS_ORDER") or "volume24hr").strip()
CLOB_BOOK_API = (os.getenv("CLOB_BOOK_API") or "https://clob.polymarket.com/book").strip()
CLOB_HISTORY_API = (os.getenv("CLOB_HISTORY_API") or "https://clob.polymarket.com/prices-history").strip()
MAX_EVENTS_PER_REQUEST = _env_int("MAX_EVENTS_PER_REQUEST", 100)
ONLY_ACTIVE = _env_bool("ONLY_ACTIVE", True)
ONLY_OPEN = _env_bool("ONLY_OPEN", True)
MIN_EVENT_VOLUME_24H = _env_float("MIN_EVENT_VOLUME_24H", 1000.0)
MIN_EVENT_LIQUIDITY = _env_float("MIN_EVENT_LIQUIDITY", 5000.0)
MAX_CANDIDATE_MARKETS_PER_NEWS = _env_int("MAX_CANDIDATE_MARKETS_PER_NEWS", 5)
HISTORY_LOOKBACK_MINUTES = _env_int("HISTORY_LOOKBACK_MINUTES", 120)
PRE_NEWS_CONTEXT_MINUTES = _env_int("PRE_NEWS_CONTEXT_MINUTES", 20)

# Watchlist
WATCHLIST_MODE = (os.getenv("WATCHLIST_MODE") or "watchlist").strip().lower()
WATCHLIST_TERMS = [
    "etf",
    "sec",
    "federal reserve",
    "fed",
    "inflation",
    "cpi",
    "recession",
    "trump",
    "biden",
    "senate",
    "house",
    "solana",
    "bitcoin",
    "ethereum",
]

# signal thresholds (optional .env)
MIN_REMAINING_MOVE = _env_float("MIN_REMAINING_MOVE", 0.05)
MIN_ABSOLUTE_MISMATCH = _env_float("MIN_ABSOLUTE_MISMATCH", 0.05)
MAX_ALLOWED_SPREAD = _env_float("MAX_ALLOWED_SPREAD", 0.08)
ALREADY_PRICED_RATIO = _env_float("ALREADY_PRICED_RATIO", 0.80)
TP_CAPTURE_RATIO = _env_float("TP_CAPTURE_RATIO", 0.80)
MIN_SIGNAL_CONFIDENCE = _env_int("MIN_SIGNAL_CONFIDENCE", 58)

STOPWORDS = {
    "the", "a", "an", "and", "or", "to", "of", "for", "in", "on", "by", "with", "at", "from", "is",
    "are", "was", "were", "be", "will", "would", "can", "could", "should", "this", "that", "these",
    "those", "as", "about", "after", "before", "into", "than", "then", "it", "its", "their", "they",
    "them", "he", "she", "his", "her", "you", "your", "we", "our", "has", "have", "had", "says",
    "say", "said", "amid", "amidst", "new", "latest", "more", "less", "over", "under", "up", "down",
    "us", "u.s", "u.s.", "news", "report", "reports", "reuters", "update", "live", "market",
}
UNCERTAINTY_WORDS = {
    "may", "might", "could", "consider", "considers", "considering", "possible", "possibly",
    "expected", "expects", "likely", "reportedly", "rumor", "rumoured", "rumored",
}
CONFIRMATION_WORDS = {
    "approved", "rejected", "won", "passed", "launched", "elected", "delayed",
    "cuts", "hikes", "raised", "reduced", "blocked", "files", "filed", "announced",
}
ACTION_SYNONYMS = {
    "approve": {"approve", "approved", "approval", "approves", "pass", "passed", "passes", "greenlights", "greenlight"},
    "reject": {"reject", "rejected", "rejects", "deny", "denied", "denies", "block", "blocked", "blocks", "ban", "banned", "bans"},
    "delay": {"delay", "delayed", "delays", "postpone", "postponed", "postpones", "extend", "extended", "extends"},
    "cut": {"cut", "cuts", "reduce", "reduced", "reduces", "easing", "ease"},
    "hike": {"hike", "hikes", "raise", "raises", "raised", "increase", "increased", "increases", "tightening", "tighten"},
    "win": {"win", "wins", "won", "victory", "victories", "elected", "reelected", "beats", "beat"},
    "lose": {"lose", "loses", "lost", "defeat", "defeated", "falls", "fell", "drop", "drops", "dropped"},
    "launch": {"launch", "launches", "launched", "debut", "debuts", "debuted", "list", "listed", "lists", "file", "files", "filed"},
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
    "politics": {"election", "president", "senate", "house", "trump", "biden", "campaign", "vote", "voter", "poll"},
}
ENTITY_SYNONYMS = {
    "sec": ["sec", "securities and exchange commission"],
    "bitcoin": ["bitcoin", "btc"],
    "ethereum": ["ethereum", "eth"],
    "solana": ["solana", "sol"],
    "fed": ["federal reserve", "fed", "fomc"],
    "inflation": ["inflation", "cpi", "consumer price index"],
    "recession": ["recession"],
    "trump": ["trump", "donald trump"],
    "biden": ["biden", "joe biden"],
    "senate": ["senate"],
    "house": ["house of representatives", "house"],
    "election": ["election", "vote", "voter", "poll"],
    "etf": ["etf", "exchange traded fund", "exchange-traded fund"],
    "token_launch": ["launch", "listing", "debut", "airdrop", "token"],
}
SOURCE_PRIORITY = {
    "reuters": 5,
    "bloomberg": 4,
    "cnbc": 4,
    "coindesk": 3,
    "the block": 3,
    "newsapi": 2,
    "thenewsapi": 2,
    "gdelt": 2,
    "google_news_rss": 1,
    "direct_rss": 1,
}
FINAL_DECISION_WORDS = {"approved", "rejected", "passed", "won", "elected", "blocked"}
PROCEDURAL_WORDS = {"filing", "comment", "period", "hearing", "deadline", "submitted", "filed", "extends"}
POLL_WORDS = {"poll", "polling", "survey"}
COMMENTARY_WORDS = {"says", "said", "commentary", "expects", "forecast", "forecasts", "signals"}
MACRO_RELEASE_WORDS = {"cpi", "inflation", "payrolls", "gdp", "jobs", "unemployment"}
TIER1_SOURCES = {"reuters", "bloomberg", "cnbc"}


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
    log = logging.getLogger("polymarket_bot_v5")
    log.setLevel(getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").strip().upper(), logging.INFO))
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_MSKFormatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(handler)
    log.propagate = False
    _logger = log
    return log


def log_startup_banner() -> None:
    get_logger().info("polymarket signal bot v5 starting | version=%s | mode=%s", VERSION, MODE)


def newsapi_queries_resolved() -> List[str]:
    return _env_multiline_list("NEWSAPI_QUERIES", _NEWSAPI_EVERYTHING_QUERIES_DEFAULT)


def thenewsapi_queries_resolved() -> List[str]:
    return _env_multiline_list("THENEWSAPI_QUERIES", _THENEWSAPI_QUERIES_DEFAULT)


def gdelt_queries_resolved() -> List[str]:
    return _env_multiline_list("GDELT_QUERIES_ENV", GDELT_QUERIES)


def direct_rss_feeds_resolved() -> List[str]:
    return _env_multiline_list("DIRECT_RSS_FEEDS", _DIRECT_RSS_FEEDS_DEFAULT)


def newsapi_domains_for_request() -> str:
    if NEWSAPI_ONLY_REUTERS:
        return "reuters.com"
    return NEWSAPI_DOMAINS


def newsapi_from_iso() -> str:
    if NEWSAPI_FROM:
        return NEWSAPI_FROM
    return (now_utc() - timedelta(minutes=NEWS_LOOKBACK_MINUTES)).isoformat().replace("+00:00", "Z")


@dataclass
class NewsItem:
    title: str
    link: str
    description: str
    published_at: Optional[datetime]
    source: str
    provider: str
    source_priority: int
    topics: Set[str] = field(default_factory=set)
    actions: Set[str] = field(default_factory=set)
    entities: Set[str] = field(default_factory=set)
    confidence: int = 0
    age_minutes: Optional[float] = None
    fresh_enough: bool = False
    cluster_key: str = ""
    event_type: str = "generic"
    certainty: str = "medium"


@dataclass
class MarketSemantics:
    question: str
    slug: str
    event_title: str
    event_slug: str
    event_volume_24h: float
    event_liquidity: float
    yes_token_id: str
    no_token_id: str
    yes_price_hint: float
    no_price_hint: float
    entities: Set[str]
    topics: Set[str]
    yes_actions: Set[str]
    deadline: str
    question_type: str
    yes_subject: str
    yes_object: str


@dataclass
class PriceContext:
    current_yes: float
    pre_news_yes: float
    old_context_yes: float
    recent_volatility: float
    spread: float
    imbalance: float
    actual_move_since_news: float
    pre_news_drift: float


@dataclass
class SignalCandidate:
    news: NewsItem
    market: MarketSemantics
    relation: str
    match_score: int
    expected_delta: float
    remaining_move: float
    trade_side: str
    entry: float
    take_profit: float
    stop: float
    current_yes: float
    pre_news_yes: float
    mismatch: float
    confidence: int
    reason: str


# =========================
# CORE HELPERS
# =========================
def has_telegram_credentials() -> bool:
    return bool(telegram_token() and chat_id())


def has_newsapi_key() -> bool:
    return bool(newsapi_api_key())


def thenewsapi_api_key() -> str:
    return (os.getenv("THENEWSAPI_API_KEY") or "").strip()


def has_thenewsapi_key() -> bool:
    key = thenewsapi_api_key()
    if not key:
        return False
    if key in ("YOUR_THENEWSAPI_KEY", "your_thenewsapi_key"):
        return False
    return True


def get_runtime_mode() -> str:
    if _env_bool("DRY_RUN", False):
        return "dry-run"
    if not has_telegram_credentials():
        return "dry-run"
    return "live"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# счётчик циклов run_bot + паузы провайдеров после 429
BOT_CYCLE_COUNTER = 0
PROVIDER_STATE: Dict[str, Dict[str, Any]] = {
    "gdelt": {"429_count": 0, "skip_until": None},
    "thenewsapi": {"429_count": 0, "skip_until": None},
}


def provider_skip_active(provider_name: str) -> bool:
    state = PROVIDER_STATE.get(provider_name, {})
    skip_until = state.get("skip_until")
    return isinstance(skip_until, datetime) and now_utc() < skip_until


def provider_mark_429(provider_name: str, backoff_minutes: int) -> None:
    state = PROVIDER_STATE.setdefault(provider_name, {"429_count": 0, "skip_until": None})
    state["429_count"] = int(state.get("429_count", 0)) + 1
    state["skip_until"] = now_utc() + timedelta(minutes=backoff_minutes)
    get_logger().warning(
        "provider %s rate limited (429), skip until %s MSK (~%s min)",
        provider_name,
        state["skip_until"].astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M"),
        backoff_minutes,
    )


def provider_mark_success(provider_name: str) -> None:
    state = PROVIDER_STATE.setdefault(provider_name, {"429_count": 0, "skip_until": None})
    state["429_count"] = 0
    state["skip_until"] = None


def should_fetch_thenewsapi() -> bool:
    if not ENABLE_THENEWSAPI or not has_thenewsapi_key():
        return False
    if provider_skip_active("thenewsapi"):
        return False
    if THENEWSAPI_EVERY_N_CYCLES <= 1:
        return True
    return BOT_CYCLE_COUNTER % THENEWSAPI_EVERY_N_CYCLES == 0


def should_fetch_gdelt() -> bool:
    if not ENABLE_GDELT:
        return False
    if provider_skip_active("gdelt"):
        return False
    if GDELT_EVERY_N_CYCLES <= 1:
        return True
    return BOT_CYCLE_COUNTER % GDELT_EVERY_N_CYCLES == 0


def should_fetch_newsapi() -> bool:
    if not ENABLE_NEWSAPI or not has_newsapi_key():
        return False
    if NEWSAPI_EVERY_N_CYCLES <= 1:
        return True
    return BOT_CYCLE_COUNTER % NEWSAPI_EVERY_N_CYCLES == 0


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def average(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def tokenize(text: str) -> List[str]:
    words = re.findall(r"[a-zA-Z0-9$]+", normalize_text(text))
    return [word for word in words if len(word) >= 3 and word not in STOPWORDS]


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


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        normalized = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def build_google_news_rss_url(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"


def score_source_priority(source_name: str, url: str, provider: str) -> int:
    text = normalize_text(f"{source_name} {url} {provider}")
    for key, value in SOURCE_PRIORITY.items():
        if key in text:
            return value
    return 1


def extract_entities(text: str) -> Set[str]:
    lowered = normalize_text(text)
    entities: Set[str] = set()
    for canonical, variants in ENTITY_SYNONYMS.items():
        for variant in variants:
            if re.search(rf"\b{re.escape(variant)}\b", lowered):
                entities.add(canonical)
                break
    return entities


def keyword_topics(text: str) -> Set[str]:
    lowered = normalize_text(text)
    topics: Set[str] = set()
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(re.search(rf"\b{re.escape(keyword)}\b", lowered) for keyword in keywords):
            topics.add(topic)
    return topics


def detect_action_tags(text: str) -> Set[str]:
    lowered = normalize_text(text)
    tags: Set[str] = set()
    for action, synonyms in ACTION_SYNONYMS.items():
        if any(re.search(rf"\b{re.escape(word)}\b", lowered) for word in synonyms):
            tags.add(action)
    return tags


def classify_news_event_type(title: str, description: str) -> Tuple[str, str]:
    text = normalize_text(f"{title} {description}")
    tokens = set(tokenize(text))
    certainty = "medium"
    if tokens & FINAL_DECISION_WORDS:
        event_type = "final_decision"
        certainty = "high"
    elif tokens & MACRO_RELEASE_WORDS:
        event_type = "macro_release"
        certainty = "high"
    elif tokens & POLL_WORDS:
        event_type = "poll_update"
        certainty = "medium"
    elif tokens & PROCEDURAL_WORDS:
        event_type = "procedural_update"
        certainty = "medium"
    elif tokens & COMMENTARY_WORDS:
        event_type = "commentary"
        certainty = "low"
    else:
        event_type = "generic"
    if tokens & UNCERTAINTY_WORDS:
        certainty = "low"
    return event_type, certainty


def headline_confidence(title: str, source_priority: int, age_minutes: Optional[float], event_type: str, certainty: str) -> int:
    tokens = set(tokenize(title))
    confidence = 35 + source_priority * 8
    if any(word in tokens for word in CONFIRMATION_WORDS):
        confidence += 15
    if any(word in tokens for word in UNCERTAINTY_WORDS):
        confidence -= 12
    if event_type == "final_decision":
        confidence += 10
    elif event_type == "macro_release":
        confidence += 8
    elif event_type == "procedural_update":
        confidence += 3
    elif event_type == "commentary":
        confidence -= 8
    elif event_type == "poll_update":
        confidence -= 2
    if certainty == "high":
        confidence += 8
    elif certainty == "low":
        confidence -= 10
    if age_minutes is not None:
        if age_minutes <= 2:
            confidence += 10
        elif age_minutes <= 5:
            confidence += 6
        elif age_minutes <= 10:
            confidence += 2
        else:
            confidence -= 6
    return int(clamp(confidence, 0, 100))


def canonical_title_key(title: str) -> str:
    return " ".join(tokenize(title)[:10])


def build_news_cluster_key(item: NewsItem) -> str:
    return f"{'-'.join(sorted(item.actions)) or 'na'}|{'-'.join(sorted(item.entities)) or 'na'}|{canonical_title_key(item.title)}"


def make_news_item(title: str, link: str, description: str, published_at: Optional[datetime], source: str, provider: str) -> NewsItem:
    source_priority = score_source_priority(source, link, provider)
    age_minutes = (now_utc() - published_at).total_seconds() / 60.0 if isinstance(published_at, datetime) else None
    event_type, certainty = classify_news_event_type(title, description)
    confidence = headline_confidence(title, source_priority, age_minutes, event_type, certainty)
    item = NewsItem(
        title=title.strip(),
        link=link.strip(),
        description=description.strip(),
        published_at=published_at,
        source=source.strip() or provider,
        provider=provider,
        source_priority=source_priority,
        topics=keyword_topics(f"{title} {description}"),
        actions=detect_action_tags(f"{title} {description}"),
        entities=extract_entities(f"{title} {description}"),
        confidence=confidence,
        age_minutes=age_minutes,
        fresh_enough=(age_minutes is None or age_minutes <= NEWS_MAX_AGE_MINUTES),
        cluster_key="",
        event_type=event_type,
        certainty=certainty,
    )
    item.cluster_key = build_news_cluster_key(item)
    return item


def best_news_item(items: Iterable[NewsItem]) -> Optional[NewsItem]:
    ranked = sorted(items, key=lambda x: (x.source_priority, x.confidence, -(x.age_minutes if x.age_minutes is not None else 9999)), reverse=True)
    return ranked[0] if ranked else None


def dedupe_and_rank_news(items: List[NewsItem]) -> List[NewsItem]:
    threshold = now_utc() - timedelta(minutes=NEWS_LOOKBACK_MINUTES)
    by_cluster: Dict[str, List[NewsItem]] = {}
    for item in items:
        if item.published_at is not None and item.published_at < threshold:
            continue
        if not item.fresh_enough or item.confidence < MIN_NEWS_CONFIDENCE or (not item.actions and not item.entities):
            continue
        by_cluster.setdefault(item.cluster_key, []).append(item)
    selected = [picked for picked in (best_news_item(v) for v in by_cluster.values()) if picked is not None]
    selected.sort(key=lambda x: (x.source_priority, x.confidence, -(x.age_minutes if x.age_minutes is not None else 9999)), reverse=True)
    return selected[:TOP_NEWS_PER_CYCLE]


# =========================
# NEWS FETCHERS
# =========================
def fetch_rss_feed(url: str, provider: str, session: Optional[requests.Session] = None) -> List[NewsItem]:
    http = session or requests
    try:
        response = http.get(url, timeout=REQUEST_TIMEOUT, headers=RSS_HTTP_HEADERS)
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except Exception as error:
        get_logger().warning("RSS fetch failed | url=%s | err=%s", url, error)
        return []

    items: List[NewsItem] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        if not title:
            continue
        items.append(
            make_news_item(
                title=title,
                link=(item.findtext("link") or "").strip(),
                description=(item.findtext("description") or "").strip(),
                published_at=parse_pub_date(item.findtext("pubDate")),
                source=(item.findtext("source") or "").strip(),
                provider=provider,
            )
        )
    return items


_google_rss_call_count = 0


def fetch_google_news_rss(session: Optional[requests.Session] = None) -> List[NewsItem]:
    global _google_rss_call_count
    if not ENABLE_GOOGLE_RSS:
        return []
    _google_rss_call_count += 1
    if GOOGLE_RSS_EVERY_N_CYCLES > 1 and (_google_rss_call_count % GOOGLE_RSS_EVERY_N_CYCLES) != 1:
        get_logger().debug("Google RSS skipped this cycle (%s/%s)", _google_rss_call_count, GOOGLE_RSS_EVERY_N_CYCLES)
        return []
    items: List[NewsItem] = []
    for idx, query in enumerate(GOOGLE_NEWS_QUERIES + REUTERS_GOOGLE_NEWS_QUERIES):
        if idx > 0 and GOOGLE_RSS_INTER_REQUEST_DELAY > 0:
            time.sleep(GOOGLE_RSS_INTER_REQUEST_DELAY)
        url = build_google_news_rss_url(query)
        if _CURL_CFFI_AVAILABLE:
            try:
                resp = curl_requests.get(url, impersonate="chrome110", timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                root = ET.fromstring(resp.content)
                for rss_item in root.findall(".//item"):
                    title = (rss_item.findtext("title") or "").strip()
                    if title:
                        items.append(make_news_item(
                            title=title,
                            link=(rss_item.findtext("link") or "").strip(),
                            description=(rss_item.findtext("description") or "").strip(),
                            published_at=parse_pub_date(rss_item.findtext("pubDate")),
                            source=(rss_item.findtext("source") or "").strip(),
                            provider="google_news_rss",
                        ))
                continue
            except Exception as err:
                get_logger().warning("Google RSS curl failed | url=%s | err=%s", url, err)
        items.extend(fetch_rss_feed(url, "google_news_rss", session=session))
    return items


def fetch_direct_rss(session: Optional[requests.Session] = None) -> List[NewsItem]:
    if not ENABLE_DIRECT_RSS:
        return []
    items: List[NewsItem] = []
    for url in direct_rss_feeds_resolved():
        items.extend(fetch_rss_feed(url, "direct_rss", session=session))
    return items


def fetch_newsapi_articles(session: Optional[requests.Session] = None) -> List[NewsItem]:
    if not should_fetch_newsapi():
        if ENABLE_NEWSAPI and has_newsapi_key() and NEWSAPI_EVERY_N_CYCLES > 1:
            get_logger().debug(
                "NewsAPI skipped this cycle (counter=%s, every %s)",
                BOT_CYCLE_COUNTER,
                NEWSAPI_EVERY_N_CYCLES,
            )
        return []
    http = session or requests
    key = newsapi_api_key()
    results: List[NewsItem] = []
    for query in newsapi_queries_resolved():
        params: Dict[str, Any] = {
            "q": query,
            "sortBy": NEWSAPI_SORT_BY,
            "pageSize": NEWSAPI_PAGE_SIZE,
            "from": newsapi_from_iso(),
        }
        if NEWSAPI_SEARCH_IN:
            params["searchIn"] = NEWSAPI_SEARCH_IN
        if NEWSAPI_TO:
            params["to"] = NEWSAPI_TO
        if NEWSAPI_LANGUAGE:
            params["language"] = NEWSAPI_LANGUAGE
        if NEWSAPI_SOURCES:
            params["sources"] = NEWSAPI_SOURCES
        else:
            dom = newsapi_domains_for_request()
            if dom:
                params["domains"] = dom
        try:
            response = http.get(NEWSAPI_BASE_URL, params=params, headers={"X-Api-Key": key}, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as error:
            get_logger().warning("NewsAPI HTTP error | q=%s | err=%s", query[:100], error)
            continue
        except Exception as error:
            get_logger().warning("NewsAPI error | q=%s | err=%s", query[:100], error)
            continue
        if not isinstance(data, dict) or data.get("status") != "ok":
            msg = data.get("message", "unknown") if isinstance(data, dict) else "invalid response"
            get_logger().warning("NewsAPI not ok | q=%s | %s", query[:80], msg)
            continue
        articles = data.get("articles", [])
        get_logger().info("NewsAPI ok | q=%s | articles=%s", query[:80], len(articles))
        for article in articles:
            if not isinstance(article, dict):
                continue
            title = str(article.get("title") or "").strip()
            if not title:
                continue
            source_obj = article.get("source", {}) if isinstance(article.get("source"), dict) else {}
            results.append(
                make_news_item(
                    title=title,
                    link=str(article.get("url") or ""),
                    description=str(article.get("description") or ""),
                    published_at=parse_iso_datetime(article.get("publishedAt")),
                    source=str(source_obj.get("name") or "NewsAPI"),
                    provider="newsapi",
                )
            )
    return results


def fetch_thenewsapi_articles(session: Optional[requests.Session] = None) -> List[NewsItem]:
    if not should_fetch_thenewsapi():
        if ENABLE_THENEWSAPI and has_thenewsapi_key() and THENEWSAPI_EVERY_N_CYCLES > 1:
            get_logger().debug(
                "TheNewsAPI skipped this cycle (counter=%s, every %s)",
                BOT_CYCLE_COUNTER,
                THENEWSAPI_EVERY_N_CYCLES,
            )
        return []
    http = session or requests
    results: List[NewsItem] = []
    for query in thenewsapi_queries_resolved():
        params: Dict[str, Any] = {
            "api_token": thenewsapi_api_key(),
            "search": query,
            "language": "en",
            "limit": THENEWSAPI_LIMIT,
            "sort": "published_at",
            "sort_direction": "desc",
        }
        if THENEWSAPI_DOMAINS:
            params["domains"] = THENEWSAPI_DOMAINS
        try:
            response = http.get(THENEWSAPI_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                provider_mark_429("thenewsapi", THENEWSAPI_BACKOFF_MINUTES)
                return results
            response.raise_for_status()
            raw_text = (response.text or "").strip()
            if not raw_text:
                get_logger().warning("TheNewsAPI empty response | q=%s", query[:80])
                continue
            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError as je:
                get_logger().warning("TheNewsAPI non-json | q=%s | err=%s", query[:80], je)
                continue
            provider_mark_success("thenewsapi")
        except requests.RequestException as error:
            get_logger().warning("TheNewsAPI HTTP error | q=%s | err=%s", query[:80], error)
            continue
        except Exception as error:
            get_logger().warning("TheNewsAPI error | q=%s | err=%s", query[:80], error)
            continue
        articles = data.get("data", []) if isinstance(data, dict) else []
        if not isinstance(articles, list):
            continue
        for article in articles:
            if not isinstance(article, dict):
                continue
            title = str(article.get("title") or "").strip()
            if not title:
                continue
            source_name = str(article.get("source") or "TheNewsAPI").strip()
            link = str(article.get("url") or "").strip()
            description = str(article.get("description") or "").strip()
            published_at = parse_iso_datetime(article.get("published_at"))
            results.append(
                make_news_item(
                    title=title,
                    link=link,
                    description=description,
                    published_at=published_at,
                    source=source_name,
                    provider="thenewsapi",
                )
            )
    return results


def fetch_gdelt_articles(session: Optional[requests.Session] = None) -> List[NewsItem]:
    if not should_fetch_gdelt():
        if ENABLE_GDELT and GDELT_EVERY_N_CYCLES > 1:
            get_logger().debug(
                "GDELT skipped this cycle (counter=%s, every %s)",
                BOT_CYCLE_COUNTER,
                GDELT_EVERY_N_CYCLES,
            )
        return []
    http = session or requests
    results: List[NewsItem] = []
    timespan = f"{max(15, NEWS_LOOKBACK_MINUTES)}min"
    gdelt_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    queries = gdelt_queries_resolved()
    for idx, query in enumerate(queries):
        if idx > 0 and GDELT_INTER_REQUEST_DELAY > 0:
            time.sleep(GDELT_INTER_REQUEST_DELAY)
        params = {
            "query": query,
            "mode": "artlist",
            "format": "json",
            "timespan": timespan,
            "sort": "datedesc",
            "maxrecords": GDELT_MAX_RECORDS,
        }
        try:
            response = http.get(gdelt_url, params=params, timeout=REQUEST_TIMEOUT)
            retried_after_429 = False
            if response.status_code == 429:
                get_logger().info(
                    "GDELT 429, backoff %.1fs then retry | q=%s",
                    GDELT_429_RETRY_DELAY,
                    query[:80],
                )
                time.sleep(GDELT_429_RETRY_DELAY)
                response = http.get(gdelt_url, params=params, timeout=REQUEST_TIMEOUT)
                retried_after_429 = True
            if response.status_code == 429:
                provider_mark_429("gdelt", GDELT_BACKOFF_MINUTES)
                return results
            response.raise_for_status()
            raw_text = (response.text or "").strip()
            # после 429 иногда приходит 200 с пустым телом — ещё одна пауза и запрос
            if not raw_text and retried_after_429 and GDELT_429_RETRY_DELAY > 0:
                get_logger().info(
                    "GDELT empty after 429 retry, backoff %.1fs then one more try | q=%s",
                    GDELT_429_RETRY_DELAY,
                    query[:80],
                )
                time.sleep(GDELT_429_RETRY_DELAY)
                response = http.get(gdelt_url, params=params, timeout=REQUEST_TIMEOUT)
                if response.status_code == 429:
                    provider_mark_429("gdelt", GDELT_BACKOFF_MINUTES)
                    return results
                response.raise_for_status()
                raw_text = (response.text or "").strip()
            if not raw_text:
                get_logger().warning("GDELT empty response | q=%s | status=%s", query[:80], response.status_code)
                continue
            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError as je:
                prefix = raw_text[:160].replace("\n", " ")
                get_logger().warning(
                    "GDELT non-json response | q=%s | err=%s | body_prefix=%s",
                    query[:80],
                    je,
                    prefix,
                )
                continue
            provider_mark_success("gdelt")
        except requests.RequestException as error:
            get_logger().warning("GDELT fetch failed | q=%s | err=%s", query[:80], error)
            continue
        except Exception as error:
            get_logger().warning("GDELT fetch failed | q=%s | err=%s", query[:80], error)
            continue
        articles = data.get("articles", []) if isinstance(data, dict) else []
        for article in articles:
            if not isinstance(article, dict):
                continue
            title = str(article.get("title") or "").strip()
            if not title:
                continue
            results.append(
                make_news_item(
                    title=title,
                    link=str(article.get("url") or ""),
                    description=str(article.get("domain") or ""),
                    published_at=parse_iso_datetime(article.get("seendate")),
                    source=str(article.get("domain") or "GDELT"),
                    provider="gdelt",
                )
            )
    return results


def fetch_recent_news(session: Optional[requests.Session] = None) -> List[NewsItem]:
    raw: List[NewsItem] = []
    raw.extend(fetch_google_news_rss(session=session))
    raw.extend(fetch_direct_rss(session=session))
    raw.extend(fetch_newsapi_articles(session=session))
    raw.extend(fetch_thenewsapi_articles(session=session))
    raw.extend(fetch_gdelt_articles(session=session))
    return dedupe_and_rank_news(raw)


# =========================
# POLYMARKET DISCOVERY
# =========================
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


def extract_deadline(text: str) -> str:
    lowered = normalize_text(text)
    patterns = [
        r"\bby\s+[a-z]+\s+\d{4}\b",
        r"\bbefore\s+[a-z]+\s+\d{4}\b",
        r"\bin\s+\d{4}\b",
        r"\bby\s+q[1-4]\s+\d{4}\b",
        r"\bbefore\s+[a-z]+\b",
        r"\bthis year\b",
        r"\bthis month\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            return match.group(0)
    return ""


def parse_yes_subject_object(question: str) -> Tuple[str, str]:
    lowered = normalize_text(question)
    if lowered.startswith("will "):
        core = lowered[5:]
        parts = core.split(" by ", 1)[0].split(" before ", 1)[0]
        return parts[:40], parts[40:80]
    return lowered[:40], ""


def classify_question_type(question: str) -> str:
    lowered = normalize_text(question)
    if "approve" in lowered or "approval" in lowered or "etf" in lowered:
        return "approval"
    if "reject" in lowered:
        return "rejection"
    if "delay" in lowered or "postpone" in lowered:
        return "delay"
    if "cut" in lowered or "hike" in lowered or "rates" in lowered:
        return "rates"
    if "election" in lowered or "president" in lowered or "senate" in lowered or "house" in lowered:
        return "election"
    if "recession" in lowered:
        return "recession"
    if "inflation" in lowered or "cpi" in lowered:
        return "inflation"
    if "launch" in lowered or "token" in lowered or "airdrop" in lowered:
        return "token_launch"
    return "binary"


def watchlist_allows(text: str) -> bool:
    if WATCHLIST_MODE == "broad":
        return True
    lowered = normalize_text(text)
    hits = sum(1 for term in WATCHLIST_TERMS if term in lowered)
    if WATCHLIST_MODE == "strict":
        return hits >= 2
    return hits >= 1


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
        return data if isinstance(data, list) else []
    except requests.RequestException as error:
        get_logger().error("Gamma /events failed: %s", error)
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
    volume = safe_float(event.get("volume24hr", event.get("volume_24hr", 0.0)))
    liquidity = safe_float(event.get("liquidity", 0.0))
    title = str(event.get("title") or event.get("slug") or "")
    return volume >= MIN_EVENT_VOLUME_24H and liquidity >= MIN_EVENT_LIQUIDITY and watchlist_allows(title)


def build_market_catalog(events: List[Dict[str, Any]]) -> List[MarketSemantics]:
    catalog: List[MarketSemantics] = []
    for event in events:
        if not passes_event_filters(event):
            continue
        event_title = str(event.get("title") or event.get("slug") or "").strip()
        event_slug = str(event.get("slug") or "").strip()
        event_volume = safe_float(event.get("volume24hr", event.get("volume_24hr", 0.0)))
        event_liquidity = safe_float(event.get("liquidity", 0.0))
        for market in extract_markets_from_event(event):
            question = str(market.get("question") or market.get("title") or "").strip()
            if not question or not watchlist_allows(f"{event_title} {question}"):
                continue

            outcomes_raw = parse_jsonish_list(market.get("outcomes"))
            prices_raw = parse_jsonish_list(market.get("outcomePrices"))
            token_ids_raw = parse_jsonish_list(market.get("clobTokenIds"))

            if not outcomes_raw and isinstance(market.get("outcomes"), list):
                for item in market.get("outcomes", []):
                    if isinstance(item, dict):
                        outcomes_raw.append(item.get("name") or item.get("outcome") or "")
                        prices_raw.append(item.get("price") or item.get("outcomePrice") or 0)
                        token_ids_raw.append(item.get("token_id") or item.get("tokenId") or "")

            names = [str(x).strip() for x in outcomes_raw]
            prices = [safe_float(x) for x in prices_raw]
            token_ids = [str(x).strip() for x in token_ids_raw]
            if not names:
                continue

            yes_index = 0
            no_index = 1 if len(names) > 1 else 0
            for idx, name in enumerate(names):
                if normalize_text(name) == "yes":
                    yes_index = idx
                elif normalize_text(name) == "no":
                    no_index = idx

            yes_actions = detect_action_tags(question) or detect_action_tags(event_title)
            yes_subject, yes_object = parse_yes_subject_object(question)
            catalog.append(
                MarketSemantics(
                    question=question,
                    slug=str(market.get("slug") or "").strip(),
                    event_title=event_title,
                    event_slug=event_slug,
                    event_volume_24h=event_volume,
                    event_liquidity=event_liquidity,
                    yes_token_id=token_ids[yes_index] if yes_index < len(token_ids) else "",
                    no_token_id=token_ids[no_index] if no_index < len(token_ids) else "",
                    yes_price_hint=prices[yes_index] if yes_index < len(prices) else 0.0,
                    no_price_hint=prices[no_index] if no_index < len(prices) else 0.0,
                    entities=extract_entities(f"{event_title} {question}"),
                    topics=keyword_topics(f"{event_title} {question}"),
                    yes_actions=yes_actions,
                    deadline=extract_deadline(question),
                    question_type=classify_question_type(question),
                    yes_subject=yes_subject,
                    yes_object=yes_object,
                )
            )
    return catalog


# =========================
# PRICE + MARKET CONTEXT
# =========================
def fetch_order_book(token_id: str, session: Optional[requests.Session] = None) -> Optional[Dict[str, Any]]:
    if not token_id:
        return None
    http = session or requests
    try:
        response = http.get(CLOB_BOOK_API, params={"token_id": token_id}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else None
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


def best_current_yes_price(yes_hint: float, book: Optional[Dict[str, Any]], history: List[Dict[str, Any]]) -> float:
    if isinstance(book, dict):
        bids = book.get("bids", []) if isinstance(book.get("bids"), list) else []
        asks = book.get("asks", []) if isinstance(book.get("asks"), list) else []
        best_bid = safe_float(bids[0].get("price")) if bids else 0.0
        best_ask = safe_float(asks[0].get("price")) if asks else 0.0
        last_trade = safe_float(book.get("last_trade_price"))
        if best_bid > 0 and best_ask > 0:
            return round((best_bid + best_ask) / 2.0, 4)
        if last_trade > 0:
            return round(last_trade, 4)
        if best_bid > 0:
            return round(best_bid, 4)
        if best_ask > 0:
            return round(best_ask, 4)
    if history:
        last_price = safe_float(history[-1].get("p"), 0.0)
        if last_price > 0:
            return round(last_price, 4)
    return round(yes_hint, 4)


def compute_order_book_metrics(book: Optional[Dict[str, Any]]) -> Tuple[float, float]:
    if not isinstance(book, dict):
        return 0.0, 0.0
    bids = book.get("bids", []) if isinstance(book.get("bids"), list) else []
    asks = book.get("asks", []) if isinstance(book.get("asks"), list) else []
    best_bid = safe_float(bids[0].get("price")) if bids else 0.0
    best_ask = safe_float(asks[0].get("price")) if asks else 0.0
    spread = max(0.0, best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 0.0

    bid_size = sum(safe_float(x.get("size")) for x in bids[:5])
    ask_size = sum(safe_float(x.get("size")) for x in asks[:5])
    total = bid_size + ask_size
    imbalance = ((bid_size - ask_size) / total) if total > 0 else 0.0
    return round(spread, 4), round(imbalance, 4)


def price_at_or_before(history: List[Dict[str, Any]], ts: int, fallback: float) -> float:
    chosen = fallback
    for point in history:
        point_ts = int(safe_float(point.get("t"), 0))
        if point_ts <= ts:
            chosen = safe_float(point.get("p"), chosen)
        else:
            break
    return round(chosen, 4)


def compute_recent_volatility(history: List[Dict[str, Any]]) -> float:
    prices = [safe_float(point.get("p"), 0.0) for point in history if safe_float(point.get("p"), 0.0) > 0]
    if len(prices) < 2:
        return 0.0
    moves = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    return round(average(moves), 4)


def build_price_context(news: NewsItem, market: MarketSemantics, session: Optional[requests.Session] = None) -> Optional[PriceContext]:
    book = fetch_order_book(market.yes_token_id, session=session)
    history = fetch_price_history(market.yes_token_id, session=session)
    current_yes = best_current_yes_price(market.yes_price_hint, book, history)
    if current_yes <= 0:
        return None

    now_ts = int(time.time())
    headline_ts = int(news.published_at.timestamp()) if isinstance(news.published_at, datetime) else now_ts
    pre_news_ts = max(headline_ts - 60, 0)
    old_context_ts = max(headline_ts - (PRE_NEWS_CONTEXT_MINUTES * 60), 0)

    pre_news_yes = price_at_or_before(history, pre_news_ts, current_yes)
    old_context_yes = price_at_or_before(history, old_context_ts, pre_news_yes)
    recent_volatility = compute_recent_volatility(history)
    spread, imbalance = compute_order_book_metrics(book)

    return PriceContext(
        current_yes=current_yes,
        pre_news_yes=pre_news_yes,
        old_context_yes=old_context_yes,
        recent_volatility=recent_volatility,
        spread=spread,
        imbalance=imbalance,
        actual_move_since_news=round(current_yes - pre_news_yes, 4),
        pre_news_drift=round(pre_news_yes - old_context_yes, 4),
    )


# =========================
# MATCHING + MODELS
# =========================
def direction_relation(news_actions: Set[str], market_yes_actions: Set[str]) -> str:
    if not news_actions or not market_yes_actions:
        return "none"
    if news_actions & market_yes_actions:
        return "same"
    for action in news_actions:
        if market_yes_actions & OPPOSITE_ACTIONS.get(action, set()):
            return "opposite"
    return "none"


def overlap_score(news_text: str, market_text: str) -> int:
    return len(set(tokenize(news_text)) & set(tokenize(market_text)))


def score_market_match(news: NewsItem, market: MarketSemantics) -> Tuple[int, str]:
    relation = direction_relation(news.actions, market.yes_actions)
    score = 0
    score += len(news.entities & market.entities) * 8
    score += len(news.topics & market.topics) * 4
    score += overlap_score(news.title, f"{market.event_title} {market.question}") * 2
    score += news.source_priority
    if relation == "same":
        score += 10
    elif relation == "opposite":
        score += 8
    if market.deadline:
        score += 1
    if news.event_type == "commentary":
        score -= 3
    return score, relation


def select_candidate_markets_for_news(news: NewsItem, catalog: List[MarketSemantics]) -> List[Tuple[MarketSemantics, int, str]]:
    scored: List[Tuple[MarketSemantics, int, str]] = []
    for market in catalog:
        score, relation = score_market_match(news, market)
        if score >= 10 and relation != "none":
            scored.append((market, score, relation))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:MAX_CANDIDATE_MARKETS_PER_NEWS]


def source_tier(source_name: str) -> int:
    lowered = normalize_text(source_name)
    if any(name in lowered for name in TIER1_SOURCES):
        return 3
    if "coindesk" in lowered or "the block" in lowered:
        return 2
    return 1


def event_type_move_multiplier(news: NewsItem) -> float:
    return {
        "final_decision": 1.3,
        "macro_release": 1.2,
        "procedural_update": 0.8,
        "poll_update": 0.7,
        "commentary": 0.5,
        "generic": 0.9,
    }.get(news.event_type, 0.9)


def market_family_base_move(market: MarketSemantics) -> float:
    return {
        "approval": 0.10,
        "rejection": 0.10,
        "delay": 0.08,
        "rates": 0.08,
        "election": 0.09,
        "recession": 0.07,
        "inflation": 0.07,
        "token_launch": 0.09,
        "binary": 0.06,
    }.get(market.question_type, 0.06)


def estimate_expected_delta(news: NewsItem, market: MarketSemantics, context: PriceContext, relation: str) -> float:
    base = market_family_base_move(market)
    base *= event_type_move_multiplier(news)
    base += 0.008 * source_tier(news.source)
    base += 0.01 * max(0.0, (news.confidence - 60) / 20.0)
    if relation == "same":
        base += 0.02
    elif relation == "opposite":
        base += 0.015
    if news.certainty == "high":
        base += 0.02
    elif news.certainty == "low":
        base -= 0.02
    if news.age_minutes is not None:
        freshness_mult = clamp(1.2 - (news.age_minutes / max(NEWS_MAX_AGE_MINUTES, 1)) * 0.5, 0.6, 1.2)
        base *= freshness_mult
    base += min(0.04, context.recent_volatility * 1.8)
    base -= min(0.05, abs(context.pre_news_drift) * 0.9)
    base -= min(0.03, context.spread * 0.5)
    if market.event_liquidity < 20000:
        base += 0.01
    if market.event_liquidity > 100000:
        base -= 0.005
    if market.deadline and "202" in market.deadline:
        base -= 0.005
    return round(clamp(base, 0.04, 0.30), 4)


def is_probably_priced_in(expected_delta: float, context: PriceContext, relation: str) -> Tuple[bool, str]:
    actual = context.actual_move_since_news
    if relation == "same" and actual >= expected_delta * ALREADY_PRICED_RATIO:
        return True, "market already reacted strongly in expected direction"
    if relation == "opposite" and actual <= -expected_delta * ALREADY_PRICED_RATIO:
        return True, "market already reacted strongly in expected direction"
    if abs(context.pre_news_drift) >= expected_delta * 0.75:
        return True, "large pre-news drift suggests information leak or priced-in action"
    return False, ""


def compute_trade_levels(trade_side: str, current_yes: float, remaining_move: float, expected_delta: float) -> Tuple[float, float, float]:
    signed_target_yes = clamp(current_yes + remaining_move * TP_CAPTURE_RATIO, 0.01, 0.99)
    risk_buffer = max(0.03, min(0.08, expected_delta * 0.55))
    if trade_side == "BUY YES":
        return round(current_yes, 2), round(signed_target_yes, 2), round(clamp(current_yes - risk_buffer, 0.01, 0.99), 2)
    current_no = 1.0 - current_yes
    target_no = 1.0 - signed_target_yes
    stop_no = clamp(current_no - risk_buffer, 0.01, 0.99)
    return round(current_no, 2), round(target_no, 2), round(stop_no, 2)


def evaluate_candidate(news: NewsItem, market: MarketSemantics, match_score: int, relation: str, session: Optional[requests.Session] = None) -> Optional[SignalCandidate]:
    context = build_price_context(news, market, session=session)
    if context is None:
        return None
    if context.spread > MAX_ALLOWED_SPREAD:
        return None

    expected_delta = estimate_expected_delta(news, market, context, relation)
    signed_expected = expected_delta if relation == "same" else -expected_delta
    actual_move = context.actual_move_since_news
    remaining_move = round(signed_expected - actual_move, 4)

    if abs(remaining_move) < MIN_REMAINING_MOVE:
        return None
    if abs(remaining_move) < MIN_ABSOLUTE_MISMATCH:
        return None

    priced_in, priced_reason = is_probably_priced_in(expected_delta, context, relation)
    if priced_in:
        return None

    trade_side = "BUY YES" if remaining_move > 0 else "BUY NO"
    entry, take_profit, stop = compute_trade_levels(trade_side, context.current_yes, remaining_move, expected_delta)

    confidence = 45
    confidence += news.source_priority * 6
    confidence += int((news.confidence - 50) * 0.4)
    confidence += min(20, len(news.entities & market.entities) * 5)
    confidence += min(15, len(news.topics & market.topics) * 4)
    confidence += min(15, int(match_score * 0.6))
    confidence += min(12, int(abs(remaining_move) * 100 * 0.7))
    confidence -= int(context.spread * 100 * 0.6)
    confidence -= int(abs(context.pre_news_drift) * 100 * 0.4)
    confidence = int(clamp(confidence, 0, 99))
    if confidence < MIN_SIGNAL_CONFIDENCE:
        return None

    reasons = [
        f"event_type={news.event_type}",
        f"certainty={news.certainty}",
        f"relation={relation}",
        f"news_confidence={news.confidence}",
        f"match_score={match_score}",
        f"source_priority={news.source_priority}",
        f"current_yes={context.current_yes:.2f}",
        f"pre_news_yes={context.pre_news_yes:.2f}",
        f"actual_move={context.actual_move_since_news:+.2f}",
        f"expected_delta={expected_delta:.2f}",
        f"remaining_move={remaining_move:+.2f}",
        f"pre_news_drift={context.pre_news_drift:+.2f}",
        f"spread={context.spread:.2f}",
        f"imbalance={context.imbalance:+.2f}",
    ]
    if priced_reason:
        reasons.append(priced_reason)

    return SignalCandidate(
        news=news,
        market=market,
        relation=relation,
        match_score=match_score,
        expected_delta=expected_delta,
        remaining_move=remaining_move,
        trade_side=trade_side,
        entry=entry,
        take_profit=take_profit,
        stop=stop,
        current_yes=round(context.current_yes, 2),
        pre_news_yes=round(context.pre_news_yes, 2),
        mismatch=round(abs(remaining_move), 2),
        confidence=confidence,
        reason="; ".join(reasons),
    )


# =========================
# SQLITE LOGGING + REPORTING
# =========================
def init_db(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            signal_key TEXT NOT NULL UNIQUE,
            cluster_key TEXT NOT NULL,
            headline TEXT NOT NULL,
            headline_link TEXT,
            source TEXT,
            provider TEXT,
            event_type TEXT,
            event_slug TEXT,
            event_title TEXT,
            market_question TEXT,
            market_family TEXT,
            token_id_yes TEXT,
            trade_side TEXT,
            entry REAL,
            take_profit REAL,
            stop REAL,
            current_yes REAL,
            pre_news_yes REAL,
            expected_delta REAL,
            remaining_move REAL,
            mismatch REAL,
            confidence INTEGER,
            reason TEXT,
            status TEXT DEFAULT 'open'
        )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL,
            captured_at TEXT NOT NULL,
            milestone_min INTEGER NOT NULL,
            current_yes REAL NOT NULL,
            note TEXT,
            UNIQUE(signal_id, milestone_min)
        )"""
        )
        conn.commit()
    finally:
        conn.close()


def db_connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def build_signal_key(signal: SignalCandidate) -> str:
    return f"{signal.news.cluster_key}|{signal.market.event_slug}|{signal.market.question}|{signal.trade_side}"


def was_recent_cluster_sent(cluster_key: str, db_path: str = DB_PATH) -> bool:
    cutoff = (now_utc() - timedelta(minutes=CLUSTER_COOLDOWN_MIN)).isoformat()
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM signals WHERE cluster_key = ? AND created_at >= ? LIMIT 1", (cluster_key, cutoff))
        return cur.fetchone() is not None
    finally:
        conn.close()


def was_recent_market_sent(event_slug: str, market_question: str, db_path: str = DB_PATH) -> bool:
    cutoff = (now_utc() - timedelta(minutes=MARKET_COOLDOWN_MIN)).isoformat()
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM signals WHERE event_slug = ? AND market_question = ? AND created_at >= ? LIMIT 1",
            (event_slug, market_question, cutoff),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def log_signal(signal: SignalCandidate, db_path: str = DB_PATH) -> Optional[int]:
    signal_key = build_signal_key(signal)
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT OR IGNORE INTO signals (
            created_at, signal_key, cluster_key, headline, headline_link, source, provider, event_type,
            event_slug, event_title, market_question, market_family, token_id_yes, trade_side, entry,
            take_profit, stop, current_yes, pre_news_yes, expected_delta, remaining_move, mismatch,
            confidence, reason, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
            (
                now_utc().isoformat(),
                signal_key,
                signal.news.cluster_key,
                signal.news.title,
                signal.news.link,
                signal.news.source,
                signal.news.provider,
                signal.news.event_type,
                signal.market.event_slug,
                signal.market.event_title,
                signal.market.question,
                signal.market.question_type,
                signal.market.yes_token_id,
                signal.trade_side,
                signal.entry,
                signal.take_profit,
                signal.stop,
                signal.current_yes,
                signal.pre_news_yes,
                signal.expected_delta,
                signal.remaining_move,
                signal.mismatch,
                signal.confidence,
                signal.reason,
            ),
        )
        conn.commit()
        return cur.lastrowid if cur.rowcount > 0 else None
    finally:
        conn.close()


def milestone_logged(signal_id: int, milestone_min: int, db_path: str = DB_PATH) -> bool:
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM snapshots WHERE signal_id = ? AND milestone_min = ? LIMIT 1", (signal_id, milestone_min))
        return cur.fetchone() is not None
    finally:
        conn.close()


def insert_snapshot(signal_id: int, milestone_min: int, current_yes: float, note: str, db_path: str = DB_PATH) -> None:
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO snapshots (signal_id, captured_at, milestone_min, current_yes, note) VALUES (?, ?, ?, ?, ?)",
            (signal_id, now_utc().isoformat(), milestone_min, current_yes, note),
        )
        conn.commit()
    finally:
        conn.close()


def get_open_signals(db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, created_at, token_id_yes, trade_side, entry, take_profit, stop, current_yes, status FROM signals WHERE status = 'open'")
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "id": row[0],
            "created_at": parse_iso_datetime(row[1]),
            "token_id_yes": row[2],
            "trade_side": row[3],
            "entry": safe_float(row[4]),
            "take_profit": safe_float(row[5]),
            "stop": safe_float(row[6]),
            "entry_yes": safe_float(row[7]),
            "status": row[8],
        }
        for row in rows
    ]


def update_signal_status(signal_id: int, status: str, db_path: str = DB_PATH) -> None:
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE signals SET status = ? WHERE id = ?", (status, signal_id))
        conn.commit()
    finally:
        conn.close()


def fetch_current_yes_from_token(token_id: str, session: Optional[requests.Session] = None) -> Optional[float]:
    book = fetch_order_book(token_id, session=session)
    history = fetch_price_history(token_id, session=session)
    price = best_current_yes_price(0.0, book, history)
    return price if price > 0 else None


def snapshot_note(trade_side: str, current_yes: float, entry: float, tp: float, stop: float) -> str:
    if trade_side == "BUY YES":
        if current_yes >= tp:
            return "tp_reached"
        if current_yes <= stop:
            return "stop_reached"
    else:
        current_no = 1.0 - current_yes
        if current_no >= tp:
            return "tp_reached"
        if current_no <= stop:
            return "stop_reached"
    return "open"


def update_open_signal_snapshots(session: Optional[requests.Session] = None, db_path: str = DB_PATH) -> None:
    for signal in get_open_signals(db_path):
        created_at = signal.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        age_min = (now_utc() - created_at).total_seconds() / 60.0
        current_yes = fetch_current_yes_from_token(signal["token_id_yes"], session=session)
        if current_yes is None:
            continue
        note = snapshot_note(signal["trade_side"], current_yes, signal["entry"], signal["take_profit"], signal["stop"])
        for milestone in SNAPSHOT_MILESTONES_MIN:
            if age_min >= milestone and not milestone_logged(signal["id"], milestone, db_path=db_path):
                insert_snapshot(signal["id"], milestone, current_yes, note, db_path=db_path)
        if age_min >= max(SNAPSHOT_MILESTONES_MIN):
            update_signal_status(signal["id"], "closed", db_path=db_path)
        elif note in {"tp_reached", "stop_reached"}:
            update_signal_status(signal["id"], note, db_path=db_path)


def print_report(db_path: str = DB_PATH) -> None:
    init_db(db_path)
    conn = db_connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM signals")
        total = cur.fetchone()[0]
        cur.execute("SELECT status, COUNT(*) FROM signals GROUP BY status")
        by_status = cur.fetchall()
        cur.execute("SELECT provider, COUNT(*), ROUND(AVG(confidence), 1) FROM signals GROUP BY provider ORDER BY COUNT(*) DESC")
        by_provider = cur.fetchall()
        cur.execute("SELECT market_family, COUNT(*), ROUND(AVG(confidence), 1) FROM signals GROUP BY market_family ORDER BY COUNT(*) DESC")
        by_family = cur.fetchall()
        cur.execute(
            "SELECT s.provider, snap.milestone_min, ROUND(AVG(ABS(snap.current_yes - s.current_yes)), 4) FROM signals s JOIN snapshots snap ON snap.signal_id = s.id GROUP BY s.provider, snap.milestone_min ORDER BY s.provider, snap.milestone_min"
        )
        avg_moves = cur.fetchall()
    finally:
        conn.close()

    print("\n=== BOT REPORT ===")
    print("Total signals:", total)
    print("\nBy status:")
    for status, count in by_status:
        print(f"  {status}: {count}")
    print("\nBy provider:")
    for provider, count, avg_conf in by_provider:
        print(f"  {provider}: {count} signals, avg confidence {avg_conf}")
    print("\nBy market family:")
    for family, count, avg_conf in by_family:
        print(f"  {family}: {count} signals, avg confidence {avg_conf}")
    print("\nAverage absolute move from signal price to snapshot:")
    for provider, milestone, avg_move in avg_moves:
        print(f"  {provider} @ {milestone}m: {avg_move}")


# =========================
# NOTIFICATIONS
# =========================
def format_signal(signal: SignalCandidate) -> str:
    market_link = f"https://polymarket.com/event/{signal.market.event_slug}" if signal.market.event_slug else ""
    age_text = f"{signal.news.age_minutes:.1f}m" if signal.news.age_minutes is not None else "unknown"
    return (
        "📰 NEWS-LAG SIGNAL V5\n\n"
        f"Headline: {signal.news.title}\n"
        f"Source: {signal.news.source}\n"
        f"Provider: {signal.news.provider}\n"
        f"Event type: {signal.news.event_type}\n"
        f"News confidence: {signal.news.confidence}/100\n"
        f"Age: {age_text}\n\n"
        f"Event: {signal.market.event_title}\n"
        f"Market: {signal.market.question}\n"
        f"Question family: {signal.market.question_type}\n"
        f"Deadline: {signal.market.deadline or 'n/a'}\n\n"
        f"Trade side: {signal.trade_side}\n"
        f"Entry: {signal.entry}\n"
        f"Take profit: {signal.take_profit}\n"
        f"Stop: {signal.stop}\n\n"
        f"Current YES: {signal.current_yes}\n"
        f"Pre-news YES: {signal.pre_news_yes}\n"
        f"Expected delta: {signal.expected_delta}\n"
        f"Remaining move: {signal.remaining_move}\n"
        f"Mismatch: {signal.mismatch}\n"
        f"Signal confidence: {signal.confidence}/99\n\n"
        f"Why: {signal.reason}\n"
        f"Polymarket: {market_link}\n"
        f"News: {signal.news.link}"
    )


def send_telegram_message(text: str, session: Optional[requests.Session] = None) -> bool:
    if get_runtime_mode() == "dry-run":
        print("\n--- DRY RUN MESSAGE ---")
        print(text)
        print("--- END MESSAGE ---\n")
        return True
    http = session or requests
    url = f"https://api.telegram.org/bot{telegram_token()}/sendMessage"
    payload = {"chat_id": chat_id(), "text": text, "disable_web_page_preview": True}
    try:
        response = http.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return bool(data.get("ok", False))
    except requests.RequestException as error:
        get_logger().error("Telegram HTTP error: %s", error)
        return False
    except Exception as error:
        get_logger().error("Telegram error: %s", error)
        return False


def send_test_message() -> None:
    log = get_logger()
    text = "🚀 Polymarket Signal Bot V5 connected. News-lag mode, logging, anti-spam and analytics are active."
    log.info("sending Telegram test message")
    tg = build_requests_session(_proxy_dict_for_prefix("TELEGRAM"))
    ok = send_telegram_message(text, session=tg)
    log.info("test message sent." if ok else "test message failed.")


def describe_active_sources() -> str:
    sources = []
    if ENABLE_GOOGLE_RSS:
        sources.append("Google News RSS")
    if ENABLE_DIRECT_RSS:
        sources.append("Direct RSS")
    if ENABLE_NEWSAPI and has_newsapi_key():
        sources.append("NewsAPI")
    if ENABLE_THENEWSAPI and has_thenewsapi_key():
        sources.append("TheNewsAPI")
    if ENABLE_GDELT:
        sources.append("GDELT (scheduled)")
    return ", ".join(sources) if sources else "none"


# =========================
# BOT ENGINE
# =========================
def find_news_driven_signals(
    polymarket_session: Optional[requests.Session] = None,
    news_session: Optional[requests.Session] = None,
    db_path: str = DB_PATH,
) -> List[SignalCandidate]:
    news_items = fetch_recent_news(session=news_session)
    if not news_items:
        get_logger().info("no fresh news this cycle")
        return []
    events = fetch_active_events(session=polymarket_session)
    if not events:
        get_logger().warning("no Polymarket events fetched")
        return []
    catalog = build_market_catalog(events)
    if not catalog:
        get_logger().info("no market catalog entries passed filters")
        return []

    signals: List[SignalCandidate] = []
    for news in news_items:
        if was_recent_cluster_sent(news.cluster_key, db_path=db_path):
            continue
        for market, match_score, relation in select_candidate_markets_for_news(news, catalog):
            if was_recent_market_sent(market.event_slug, market.question, db_path=db_path):
                continue
            candidate = evaluate_candidate(news, market, match_score, relation, session=polymarket_session)
            if candidate is not None:
                signals.append(candidate)
    signals.sort(key=lambda s: (s.confidence, s.mismatch, s.match_score), reverse=True)
    return signals


def run_bot() -> None:
    global BOT_CYCLE_COUNTER
    init_db()
    log = get_logger()
    log.info("runtime=%s | news sources: %s", get_runtime_mode(), describe_active_sources())

    tg_proxy = _proxy_dict_for_prefix("TELEGRAM")
    pm_proxy = _proxy_dict_for_prefix("GAMMA")
    news_proxy = _proxy_dict_for_prefix("NEWS")
    if tg_proxy:
        log.info("telegram requests use proxy (url not logged)")
    if pm_proxy:
        log.info("polymarket requests use proxy (url not logged)")
    if news_proxy:
        log.info("RSS/TheNewsAPI/NewsAPI/GDELT use proxy (url not logged)")

    telegram_session = build_requests_session(tg_proxy)
    polymarket_session = build_requests_session(pm_proxy)
    news_session = build_requests_session(news_proxy)

    loop_count = 0
    while True:
        BOT_CYCLE_COUNTER += 1
        loop_count = BOT_CYCLE_COUNTER
        log.info("cycle %s | updating signal snapshots", loop_count)
        update_open_signal_snapshots(session=polymarket_session)
        log.info("cycle %s | scanning news + markets", loop_count)
        signals = find_news_driven_signals(
            polymarket_session=polymarket_session,
            news_session=news_session,
        )
        if not signals:
            log.info("no tradable signals this cycle")
        for signal in signals:
            signal_id = log_signal(signal)
            if signal_id is None:
                continue
            send_telegram_message(format_signal(signal), session=telegram_session)
        if MAX_LOOPS > 0 and loop_count >= MAX_LOOPS:
            log.info("stopping after %s loop(s) (MAX_LOOPS=%s)", loop_count, MAX_LOOPS)
            break
        time.sleep(POLL_INTERVAL_SECONDS)


# =========================
# TESTS (network-free)
# =========================
def test_extract_entities() -> None:
    entities = extract_entities("SEC approved Ethereum ETF filing")
    assert "sec" in entities
    assert "ethereum" in entities
    assert "etf" in entities


def test_direction_relation() -> None:
    assert direction_relation({"approve"}, {"approve"}) == "same"
    assert direction_relation({"approve"}, {"reject"}) == "opposite"


def test_build_news_cluster_key() -> None:
    item = make_news_item("SEC approved Ethereum ETF filing", "https://example.com", "", now_utc(), "Reuters", "newsapi")
    assert "approve" in item.cluster_key
    assert ("ethereum" in item.cluster_key) or ("sec" in item.cluster_key)


def test_dedupe_and_rank_news_prefers_reuters() -> None:
    published = now_utc()
    a = make_news_item("SEC approved Ethereum ETF filing", "https://agg.example", "", published, "Aggregator", "google_news_rss")
    b = make_news_item("SEC approved Ethereum ETF filing", "https://www.reuters.com/x", "", published, "Reuters", "newsapi")
    picked = dedupe_and_rank_news([a, b])
    assert len(picked) == 1
    assert picked[0].source == "Reuters"


def test_classify_question_type() -> None:
    assert classify_question_type("Will SEC approve Ethereum ETF by June?") == "approval"
    assert classify_question_type("Will the Fed cut rates in June?") == "rates"


def test_score_market_match() -> None:
    news = make_news_item("SEC approved Ethereum ETF filing", "https://example.com", "", now_utc(), "Reuters", "newsapi")
    market = MarketSemantics(
        question="Will SEC approve Ethereum ETF by June?",
        slug="m1",
        event_title="Ethereum ETF",
        event_slug="ethereum-etf",
        event_volume_24h=10000,
        event_liquidity=20000,
        yes_token_id="yes",
        no_token_id="no",
        yes_price_hint=0.4,
        no_price_hint=0.6,
        entities={"sec", "ethereum", "etf"},
        topics={"crypto"},
        yes_actions={"approve"},
        deadline="by june",
        question_type="approval",
        yes_subject="sec approve ethereum etf",
        yes_object="",
    )
    score, relation = score_market_match(news, market)
    assert relation == "same"
    assert score >= 20


def test_evaluate_candidate_buy_yes() -> None:
    published = now_utc() - timedelta(minutes=2)
    news = make_news_item("SEC approved Ethereum ETF filing", "https://example.com", "", published, "Reuters", "newsapi")
    market = MarketSemantics(
        question="Will SEC approve Ethereum ETF by June?",
        slug="m1",
        event_title="Ethereum ETF",
        event_slug="ethereum-etf",
        event_volume_24h=10000,
        event_liquidity=20000,
        yes_token_id="yes",
        no_token_id="no",
        yes_price_hint=0.41,
        no_price_hint=0.59,
        entities={"sec", "ethereum", "etf"},
        topics={"crypto"},
        yes_actions={"approve"},
        deadline="by june",
        question_type="approval",
        yes_subject="sec approve ethereum etf",
        yes_object="",
    )
    now_ts = int(time.time())

    class DummySession:
        def get(self, url, params=None, headers=None, timeout=None):
            class Resp:
                def __init__(self, payload):
                    self._payload = payload
                def raise_for_status(self):
                    return None
                def json(self):
                    return self._payload
            if "book" in url:
                return Resp({"last_trade_price": "0.41", "bids": [{"price": "0.40", "size": "100"}], "asks": [{"price": "0.42", "size": "100"}]})
            return Resp({"history": [{"t": now_ts - 1800, "p": 0.39}, {"t": now_ts - 600, "p": 0.39}, {"t": now_ts - 120, "p": 0.40}, {"t": now_ts - 60, "p": 0.40}, {"t": now_ts, "p": 0.41}]})

    score, relation = score_market_match(news, market)
    signal = evaluate_candidate(news, market, score, relation, session=DummySession())
    assert signal is not None
    assert signal.trade_side == "BUY YES"


def test_evaluate_candidate_buy_no() -> None:
    published = now_utc() - timedelta(minutes=2)
    news = make_news_item("SEC rejected Ethereum ETF filing", "https://example.com", "", published, "Reuters", "newsapi")
    market = MarketSemantics(
        question="Will SEC approve Ethereum ETF by June?",
        slug="m1",
        event_title="Ethereum ETF",
        event_slug="ethereum-etf",
        event_volume_24h=10000,
        event_liquidity=20000,
        yes_token_id="yes",
        no_token_id="no",
        yes_price_hint=0.70,
        no_price_hint=0.30,
        entities={"sec", "ethereum", "etf"},
        topics={"crypto"},
        yes_actions={"approve"},
        deadline="by june",
        question_type="approval",
        yes_subject="sec approve ethereum etf",
        yes_object="",
    )
    now_ts = int(time.time())

    class DummySession:
        def get(self, url, params=None, headers=None, timeout=None):
            class Resp:
                def __init__(self, payload):
                    self._payload = payload
                def raise_for_status(self):
                    return None
                def json(self):
                    return self._payload
            if "book" in url:
                return Resp({"last_trade_price": "0.70", "bids": [{"price": "0.69", "size": "100"}], "asks": [{"price": "0.71", "size": "100"}]})
            return Resp({"history": [{"t": now_ts - 1800, "p": 0.70}, {"t": now_ts - 600, "p": 0.70}, {"t": now_ts - 60, "p": 0.70}, {"t": now_ts, "p": 0.70}]})

    score, relation = score_market_match(news, market)
    signal = evaluate_candidate(news, market, score, relation, session=DummySession())
    assert signal is not None
    assert signal.trade_side == "BUY NO"


def test_init_db_and_log_signal() -> None:
    test_db = "test_polymarket_bot_v5.sqlite3"
    # чистый файл — иначе INSERT OR IGNORE даёт rowcount=0 при повторном запуске тестов
    if os.path.isfile(test_db):
        os.remove(test_db)
    init_db(test_db)
    news = make_news_item("SEC approved Ethereum ETF filing", "https://example.com", "", now_utc(), "Reuters", "newsapi")
    market = MarketSemantics(
        question="Will SEC approve Ethereum ETF by June?",
        slug="m1",
        event_title="Ethereum ETF",
        event_slug="ethereum-etf",
        event_volume_24h=10000,
        event_liquidity=20000,
        yes_token_id="yes",
        no_token_id="no",
        yes_price_hint=0.41,
        no_price_hint=0.59,
        entities={"sec", "ethereum", "etf"},
        topics={"crypto"},
        yes_actions={"approve"},
        deadline="by june",
        question_type="approval",
        yes_subject="sec approve ethereum etf",
        yes_object="",
    )
    signal = SignalCandidate(news, market, "same", 20, 0.12, 0.10, "BUY YES", 0.41, 0.49, 0.36, 0.41, 0.40, 0.10, 80, "test")
    signal_id = log_signal(signal, db_path=test_db)
    assert signal_id is not None


def run_tests() -> None:
    test_extract_entities()
    test_direction_relation()
    test_build_news_cluster_key()
    test_dedupe_and_rank_news_prefers_reuters()
    test_classify_question_type()
    test_score_market_match()
    test_evaluate_candidate_buy_yes()
    test_evaluate_candidate_buy_no()
    test_init_db_and_log_signal()
    get_logger().info("all tests passed | version=%s", VERSION)


if __name__ == "__main__":
    log_startup_banner()
    try:
        if MODE == "run_tests":
            run_tests()
        elif MODE == "test_message":
            init_db()
            send_test_message()
        elif MODE == "report":
            init_db()
            print_report()
        else:
            run_bot()
    except KeyboardInterrupt:
        get_logger().info("bot stopped by user | version=%s", VERSION)
        sys.exit(0)
