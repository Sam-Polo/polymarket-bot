Замени верхний блок конфига

TELEGRAM_TOKEN = "YOUR_TELEGRAM_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"

# NewsAPI free for live mode is not recommended.
NEWSAPI_API_KEY = ""
THENEWSAPI_API_KEY = "YOUR_THENEWSAPI_KEY"

MODE = "bot"              # "run_tests", "test_message", "bot", "report"
DRY_RUN = False
MAX_LOOPS = 0
POLL_INTERVAL_SECONDS = 180
REQUEST_TIMEOUT = 20

# Storage
DB_PATH = "polymarket_bot_v5.sqlite3"
SNAPSHOT_MILESTONES_MIN = [5, 15, 30, 60]
MARKET_COOLDOWN_MIN = 20
CLUSTER_COOLDOWN_MIN = 15

# News sources
ENABLE_GOOGLE_RSS = True
ENABLE_DIRECT_RSS = True
ENABLE_NEWSAPI = False
ENABLE_THENEWSAPI = True
ENABLE_GDELT = True
ONLY_REUTERS_NEWSAPI = False

NEWS_LOOKBACK_MINUTES = 45
NEWS_MAX_AGE_MINUTES = 12
TOP_NEWS_PER_CYCLE = 40
MIN_NEWS_CONFIDENCE = 55

# Provider throttling / scheduling
THENEWSAPI_FETCH_EVERY_N_CYCLES = 2
GDELT_FETCH_EVERY_N_CYCLES = 5
GDELT_BACKOFF_MINUTES = 30
THENEWSAPI_BACKOFF_MINUTES = 15




Сразу после блока с GDELT_QUERIES

THENEWSAPI_QUERIES = [
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

THENEWSAPI_LIMIT = 3
THENEWSAPI_DOMAINS = "reuters.com,bloomberg.com,cnbc.com,coindesk.com,theblock.co"



Рядом с has_newsapi_key


def has_thenewsapi_key() -> bool:
    return THENEWSAPI_API_KEY.strip() != "" and THENEWSAPI_API_KEY != "YOUR_THENEWSAPI_KEY"


После helper функции now_utcsafe_float



PROVIDER_STATE: Dict[str, Dict[str, Any]] = {
    "gdelt": {"429_count": 0, "skip_until": None},
    "thenewsapi": {"429_count": 0, "skip_until": None},
}

BOT_CYCLE_COUNTER = 0


def provider_skip_active(provider_name: str) -> bool:
    state = PROVIDER_STATE.get(provider_name, {})
    skip_until = state.get("skip_until")
    return isinstance(skip_until, datetime) and now_utc() < skip_until


def provider_mark_429(provider_name: str, backoff_minutes: int) -> None:
    state = PROVIDER_STATE.setdefault(provider_name, {"429_count": 0, "skip_until": None})
    state["429_count"] = int(state.get("429_count", 0)) + 1
    state["skip_until"] = now_utc() + timedelta(minutes=backoff_minutes)


def provider_mark_success(provider_name: str) -> None:
    state = PROVIDER_STATE.setdefault(provider_name, {"429_count": 0, "skip_until": None})
    state["429_count"] = 0
    state["skip_until"] = None


def should_fetch_thenewsapi() -> bool:
    if not ENABLE_THENEWSAPI or not has_thenewsapi_key():
        return False
    if provider_skip_active("thenewsapi"):
        return False
    return BOT_CYCLE_COUNTER % THENEWSAPI_FETCH_EVERY_N_CYCLES == 0


def should_fetch_gdelt() -> bool:
    if not ENABLE_GDELT:
        return False
    if provider_skip_active("gdelt"):
        return False
    return BOT_CYCLE_COUNTER % GDELT_FETCH_EVERY_N_CYCLES == 



полностью замени функцию fetch_newsapi_articles


def fetch_newsapi_articles(session: Optional[requests.Session] = None) -> List[NewsItem]:
    # Disabled in v5.1 for live mode because free NewsAPI is not suitable for low-latency scanning.
    return []



Добавь новую функцию fetch_thenewsapi_articles


def fetch_thenewsapi_articles(session: Optional[requests.Session] = None) -> List[NewsItem]:
    if not should_fetch_thenewsapi():
        return []

    http = session or requests
    results: List[NewsItem] = []

    for query in THENEWSAPI_QUERIES:
        params = {
            "api_token": THENEWSAPI_API_KEY,
            "search": query,
            "language": "en",
            "limit": THENEWSAPI_LIMIT,
            "domains": THENEWSAPI_DOMAINS,
            "sort": "published_at",
            "sort_direction": "desc",
        }

        try:
            response = http.get(
                "https://api.thenewsapi.com/v1/news/all",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                provider_mark_429("thenewsapi", THENEWSAPI_BACKOFF_MINUTES)
                print("TheNewsAPI returned 429. Backing off.")
                return results

            response.raise_for_status()
            data = response.json()
            provider_mark_success("thenewsapi")

        except Exception as error:
            print(f"Error fetching TheNewsAPI for query {query}: {error}")
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



Полностью замени fetch_gdelt_articles




def fetch_gdelt_articles(session: Optional[requests.Session] = None) -> List[NewsItem]:
    if not should_fetch_gdelt():
        return []

    http = session or requests
    results: List[NewsItem] = []
    timespan = "15min"

    reduced_queries = [
        '"SEC" AND (ETF OR bitcoin OR ethereum)',
        '"Federal Reserve" AND (rates OR inflation OR recession)',
    ]

    for query in reduced_queries:
        params = {
            "query": query,
            "mode": "artlist",
            "format": "json",
            "timespan": timespan,
            "sort": "datedesc",
            "maxrecords": 10,
        }

        try:
            response = http.get(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                provider_mark_429("gdelt", GDELT_BACKOFF_MINUTES)
                print("GDELT returned 429. Backing off.")
                return results

            response.raise_for_status()
            data = response.json()
            provider_mark_success("gdelt")

        except Exception as error:
            print(f"Error fetching GDELT for query {query}: {error}")
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



Полностью замени fetch_recent_news


def fetch_recent_news(session: Optional[requests.Session] = None) -> List[NewsItem]:
    raw: List[NewsItem] = []

    # Fast / cheap sources every cycle
    raw.extend(fetch_google_news_rss(session=session))
    raw.extend(fetch_direct_rss(session=session))

    # Scheduled API sources
    raw.extend(fetch_thenewsapi_articles(session=session))
    raw.extend(fetch_gdelt_articles(session=session))

    return dedupe_and_rank_news(raw)



Полностью замени describe_active_sources


def describe_active_sources() -> str:
    sources = []
    if ENABLE_GOOGLE_RSS:
        sources.append("Google News RSS")
    if ENABLE_DIRECT_RSS:
        sources.append("Direct RSS")
    if ENABLE_THENEWSAPI and has_thenewsapi_key():
        sources.append("TheNewsAPI")
    if ENABLE_GDELT:
        sources.append("GDELT (scheduled)")
    return ", ".join(sources) if sources else "none"



Полностью замени run_bot



def run_bot() -> None:
    global BOT_CYCLE_COUNTER

    init_db()
    print(f"Starting bot in {get_runtime_mode()} mode.")
    print("Active news sources:", describe_active_sources())

    loop_count = 0
    while True:
        loop_count += 1
        BOT_CYCLE_COUNTER += 1

        print("Updating logged signal snapshots...")
        update_open_signal_snapshots()

        print("Scanning fresh headlines and matching Polymarket markets...")
        signals = find_news_driven_signals()

        if not signals:
            print("No tradable signals found this cycle.")

        for signal in signals:
            signal_id = log_signal(signal)
            if signal_id is None:
                continue
            send_telegram_message(format_signal(signal))

        if MAX_LOOPS > 0 and loop_count >= MAX_LOOPS:
            print(f"Stopping after {loop_count} loop(s).")
            break

        time.sleep(POLL_INTERVAL_SECONDS)