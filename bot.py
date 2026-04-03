import os
import sys
import time
from typing import Any, Dict, List, Optional, Set

import requests

# ===== CONFIG =====
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
POLYMARKET_API = os.getenv("POLYMARKET_API", "https://gamma-api.polymarket.com/markets")
REQUEST_TIMEOUT = 20
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
MAX_LOOPS = int(os.getenv("MAX_LOOPS", "0") or "0")  # 0 = infinite


def get_runtime_mode() -> str:
    """
    Return runtime mode: live or dry-run.

    If required Telegram credentials are missing, the bot automatically falls back
    to dry-run mode instead of raising an exception.
    """
    if DRY_RUN:
        return "dry-run"
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return "dry-run"
    return "live"


RUNTIME_MODE = get_runtime_mode()


def print_startup_help() -> None:
    """Print startup information and setup hints."""
    print(f"Starting bot in {RUNTIME_MODE} mode.")
    if RUNTIME_MODE == "dry-run":
        print(
            "Telegram credentials are missing or DRY_RUN=1, so signals will be printed to the console instead of being sent to Telegram."
        )
        if not TELEGRAM_TOKEN:
            print("Hint: set TELEGRAM_TOKEN to enable live Telegram messages.")
        if not CHAT_ID:
            print("Hint: set CHAT_ID to enable live Telegram messages.")


def get_markets(session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """
    Fetch markets from the Polymarket API.

    Returns an empty list on error or if the response is not a list.
    """
    http = session or requests
    try:
        res = http.get(POLYMARKET_API, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        data = res.json()
        if isinstance(data, list):
            return data
        print("Unexpected API response type:", type(data).__name__)
        return []
    except Exception as e:
        print("Error fetching markets:", e)
        return []


def _extract_outcomes(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize market outcomes.

    Some APIs return outcomes as a list of dicts, while others may return a JSON string
    or omit it entirely. This bot only accepts list[dict].
    """
    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, list):
        return [o for o in outcomes if isinstance(o, dict)]
    return []


def analyze_market(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate simple trading signals from a single market."""
    signals: List[Dict[str, Any]] = []

    try:
        title = str(market.get("question", "")).strip() or "Untitled market"
        outcomes = _extract_outcomes(market)

        for outcome in outcomes:
            try:
                price = float(outcome.get("price", 0))
            except (TypeError, ValueError):
                continue

            # Ignore impossible or junk values
            if price <= 0 or price >= 1:
                continue

            # Strategy 1: Overpriced (>80%)
            if price > 0.80:
                signals.append(
                    {
                        "type": "SHORT",
                        "title": title,
                        "price": price,
                        "entry": round(price, 2),
                        "exit": round(max(price - 0.20, 0.01), 2),
                        "reason": "probability imbalance: price above 0.80",
                    }
                )

            # Strategy 2: Mid range (possible value zone)
            if 0.40 < price < 0.60:
                signals.append(
                    {
                        "type": "MID",
                        "title": title,
                        "price": price,
                        "entry": round(price, 2),
                        "exit": round(min(price + 0.20, 0.99), 2),
                        "reason": "probability imbalance: price between 0.40 and 0.60",
                    }
                )

    except Exception as e:
        print("Error analyzing market:", e)

    return signals


def format_signal(signal: Dict[str, Any]) -> str:
    """Format a signal message for Telegram or console output."""
    return (
        "📢 SIGNAL\n\n"
        f"Market: {signal['title']}\n"
        f"Type: {signal['type']}\n\n"
        f"Entry: {signal['entry']}\n"
        f"Exit: {signal['exit']}\n\n"
        f"Reason: {signal['reason']}"
    )


def send_telegram_message(text: str, session: Optional[requests.Session] = None) -> bool:
    """
    Send a Telegram message using the raw Telegram Bot HTTP API.

    In dry-run mode, prints the message and returns True.
    """
    if RUNTIME_MODE == "dry-run":
        print("\n--- DRY RUN MESSAGE ---")
        print(text)
        print("--- END MESSAGE ---\n")
        return True

    http = session or requests
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}

    try:
        res = http.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        data = res.json()
        if not data.get("ok", False):
            print("Telegram API returned an error:", data)
            return False
        return True
    except Exception as e:
        print("Error sending Telegram message:", e)
        return False


def send_signal(signal: Dict[str, Any], session: Optional[requests.Session] = None) -> bool:
    """Format and send a trading signal."""
    msg = format_signal(signal)
    return send_telegram_message(msg, session=session)


def build_signal_key(signal: Dict[str, Any]) -> str:
    """Build a stable deduplication key for a signal."""
    return f"{signal['title']}|{signal['type']}|{signal['entry']}"


def run_bot(poll_interval_seconds: int = 60) -> None:
    """
    Main loop.

    In dry-run mode, signals are printed to stdout. In live mode, they are sent to Telegram.
    Use MAX_LOOPS environment variable for finite runs during local testing.
    """
    print_startup_help()
    seen: Set[str] = set()
    loop_count = 0

    while True:
        loop_count += 1
        print("Checking markets...")
        markets = get_markets()

        for market in markets:
            signals = analyze_market(market)

            for signal in signals:
                key = build_signal_key(signal)
                if key not in seen:
                    sent = send_signal(signal)
                    if sent:
                        seen.add(key)

        if MAX_LOOPS > 0 and loop_count >= MAX_LOOPS:
            print(f"Stopping after {loop_count} loop(s) because MAX_LOOPS={MAX_LOOPS}.")
            break

        time.sleep(poll_interval_seconds)


# ===== SIMPLE TESTS =====

def _test_analyze_market_short_signal() -> None:
    market = {
        "question": "Will token X reach Y?",
        "outcomes": [{"price": "0.82"}],
    }
    signals = analyze_market(market)
    assert len(signals) == 1
    assert signals[0]["type"] == "SHORT"
    assert signals[0]["entry"] == 0.82
    assert signals[0]["exit"] == 0.62


def _test_analyze_market_mid_signal() -> None:
    market = {
        "question": "Will event Z happen?",
        "outcomes": [{"price": "0.50"}],
    }
    signals = analyze_market(market)
    assert len(signals) == 1
    assert signals[0]["type"] == "MID"
    assert signals[0]["entry"] == 0.5
    assert signals[0]["exit"] == 0.7


def _test_analyze_market_ignores_bad_prices() -> None:
    market = {
        "question": "Bad market",
        "outcomes": [
            {"price": "abc"},
            {"price": 0},
            {"price": 1},
            {"price": -0.1},
        ],
    }
    signals = analyze_market(market)
    assert signals == []


def _test_format_signal() -> None:
    signal = {
        "title": "Sample market",
        "type": "SHORT",
        "entry": 0.82,
        "exit": 0.62,
        "reason": "probability imbalance: price above 0.80",
    }
    text = format_signal(signal)
    assert "📢 SIGNAL" in text
    assert "Sample market" in text
    assert "Entry: 0.82" in text


def _test_runtime_mode_defaults_to_dry_run_without_credentials() -> None:
    global TELEGRAM_TOKEN, CHAT_ID, DRY_RUN, RUNTIME_MODE
    old_token, old_chat, old_dry, old_mode = TELEGRAM_TOKEN, CHAT_ID, DRY_RUN, RUNTIME_MODE
    try:
        TELEGRAM_TOKEN = ""
        CHAT_ID = ""
        DRY_RUN = False
        RUNTIME_MODE = get_runtime_mode()
        assert RUNTIME_MODE == "dry-run"
    finally:
        TELEGRAM_TOKEN, CHAT_ID, DRY_RUN, RUNTIME_MODE = old_token, old_chat, old_dry, old_mode


def _test_runtime_mode_live_with_credentials() -> None:
    global TELEGRAM_TOKEN, CHAT_ID, DRY_RUN, RUNTIME_MODE
    old_token, old_chat, old_dry, old_mode = TELEGRAM_TOKEN, CHAT_ID, DRY_RUN, RUNTIME_MODE
    try:
        TELEGRAM_TOKEN = "token"
        CHAT_ID = "123"
        DRY_RUN = False
        RUNTIME_MODE = get_runtime_mode()
        assert RUNTIME_MODE == "live"
    finally:
        TELEGRAM_TOKEN, CHAT_ID, DRY_RUN, RUNTIME_MODE = old_token, old_chat, old_dry, old_mode


def _test_send_telegram_message_dry_run_returns_true() -> None:
    global RUNTIME_MODE
    old_mode = RUNTIME_MODE
    try:
        RUNTIME_MODE = "dry-run"
        assert send_telegram_message("hello") is True
    finally:
        RUNTIME_MODE = old_mode


def run_tests() -> None:
    _test_analyze_market_short_signal()
    _test_analyze_market_mid_signal()
    _test_analyze_market_ignores_bad_prices()
    _test_format_signal()
    _test_runtime_mode_defaults_to_dry_run_without_credentials()
    _test_runtime_mode_live_with_credentials()
    _test_send_telegram_message_dry_run_returns_true()
    print("All tests passed.")


if __name__ == "__main__":
    if os.getenv("RUN_TESTS") == "1":
        run_tests()
    else:
        try:
            run_bot()
        except KeyboardInterrupt:
            print("Bot stopped by user.")
            sys.exit(0)