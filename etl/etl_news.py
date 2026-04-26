"""
ETL Pipeline 2: News Articles → MongoDB
Extracts financial news from NewsAPI, tags with topics and tickers, loads into MongoDB.
"""

import os
import requests
from pymongo import MongoClient

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")

SEARCH_TERMS = [
    # Market / macro
    "federal reserve interest rates",
    "S&P 500 stock market",
    "inflation CPI economy",
    "stock market rally selloff",
    "nasdaq dow jones",
    # Apple
    "Apple AAPL stock",
    "Apple earnings revenue",
    "Apple iPhone iPad",
    "Apple CEO Tim Cook",
    "AAPL investor",
    # Microsoft
    "Microsoft MSFT stock",
    "Microsoft earnings revenue",
    "Microsoft Azure cloud",
    "Microsoft AI Copilot",
    "MSFT investor",
    # Google / Alphabet
    "Google Alphabet GOOGL stock",
    "Google earnings revenue",
    "Google AI Gemini",
    "Alphabet advertising",
    "GOOGL investor",
    # Amazon
    "Amazon AMZN stock",
    "Amazon earnings revenue",
    "Amazon AWS cloud",
    "Amazon retail ecommerce",
    "AMZN investor",
    # Tesla
    "Tesla TSLA stock",
    "Tesla earnings revenue",
    "Tesla EV electric vehicle",
    "Tesla CEO Elon Musk",
    "TSLA investor",
]

KEYWORD_MAP = {
    "fed": ["federal reserve", "fomc", "fed "],
    "interest-rates": ["interest rate", "rate hike", "rate cut"],
    "inflation": ["inflation", "cpi", "consumer price"],
    "oil": ["crude oil", "opec", "petroleum"],
    "housing": ["mortgage", "housing", "real estate"],
    "stocks": ["s&p 500", "nasdaq", "dow jones", "stock market"],
    "earnings": ["earnings", "revenue", "profit", "quarterly results"],
}

TICKER_MAP = {
    "AAPL": ["apple", "aapl"],
    "MSFT": ["microsoft", "msft"],
    "GOOGL": ["google", "alphabet", "googl"],
    "AMZN": ["amazon", "amzn"],
    "TSLA": ["tesla", "tsla"],
}


def get_mongo_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["financial_market"]
    return db["scraped_news"]


def tag_article(headline, content):
    """Assign topic tags based on keyword matches."""
    text = (headline + " " + content).lower()
    tags = []
    for tag, keywords in KEYWORD_MAP.items():
        if any(kw in text for kw in keywords):
            tags.append(tag)
    return tags


def tag_tickers(headline, content):
    """Identify which tracked tickers are mentioned."""
    text = (headline + " " + content).lower()
    found = []
    for ticker, keywords in TICKER_MAP.items():
        if any(kw in text for kw in keywords):
            found.append(ticker)
    return found


def fetch_articles(query):
    """Fetch articles from NewsAPI for a given search term."""
    response = requests.get(
        "https://newsapi.org/v2/everything",
        params={
            "q": query,
            "apiKey": NEWSAPI_KEY,
            "language": "en",
            "sortBy": "relevancy",
            "pageSize": 20,
        },
    )
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "ok":
        print(f"  API error for '{query}': {data.get('message', 'unknown error')}")
        return []

    return data.get("articles", [])


def main():
    if not NEWSAPI_KEY:
        print("ERROR: NEWSAPI_KEY is not set. Add it to your .env file.")
        raise SystemExit(1)

    collection = get_mongo_collection()

    # Create unique index on URL to prevent duplicates across runs
    collection.create_index("url", unique=True, sparse=True)

    # Load existing URLs to skip duplicates within this run
    seen_urls = set(
        doc["url"] for doc in collection.find({}, {"url": 1}) if doc.get("url")
    )
    total_inserted = 0

    for term in SEARCH_TERMS:
        print(f"Fetching articles for: '{term}'...")
        articles = fetch_articles(term)

        for article in articles:
            url = article.get("url", "")

            if url in seen_urls:
                continue
            seen_urls.add(url)

            headline = article.get("title") or ""
            content = article.get("description") or ""

            if not headline and not content:
                continue

            doc = {
                "headline": headline,
                "content": content,
                "source": article.get("source", {}).get("name", "unknown"),
                "date": article.get("publishedAt", ""),
                "url": url,
                "tickers": tag_tickers(headline, content),
                "tags": tag_article(headline, content),
            }

            collection.insert_one(doc)
            total_inserted += 1

        print(f"  {len(articles)} fetched, {total_inserted} total inserted so far.")

    print(f"\nETL Pipeline 2 complete: {total_inserted} articles loaded into MongoDB.")


if __name__ == "__main__":
    main()
