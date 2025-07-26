import yfinance as yf
import pymongo
import schedule
import time

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock_db"]

def fetch_and_store(ticker: str):
    df = yf.download(ticker, start="2024-01-01", end="2025-07-24")
    records = df.reset_index().to_dict("records")
    for r in records:
        r["ticker"] = ticker
    db.prices.insert_many(records)
    print(f"{ticker} data stored.")

def job():
    for t in ["AAPL", "MSFT", "GOOGL"]:
        fetch_and_store(t)

schedule.every().day.at("01:00").do(job)

if __name__ == "__main__":
    job()  # 초기 실행
    while True:
        schedule.run_pending()
        time.sleep(60)
