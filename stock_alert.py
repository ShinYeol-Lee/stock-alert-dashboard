import asyncio
from telethon.sync import TelegramClient
import pandas as pd
from konlpy.tag import Okt
from collections import Counter
import sqlite3
import schedule
import time
from datetime import datetime, timedelta
import streamlit as st
from transformers import pipeline
import plotly.express as px
import re

# 텔레그램 설정
api_id = "16409037"  # my.telegram.org에서 발급받은 ID
api_hash = "0a22b7402b242338fcc9f9f904a91e5f"  # my.telegram.org에서 발급받은 Hash
phone = "+821093343869"  # 전화번호 (+8210xxxx)
channels = ["@econostudy", "@pengmeup", "@daegurr"]

# 데이터 준비
stocks = pd.read_csv("stocks.csv")
stock_names = stocks["name"].tolist()
stock_codes = stocks["code"].tolist()
industries = open("industries.txt", encoding="utf-8").read().splitlines()

# 한국어 NLP
okt = Okt()

# 감정 분석 모델
sentiment_analyzer = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")

# SQLite 데이터베이스
conn = sqlite3.connect("stock_mentions.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS mentions (
        date TEXT,
        channel TEXT,
        stock TEXT,
        industry TEXT,
        count INTEGER,
        sentiment REAL
    )
""")
conn.commit()

async def scrape_channels(initial_days=3):
    async with TelegramClient("session", api_id, api_hash) as client:
        end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=initial_days)
        
        for channel in channels:
            stock_counts = Counter()
            industry_counts = Counter()
            sentiments = {}
            
            async for message in client.iter_messages(channel, limit=1000):
                if not message.text or message.date < start_time or message.date >= end_time:
                    continue
                
                text = message.text
                msg_date = message.date.strftime("%Y-%m-%d")
                
                # 종목 매칭
                for name, code in zip(stock_names, stock_codes):
                    if name in text or code in text:
                        stock_counts[(msg_date, name)] += 1
                        try:
                            sentiment = sentiment_analyzer(text[:512])[0]
                            score = sentiment["score"] if sentiment["label"].startswith("positive") else -sentiment["score"]
                            sentiments[(msg_date, name)] = sentiments.get((msg_date, name), []) + [score]
                        except:
                            pass
                
                # 산업 매칭
                for industry in industries:
                    if industry in text:
                        industry_counts[(msg_date, industry)] += 1
                
                # 추가 NLP
                nouns = okt.nouns(text)
                for noun in nouns:
                    if noun in stock_names:
                        stock_counts[(msg_date, noun)] += 1
                    if noun in industries:
                        industry_counts[(msg_date, noun)] += 1
            
            # 데이터베이스 저장
            for (date, stock), count in stock_counts.items():
                avg_sentiment = sum(sentiments.get((date, stock), [0])) / len(sentiments.get((date, stock), [1]))
                cursor.execute(
                    "INSERT INTO mentions (date, channel, stock, industry, count, sentiment) VALUES (?, ?, ?, ?, ?, ?)",
                    (date, channel, stock, None, count, avg_sentiment)
                )
            for (date, industry), count in industry_counts.items():
                cursor.execute(
                    "INSERT INTO mentions (date, channel, stock, industry, count, sentiment) VALUES (?, ?, ?, ?, ?, ?)",
                    (date, channel, None, industry, count, 0)
                )
            conn.commit()

def run_scraper():
    asyncio.run(scrape_channels(initial_days=1))  # 이후 하루 치
    print("Scraping completed for", datetime.now().strftime("%Y-%m-%d"))

def init_scrape():
    asyncio.run(scrape_channels(initial_days=3))  # 초기 3일 치
    print("Initial 3-day scraping completed")

# 스케줄링
schedule.every().day.at("02:00").do(run_scraper)

# Streamlit 대시보드
def load_data():
    df = pd.read_sql("SELECT * FROM mentions", conn)
    return df

def main():
    st.title("주식 조기경보기 대시보드")
    
    df = load_data()
    if df.empty:
        st.write("데이터가 없습니다. 스크래핑을 먼저 실행하세요.")
        if st.button("초기 3일 치 스크래핑 실행"):
            init_scrape()
        return
    
    # 필터
    st.sidebar.header("필터")
    date = st.sidebar.selectbox("날짜 선택", sorted(df["date"].unique(), reverse=True))
    channel = st.sidebar.multiselect("채널 선택", channels, default=channels)
    df_filtered = df[(df["date"] == date) & (df["channel"].isin(channel))]
    
    # 종목 분석
    st.header("종목 언급 빈도")
    stock_df = df_filtered[df_filtered["stock"].notnull()][["stock", "count", "sentiment"]]
    if not stock_df.empty:
        stock_df = stock_df.groupby("stock").agg({"count": "sum", "sentiment": "mean"}).reset_index()
        top_stocks = stock_df.sort_values("count", ascending=False).head(10)
        
        fig = px.bar(top_stocks, x="stock", y="count", title="상위 10개 종목")
        st.plotly_chart(fig)
        
        # 감정 분석
        st.header("종목 감정 분석")
        top_stocks["sentiment_label"] = top_stocks["sentiment"].apply(lambda x: "긍정" if x > 0 else "부정")
        fig = px.pie(top_stocks, names="stock", values="sentiment", title="종목별 감정 비율")
        st.plotly_chart(fig)
    
    # 산업 분석
    st.header("산업 언급 빈도")
    industry_df = df_filtered[df_filtered["industry"].notnull()][["industry", "count"]]
    if not industry_df.empty:
        industry_df = industry_df.groupby("industry").sum().reset_index()
        top_industries = industry_df.sort_values("count", ascending=False).head(10)
        
        fig = px.bar(top_industries, x="industry", y="count", title="상위 10개 산업")
        st.plotly_chart(fig)
    
    # 트렌드 분석
    st.header("트렌드 분석 (최근 3일)")
    last_3_days = df[df["date"].isin(sorted(df["date"].unique(), reverse=True)[:3])]
    if not last_3_days.empty:
        stock_trend = last_3_days[last_3_days["stock"].notnull()].groupby(["date", "stock"])["count"].sum().unstack().fillna(0)
        fig = px.line(stock_trend, title="종목 언급 추이")
        st.plotly_chart(fig)
        
        industry_trend = last_3_days[last_3_days["industry"].notnull()].groupby(["date", "industry"])["count"].sum().unstack().fillna(0)
        fig = px.line(industry_trend, title="산업 언급 추이")
        st.plotly_chart(fig)
    
    # 알림 섹션
    st.header("급등 알림")
    prev_day = (pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_df = df[df["date"] == prev_day]
    for stock in stock_df["stock"]:
        curr_count = stock_df[stock_df["stock"] == stock]["count"].iloc[0]
        prev_count = prev_df[prev_df["stock"] == stock]["count"].sum()
        if prev_count > 0 and curr_count / prev_count > 2:
            st.write(f"🚨 {stock}: 언급 {curr_count}회 (전일 대비 {curr_count/prev_count:.1%} 증가)")

if __name__ == "__main__":
    # 스케줄러 백그라운드
    import threading
    def run_schedule():
        while True:
            schedule.run_pending()
            time.sleep(60)
    threading.Thread(target=run_schedule, daemon=True).start()
    
    # Streamlit 실행
    main()