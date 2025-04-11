import pandas as pd
import sqlite3
import streamlit as st
import plotly.express as px

conn = sqlite3.connect("stock_mentions.db")

def load_data():
    df = pd.read_sql("SELECT * FROM mentions", conn)
    return df

def main():
    st.title("주식 조기경보기 대시보드")
    df = load_data()
    if df.empty:
        st.write("데이터가 없습니다. 스크래핑을 먼저 실행하세요.")
        return

    st.header("종목별 언급 빈도")
    stock_counts = df[df["stock"].notnull()].groupby("stock")["count"].sum().sort_values(ascending=False).head(10)
    fig1 = px.bar(x=stock_counts.values, y=stock_counts.index, orientation="h", title="상위 10개 종목")
    st.plotly_chart(fig1)

    st.header("산업별 언급 빈도")
    industry_counts = df[df["industry"].notnull()].groupby("industry")["count"].sum().sort_values(ascending=False).head(10)
    fig2 = px.bar(x=industry_counts.values, y=industry_counts.index, orientation="h", title="상위 10개 산업")
    st.plotly_chart(fig2)

    st.header("시간별 트렌드")
    stock_trends = df[df["stock"].notnull()].groupby(["date", "stock"])["count"].sum().unstack().fillna(0)
    fig3 = px.line(stock_trends, title="종목별 언급 트렌드")
    st.plotly_chart(fig3)

    st.header("알림: 급등 종목")
    today = df["date"].max()
    yesterday = (pd.to_datetime(today) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    today_counts = df[df["date"] == today].groupby("stock")["count"].sum()
    yesterday_counts = df[df["date"] == yesterday].groupby("stock")["count"].sum()
    changes = ((today_counts - yesterday_counts) / yesterday_counts * 100).dropna().sort_values(ascending=False)
    for stock, change in changes.head(5).items():
        if change > 100:
            st.write(f"🚨 {stock}: 언급 {int(today_counts[stock])}회 (전일 대비 {change:.1f}% 증가)")

if __name__ == "__main__":
    main()
