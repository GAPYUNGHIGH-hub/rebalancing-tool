import streamlit as st
import pandas as pd
import requests
from io import StringIO
import math
import urllib.parse
import time

st.set_page_config(page_title="올웨더 리밸런싱", layout="wide")
st.title("올웨더 리밸런싱")

st.sidebar.title("⚙️ 설정")
mode = st.sidebar.radio(
    "주가 수집 방식",
    ["🤖 자동 (네이버 금융)", "📊 시트 연동 (추천)", "✍️ 직접 수정"],
    help="자동: 네이버에서 크롤링 (오류 가능)\n시트 연동: Google Sheets에서 읽기 (추천)\n직접 수정: 필요시 수정"
)

@st.cache_data(ttl=60)
def load_data_from_gsheet(url):
    try:
        file_id = url.split('/')[-2]
        encoded_sheet1 = urllib.parse.quote("Sheet1")
        encoded_sheet2 = urllib.parse.quote("주문전잔고(수량)")
        encoded_sheet3 = urllib.parse.quote("현재가")
        timestamp = int(time.time())
        sheet1_url = f"https://docs.google.com/spreadsheets/d/{file_id}/gviz/tq?tqx=out:csv&sheet={encoded_sheet1}&_={timestamp}"
        sheet2_url = f"https://docs.google.com/spreadsheets/d/{file_id}/gviz/tq?tqx=out:csv&sheet={encoded_sheet2}&_={timestamp}"
        sheet3_url = f"https://docs.google.com/spreadsheets/d/{file_id}/gviz/tq?tqx=out:csv&sheet={encoded_sheet3}&_={timestamp}"
        df_target = pd.read_csv(sheet1_url)
        df_balance = pd.read_csv(sheet2_url)
        try:
            df_prices = pd.read_csv(sheet3_url)
            df_prices.columns = [str(c).strip() for c in df_prices.columns]
            df_prices['종목명'] = df_prices['종목명'].astype(str).str.strip()
            df_prices = df_prices.set_index('종목명')
        except:
            df_prices = None
        df_target.columns = [str(c).strip() for c in df_target.columns]
        df_balance.columns = [str(c).strip() for c in df_balance.columns]
        def to_num(val):
            if pd.isna(val): return 0
            try: return float(str(val).replace(',', '').strip())
            except: return 0
        for col in df_balance.columns:
            if col not in ['종목명', '종목코드']:
                df_balance[col] = df_balance[col].apply(to_num)
        if '종목별최초투자비중' in df_target.columns:
            df_target['종목별최초투자비중'] = df_target['종목별최초투자비중'].apply(to_num)
        df_target['종목명'] = df_target['종목명'].astype(str).str.strip()
        df_balance['종목명'] = df_balance['종목명'].astype(str).str.strip()
        df_target = df_target.set_index('종목명')
        df_balance = df_balance.set_index('종목명')
        merged_df = df_target.join(df_balance, how='left').fillna(0)
        merged_df = merged_df.loc[:, ~merged_df.columns.str.startswith('Unnamed')]
        return merged_df, df_prices, None
    except Exception as e:
        return None, None, f"로드 실패: {e}"

@st.cache_data(ttl=60)
def get_naver_prices(stock_code):
    try:
        url = f"https://finance.naver.com/item/sise.naver?code={str(stock_code).zfill(6)}"
        html = requests.get(url, timeout=5).text
        tables = pd.read_html(StringIO(html), encoding='euc-kr')
        today = int(str(tables[1].iloc[0, 1]).replace(',', ''))
        prev = int(str(tables[1].iloc[2, 3]).replace(',', ''))
        return prev, today
    except:
        return None, None

def fetch_market_data_auto(df_code_map):
    data = {}
    progress_bar = st.progress(0)
    status_text = st.empty()
    failed_stocks = []
    total = len(df_code_map)
    for idx, (stock_name, row) in enumerate(df_code_map.iterrows()):
        status_text.text(f"주가 데이터 로딩 중... ({idx+1}/{total}) - {stock_name}")
        code = row['종목코드']
        prev, today = get_naver_prices(code)
        if prev is None or today is None:
            failed_stocks.append(stock_name)
            data[stock_name] = {'curr': 0, 'prev': 0}
        else:
            data[stock_name] = {'curr': today, 'prev': prev}
        progress_bar.progress((idx + 1) / total)
        time.sleep(0.2)
    progress_bar.empty()
    status_text.empty()
    if failed_stocks:
        st.error(f"⚠️ 주가 로딩 실패: {', '.join(failed_stocks)}")
    return data, len(failed_stocks) == 0

def format_weight(value):
    percent = value * 100
    if percent == int(percent):
        return f"{int(percent)}"
    else:
        return f"{percent:.10f}".rstrip('0').rstrip('.')

url = "https://docs.google.com/spreadsheets/d/1J1CWM1xCmzw1tBEMzw3c1OCQPOsiVWZF7MnZ_lEY-4Q/edit?usp=sharing"

col1, col2 = st.columns([6, 1])
with col2:
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()

with st.spinner("데이터 로딩 중..."):
    df_master, df_prices, err = load_data_from_gsheet(url)

if err:
    st.error(err)
else:
    exclude = ['종목코드', '종목별최초투자비중']
    active_accs = [c for c in df_master.columns if c not in exclude]
    st.sidebar.header("💰 계좌별 추가 자금")
    cash_map = {acc: st.sidebar.number_input(f"{acc} (원)", value=0, step=10000) for acc in active_accs}
    if mode in ["📊 시트 연동 (추천)", "✍️ 직접 수정"]:
        st.sidebar.header("📊 종목별 주가")
        if df_prices is not None and mode == "📊 시트 연동 (추천)":
            st.sidebar.success("✅ Google Sheets '현재가' 시트에서 자동 로딩됨")
            st.sidebar.caption("필요시 '직접 수정' 모드로 변경하세요")
        else:
            st.sidebar.info("💡 Google Sheets에서 주가를 확인하세요")
        price_data = {}
        for idx, row in df_master.iterrows():
            default_curr = 0
            default_prev = 0
            if df_prices is not None and idx in df_prices.index:
                try:
                    if '현재가' in df_prices.columns:
                        default_curr = int(float(str(df_prices.loc[idx, '현재가']).replace(',', '')))
                    if '전일종가' in df_prices.columns:
                        default_prev = int(float(str(df_prices.loc[idx, '전일종가']).replace(',', '')))
                except:
                    pass
            with st.sidebar.expander(f"📌 {idx}"):
                col1, col2 = st.columns(2)
                if mode == "📊 시트 연동 (추천)":
                    with col1:
                        st.markdown(f"<div style='text-align:center;padding:5px'><div style='font-size:11px;color:#666;margin-bottom:3px'>현재가</div><div style='font-size:16px;font-weight:bold;color:#1f77b4'>{default_curr:,}원</div></div>", unsafe_allow_html=True)
                    with col2:
                        st.markdown(f"<div style='text-align:center;padding:5px'><div style='font-size:11px;color:#666;margin-bottom:3px'>전일종가</div><div style='font-size:16px;font-weight:bold;color:#1f77b4'>{default_prev:,}원</div></div>", unsafe_allow_html=True)
                    price_data[idx] = {'curr': default_curr, 'prev': default_prev}

    if st.button("🚀 분석 및 주문 계산 실행", type="primary"):
        if mode == "🤖 자동 (네이버 금융)":
            df_code_map = df_master[['종목코드']].copy()
            m_data, success = fetch_market_data_auto(df_code_map)
            if not success:
                st.warning("⚠️ 일부 종목 로딩 실패. '시트 연동' 모드를 사용하세요.")
        else:
            if not all(v['curr'] > 0 for v in price_data.values()):
                st.error("⚠️ 일부 종목의 현재가가 0입니다. Google Sheets '현재가' 시트를 확인하세요.")
                st.stop()
            m_data = price_data
        total_val_curr = 0
        total_val_prev = 0
        acc_results = {}
        for acc in active_accs:
            df = df_master[['종목코드', '종목별최초투자비중', acc]].copy()
            df.rename(columns={acc: '현재잔고'}, inplace=True)
            df['현재가'] = df.index.map(lambda x: m_data.get(x, {}).get('curr', 0))
            df['전일가'] = df.index.map(lambda x: m_data.get(x, {}).get('prev', 0))
            df['평가액_raw'] = df['현재잔고'] * df['현재가']
            df['전일평가액_raw'] = df['현재잔고'] * df['전일가']
            df['현재평가액'] = df['평가액_raw'].apply(lambda x: math.floor(x))
            current_stock_total = df['평가액_raw'].sum()
            total_val_curr += current_stock_total
            total_val_prev += df['전일평가액_raw'].sum()
            added_cash = cash_map[acc]
            total_asset_rebal = current_stock_total + added_cash
            df['현재비중_raw'] = df['평가액_raw'] / current_stock_total
            df['상대비중차이'] = (df['현재비중_raw'] / df['종목별최초투자비중']) - 1
            df['현재비중'] = df['현재비중_raw'].round(6)
            df.loc[df['종목별최초투자비중'] == 0, '상대비중차이'] = 0
            acc_max_diff = df['상대비중차이'].abs().max()
            df['목표수량'] = ((total_asset_rebal * df['종목별최초투자비중']) / df['현재가']).apply(lambda x: math.floor(x) if x > 0 else 0)
            df['주문수량'] = df['목표수량'] - df['현재잔고']
            df['주문금액'] = (df['주문수량'] * df['현재가']).apply(math.floor)
            if added_cash == 0 and acc_max_diff <= 0.1:
                df['주문수량'] = 0
                df['주문금액'] = 0
            acc_results[acc] = {'df': df, 'view_total': current_stock_total, 'rebal_total': total_asset_rebal, 'max_diff': acc_max_diff}
        total_diff = total_val_curr - total_val_prev
        total_rate = (total_diff / total_val_prev * 100) if total_val_prev > 0 else 0
        st.markdown(f"""
        <div style='background:#f0f2f6;padding:20px;border-radius:10px;margin-bottom:20px'>
        <h3 style='margin:0'>전체 계좌 순수 자산 합계</h3>
        <p style='font-size:12px;color:#666;margin:5px 0'>(추가매수금액 또는 매도금액은 포함안됨)</p>
        <h2 style='color:#1f77b4;margin:10px 0'>{int(total_val_curr):,}원</h2>
        <p style='color:{'green' if total_diff >=0 else 'red'}'>전일대비 {int(total_diff):+,}원 ({total_rate:+.2f}%)</p>
        </div>
        """, unsafe_allow_html=True)
        tabs = st.tabs([f"👤 {acc}" for acc in active_accs])
        for i, acc in enumerate(active_accs):
            with tabs[i]:
                res = acc_results[acc]
                df_acc = res['df']
                added_c = cash_map[acc]
                st.markdown(f"**💰 순수 주식 자산:** {int(res['view_total']):,}원 | **➕ 추가 자금:** {added_c:,}원")
                st.caption(f"(리밸런싱 계산 기준 총 자산: {int(res['rebal_total']):,}원)")
                if added_c == 0 and res['max_diff'] <= 0.1:
                    st.success(f"✅ 최대 상대비중차이 {res['max_diff']*100:.6f}% (10% 이내) → 주문 없음")
                elif added_c == 0:
                    st.warning(f"⚠️ 최대 상대비중차이 {res['max_diff']*100:.6f}% (10% 초과) → 리밸런싱 실행")
                st.markdown("<h4 style='border-bottom:2px solid #1f77b4'>📊 포트폴리오 상세 분석</h4>", unsafe_allow_html=True)
                display_df = pd.DataFrame({
                    "종목명": df_acc.index,
                    "현재가": df_acc['현재가'].apply(lambda x: f"{int(x):,}원" if x > 0 else "미입력"),
                    "보유수량": df_acc['현재잔고'].apply(lambda x: f"{int(x):,}주"),
                    "평가금액": df_acc['현재평가액'].apply(lambda x: f"{int(x):,}원"),
                    "목표비중": df_acc['종목별최초투자비중'].apply(lambda x: f"{format_weight(x)}%"),
                    "현재비중": df_acc['현재비중'].apply(lambda x: f"{x*100:.6f}%"),
                    "상대비중차이": df_acc['상대비중차이'].apply(lambda x: f"<span style='color:{'red' if x > 0 else 'blue'}'>{x*100:+.6f}%</span>")
                })
                st.write(display_df.to_html(classes='table', escape=False, index=False), unsafe_allow_html=True)
                st.markdown("<h4 style='border-bottom:2px solid #1f77b4'>🛒 리밸런싱 주문표</h4>", unsafe_allow_html=True)
                o_df = df_acc[df_acc['주문수량'] != 0].copy()
                if o_df.empty:
                    st.info("✅ 현재 비중이 목표 대비 10% 이내로 잘 유지되고 있습니다.")
                else:
                    order_df = pd.DataFrame({
                        "종목명": o_df.index,
                        "구분": o_df['주문수량'].apply(lambda x: f"<span style='color:{'green' if x > 0 else 'red'}'>{'매수' if x > 0 else '매도'}</span>"),
                        "주문수량": o_df['주문수량'].apply(lambda x: f"<span style='color:red'>{int(x):,}주</span>" if x < 0 else f"{int(x):,}주"),
                        "예상금액": o_df['주문금액'].apply(lambda x: f"{abs(int(x)):,}원"),
                        "최종목표수량": o_df['목표수량'].apply(lambda x: f"{int(x):,}주")
                    })
                    st.write(order_df.to_html(classes='table', escape=False, index=False), unsafe_allow_html=True)
                    buy_amount = df_acc[df_acc['주문수량'] > 0]['주문금액'].sum()
                    sell_amount = df_acc[df_acc['주문수량'] < 0]['주문금액'].sum()
                    net_order = buy_amount + sell_amount
                    st.markdown(f"""
                    <div style='background:#f8f9fa;padding:15px;border-radius:8px;margin-top:15px'>
                    <table style='width:100%;border-collapse:collapse'>
                    <tr><td style='padding:5px;font-weight:bold'>최종 매수금액:</td><td style='padding:5px;text-align:right;color:green'>{int(buy_amount):,}원</td></tr>
                    <tr><td style='padding:5px;font-weight:bold'>최종 매도금액:</td><td style='padding:5px;text-align:right;color:red'>{int(abs(sell_amount)):,}원</td></tr>
                    <tr style='border-top:2px solid #dee2e6'><td style='padding:5px;font-weight:bold'>차액 (순매수):</td><td style='padding:5px;text-align:right;color:{'green' if net_order > 0 else 'red'};font-weight:bold'>{int(net_order):,}원</td></tr>
                    </table>
                    </div>
                    """, unsafe_allow_html=True)
