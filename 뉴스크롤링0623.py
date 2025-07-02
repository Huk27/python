import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import urllib3
import random
import smtplib
from email.message import EmailMessage
import re

from playwright.sync_api import sync_playwright
import os  # 현재 작업 폴더 확인을 위해 import

# InsecureRequestWarning 제거
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 두 개의 기업명 리스트 설정
company_names = ["국민카드", "KB카드", "롯데카드", "BC카드", "비씨카드", "우리카드", "농협카드", "NH카드"]
company_data_names = ["국민카드 데이터", "KB카드 데이터", "롯데카드 데이터", "BC카드 데이터", "비씨카드 데이터", "우리카드 데이터", "농협카드 데이터", "NH카드 데이터"]
data_business_keyword = ["데이터 사업", "데이타 사업", "데이타 판매", "데이터 판매", "Data 사업", "Data 판매", "data 사업", "data 판매"]
loan_keyword = ["대출", "개인사업자", "앱테크", "광고사업"]

# 날짜 설정
today = datetime.today()
start_date = today - timedelta(days=7)
end_date = today - timedelta(days=1)

start_date_str = start_date.strftime('%Y.%m.%d')
end_date_str = end_date.strftime('%Y.%m.%d')
start_date_nso = start_date.strftime('%Y%m%d')
end_date_nso = end_date.strftime('%Y%m%d')

user_agents = [
    # User-agents list from the original code...
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.224 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.123 Safari/537.36",
]


# 상대시간 변환 함수
def convert_relative_time_to_date(relative_time):
    try:
        if "시간 전" in relative_time:
            hours = int(relative_time.split("시간 전")[0].strip())
            return (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d')
        elif "일 전" in relative_time:
            days = int(relative_time.split("일 전")[0].strip())
            return (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        elif "개월 전" in relative_time:
            months = int(relative_time.split("개월 전")[0].strip())
            return (datetime.now() - timedelta(days=months * 30)).strftime('%Y-%m-%d')
        elif "주 전" in relative_time:
            weeks = int(relative_time.split("주 전")[0].strip())
            return (datetime.now() - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
        elif "년 전" in relative_time:
            years = int(relative_time.split("년 전")[0].strip())
            return (datetime.now() - timedelta(days=years * 365)).strftime('%Y-%m-%d')
        return datetime.now().strftime('%Y-%m-%d')
    except:
        return "날짜 없음"


# --- [수정됨] crawl_news 함수 ---
# 네이버의 새로운 UI 구조(Fender)와 기존 UI 구조를 모두 처리할 수 있도록 수정되었습니다.
def crawl_news(keywords):
    news_list = []
    titles_seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=random.choice(user_agents),
            locale='ko-KR',
            timezone_id='Asia/Seoul',
        )
        page = context.new_page()

        for keyword in keywords:
            encoded_keyword = keyword.replace(" ", "+")

            # 루프 내에서 날짜를 다시 계산하여 항상 최신 상태를 유지
            today_for_loop = datetime.today()
            start_date_for_loop = today_for_loop - timedelta(days=7)
            end_date_for_loop = today_for_loop - timedelta(days=1)

            current_date = start_date_for_loop
            while current_date <= end_date_for_loop:
                current_date_str = current_date.strftime('%Y.%m.%d')
                current_date_nso = current_date.strftime('%Y%m%d')

                search_url = f"https://search.naver.com/search.naver?where=news&query={encoded_keyword}&sm=tab_opt&sort=0&photo=0&field=0&pd=3&ds={current_date_str}&de={current_date_str}&nso=so:r,p:from{current_date_nso}to{current_date_nso}"
                print(f"크롤링 URL: {search_url}")

                all_articles = []
                is_fender_ui = False  # UI 타입 플래그
                try:
                    page.goto(search_url, timeout=30000)
                    page.wait_for_selector(".list_news, .fds-news-item-list-tab, .not_found", timeout=20000)

                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")

                    if soup.select_one(".not_found"):
                        print(f"'{keyword}'에 대한 검색 결과 없음.")
                    else:
                        # ★★★ 버그 수정 지점 ★★★
                        # 페이지 로드 후, 신형 UI 요소가 있는지 먼저 확인하여 플래그를 설정
                        fender_articles = soup.select("section.sc_new .fds-news-item-list-tab > div")
                        if fender_articles:
                            is_fender_ui = True
                            all_articles = fender_articles
                        else:
                            # 신형 UI가 없으면 구형 UI로 간주하고 선택
                            all_articles = soup.select("ul.list_news > li")

                        print(f"{len(all_articles)}개의 기사를 찾았습니다.")

                except Exception as e:
                    print(f"페이지 로드/파싱 중 오류 발생: {e}")
                    all_articles = []

                for article in all_articles:
                    try:
                        title_tag = None
                        summary_tag = None
                        company_tag = None
                        date_tag = None
                        link_tag = None

                        # ★★★ 버그 수정 지점 ★★★
                        # 루프 시작 전에 설정된 is_fender_ui 플래그를 사용하여 파싱 로직 결정
                        if is_fender_ui:
                            # --- 신형 구조 파싱 (Fender UI) ---
                            title_span = article.select_one('span.sds-comps-text-type-headline1')
                            if title_span:
                                title_tag = title_span.parent
                                link_tag = title_tag
                            summary_tag = article.select_one('span.sds-comps-text-type-body1')
                            company_tag = article.select_one('.sds-comps-profile-info-title a span')
                            date_info_tag = article.select_one(
                                '.sds-comps-profile-info-subtext .sds-comps-text-type-body2')
                            if date_info_tag:
                                date_tag = date_info_tag

                        else:  # is_legacy_ui
                            # --- 구형 구조 파싱 ---
                            title_tag = article.select_one(".news_tit")
                            link_tag = title_tag
                            summary_tag = article.select_one(".api_txt_lines.dsc_txt_wrap")
                            company_tag = article.select_one(".info.press")
                            date_tag = article.select_one(".info_group > span.info")

                        if not title_tag: continue

                        title = title_tag.get('title') or title_tag.get_text(strip=True)
                        link = link_tag.get('href') if link_tag else None

                        if not title or not link or title in titles_seen: continue
                        titles_seen.add(title)

                        summary = summary_tag.get_text(strip=True) if summary_tag else "요약 없음"
                        news_company = company_tag.get_text(strip=True).replace("언론사 선정",
                                                                                "").strip() if company_tag else "언론사 없음"

                        date_text = date_tag.get_text(strip=True) if date_tag else ""
                        article_date = ""
                        date_match = re.search(r'(\d{4}\.\d{2}\.\d{2}\.)', date_text)  # yyyy.mm.dd. 형식
                        if date_match:
                            article_date = datetime.strptime(date_match.group(1), '%Y.%m.%d.').strftime('%Y-%m-%d')
                        elif "전" in date_text:  # 'N시간 전' 등의 상대 시간
                            article_date = convert_relative_time_to_date(date_text)
                        else:  # 날짜 정보가 없는 경우 크롤링 날짜로 대체
                            article_date = current_date.strftime('%Y-%m-%d')

                        news_list.append([news_company, keyword, title, summary, article_date, link])
                    except Exception as e:
                        print(f"개별 기사 정보 처리 중 오류: {e} | 기사 HTML: {str(article)[:500]}")

                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] '{keyword}' 관련 뉴스({current_date_str}) 크롤링 완료 (수집된 총 기사 수: {len(news_list)})")
                time.sleep(random.uniform(2.0, 3.0))
                current_date += timedelta(days=1)

        browser.close()

    return pd.DataFrame(news_list, columns=["언론사명", "검색키워드", "기사제목", "기사요약", "기사날짜", "링크"])


def clean_text(text):
    if isinstance(text, str):
        # 제어문자 (엑셀 비허용 문자) 제거
        text = re.sub(r'[\000-\010\013-\014\016-\037]', '', text)
        return text.strip()
    return ""


# 각 배열별 데이터 크롤링
print("--- 일반 카드사 뉴스 크롤링 시작 ---")
df_general = crawl_news(company_names)
print("\n--- 카드사 데이터 관련 뉴스 크롤링 시작 ---")
df_data_specific = crawl_news(company_data_names)
print("\n--- 데이터 사업 관련 뉴스 크롤링 시작 ---")
df_data_business = crawl_news(data_business_keyword)
print("\n--- 기타 키워드 뉴스 크롤링 시작 ---")
df_loan = crawl_news(loan_keyword)

print("\n--- 모든 크롤링 완료, 데이터 정리 및 엑셀 저장 시작 ---")

df_general = df_general.applymap(clean_text)
df_data_specific = df_data_specific.applymap(clean_text)
df_data_business = df_data_business.applymap(clean_text)
df_loan = df_loan.applymap(clean_text)

# 별도의 시트로 엑셀 저장
excel_path = f"""filtered_news({start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}).xlsx"""
try:
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df_general.to_excel(writer, sheet_name="카드사", index=False)
        df_data_specific.to_excel(writer, sheet_name="카드사_데이터", index=False)
        df_data_business.to_excel(writer, sheet_name="데이터_사업관련", index=False)
        df_loan.to_excel(writer, sheet_name="기타(2팀요청)", index=False)
    print(f"엑셀 파일 저장 완료: {os.path.abspath(excel_path)}")
except Exception as e:
    print(f"엑셀 파일 저장 중 오류 발생: {e}")

if (len(df_general) == 0 and len(df_data_specific) == 0 and len(df_data_business) == 0 and len(df_loan) == 0):
    print(f"""[경고] 모든 카테고리에서 수집된 기사가 없습니다. 네트워크나 차단 문제를 확인해주세요.""")
else:
    print("\n--- 최종 수집 결과 ---")
    print(f"카드사: {len(df_general)}건")
    print(f"카드사_데이터: {len(df_data_specific)}건")
    print(f"데이터_사업관련: {len(df_data_business)}건")
    print(f"기타(2팀요청): {len(df_loan)}건")