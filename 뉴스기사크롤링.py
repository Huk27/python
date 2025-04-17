import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import urllib3
import random
import smtplib
from email.message import EmailMessage

# InsecureRequestWarning 제거
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 두 개의 기업명 리스트 설정
company_names = ["국민카드", "KB카드", "롯데카드", "BC카드", "비씨카드", "우리카드", "농협카드", "NH카드"]
company_data_names = ["국민카드 데이터", "KB카드 데이터", "롯데카드 데이터", "BC카드 데이터", "비씨카드 데이터", "우리카드 데이터", "농협카드 데이터", "NH카드 데이터"]
data_business_keyword = ["데이터 사업", "데이타 사업", "데이타 판매", "데이터 판매", "Data 사업", "Data 판매", "data 사업", "data 판매"]
loan_keyword = ["대출","개인사업자","앱테크","광고사업"]

# # 관련 키워드 설정
# keywords = ["국민카드", "KB카드", "삼성카드", "현대카드", "롯데카드", "BC카드", "비씨카드", "우리카드", "농협카드", "NH카드"]

# 날짜 설정
today = datetime.today()
start_date = today - timedelta(days=7)
end_date = today - timedelta(days=1)

start_date_str = start_date.strftime('%Y.%m.%d')
end_date_str = end_date.strftime('%Y.%m.%d')
start_date_nso = start_date.strftime('%Y%m%d')
end_date_nso = end_date.strftime('%Y%m%d')

user_agents = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.224 Safari/537.36",

    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.123 Safari/537.36",

    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",

    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",

    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:121.0) Gecko/20100101 Firefox/121.0",

    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.2277.83 Safari/537.36 Edg/121.0.2277.83",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.2275.92 Safari/537.36 Edg/120.0.2275.92",

    # Chrome on Android
    "Mozilla/5.0 (Linux; Android 14; SM-S918N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.144 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.128 Mobile Safari/537.36",

    # Safari on iPhone
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",

    # Chrome on iPad
    "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/121.0.6167.144 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.6099.132 Mobile/15E148 Safari/604.1",

    # Edge on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.2277.83 Safari/537.36 Edg/121.0.2277.83",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.2275.100 Safari/537.36 Edg/120.0.2275.100",

    # Opera on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.224 Safari/537.36 OPR/93.0.4561.82",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.150 Safari/537.36 OPR/94.0.4606.54",

    # Opera on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.150 Safari/537.36 OPR/94.0.4606.54",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.132 Safari/537.36 OPR/93.0.4561.82",

    # Samsung Internet on Android
    "Mozilla/5.0 (Linux; Android 14; SM-S918N) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/22.0 Chrome/121.0.6167.144 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/21.0 Chrome/120.0.6099.128 Mobile Safari/537.36",

    # Vivaldi on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.150 Safari/537.36 Vivaldi/6.3.3139.157",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.224 Safari/537.36 Vivaldi/6.2.3105.58",
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

# 크롤링 함수 정의
def crawl_news(keywords):
    news_list = []
    titles_seen = set()

    for keyword in keywords:
        keyword = keyword.replace(" ", "+")

        current_date = start_date
        while current_date <= end_date:
            current_date_str = current_date.strftime('%Y.%m.%d')
            current_date_nso = current_date.strftime('%Y%m%d')

            for page in range(1, 3):
                retry_count = 0
                max_retry = 5
                headers = {
                    "User-Agent": random.choice(user_agents)
                }

                while retry_count < max_retry:
                    search_url = f"https://search.naver.com/search.naver?where=news&query={keyword}&sm=tab_opt&sort=0&photo=0&field=0&pd=3&ds={current_date_str}&de={current_date_str}&docid=&related=0&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so%3Ar%2Cp%3Afrom{current_date_nso}to{current_date_nso}&start={(page - 1) * 10 + 1}"
                    print(f"크롤링 URL: {search_url}")

                    headers = {
                        "User-Agent": random.choice(user_agents)
                    }
                    response = requests.get(search_url, headers=headers, verify=False)
                    soup = BeautifulSoup(response.text, "html.parser")
                    articles = soup.select(".news_area")

                    if articles:
                        break  # articles가 있으면 retry 종료
                    else:
                        retry_count += 1
                        print(f"{keyword}({current_date_str}) result is empty... retry {retry_count}/{max_retry} / userAgent: {headers}")
                        time.sleep(random.uniform(0.5, 1.0))  # 1초 대기 후 재시도

                if not articles:  # 최대 retry 후에도 비어있으면 페이징 중단
                    print(f"{keyword} paging end due to empty result after retries.")
                    break

                for article in articles:
                    try:
                        news_company = article.select_one(".info_group a").text.strip()
                        title = article.select_one(".news_tit").text.strip()
                        link = article.select_one(".news_tit")["href"]

                        if title in titles_seen:
                            continue
                        titles_seen.add(title)

                        date_element = article.select_one(".news_info .info_group span.info")
                        date_text = date_element.text.strip() if date_element else ""
                        article_date = convert_relative_time_to_date(date_text)

                        if article_date != "날짜 없음" and not (start_date.strftime('%Y-%m-%d') <= article_date <= end_date.strftime('%Y-%m-%d')):
                            continue

                        article_response = requests.get(link, headers=headers, verify=False)
                        article_soup = BeautifulSoup(article_response.text, "html.parser")
                        paragraphs = article_soup.select("p")
                        article_content = " ".join([p.text.strip() for p in paragraphs if p.text.strip()])

                        # if any(keyword in article_content for keyword in keywords):
                        summary = " ".join(article_content.split()[:500])
                        news_list.append([news_company, keyword, title, summary, article_date])

                        time.sleep(random.uniform(1.0, 1.5))  # 딜레이를 랜덤으로 길게 주기
                    except Exception as e:
                        print(f"Error: {e}")

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {keyword} 관련 뉴스({current_date_str}) 크롤링 (현재:{len(news_list)}) 완료.")
            current_date += timedelta(days=1)  # 다음 날짜로 이동


    return pd.DataFrame(news_list, columns=["언론사명", "검색키워드", "기사제목", "기사내용", "기사날짜"])

# 각 배열별 데이터 크롤링
df_general = crawl_news(company_names)
df_data_specific = crawl_news(company_data_names)
df_data_business = crawl_news(data_business_keyword)
df_loan = crawl_news(loan_keyword)

import re

def clean_text(text):
    if isinstance(text, str):
        # 제어문자 (엑셀 비허용 문자) 제거
        text = re.sub(r'[\000-\010\013-\014\016-\037]', '', text)
        return text.strip()
    return ""

df_general = df_general.applymap(clean_text)
df_data_specific = df_data_specific.applymap(clean_text)
df_data_business = df_data_business.applymap(clean_text)
df_loan = df_loan.applymap(clean_text)

# 별도의 시트로 엑셀 저장
excel_path = f"""filtered_news({start_date_str}-{end_date_str}).xlsx"""
try:
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df_general.to_excel(writer, sheet_name="카드사", index=False)
        df_data_specific.to_excel(writer, sheet_name="카드사_데이터", index=False)
        df_data_business.to_excel(writer, sheet_name="데이터_사업관련", index=False)
        df_loan.to_excel(writer, sheet_name="기타(2팀요청)", index=False)
except Exception as e:
    print(f"Error: {e}")

print(f"엑셀 파일 저장 완료: {excel_path}")


if (len(df_general) < 30 or len(df_data_specific) < 30 or len(df_data_business) < 30 or len(df_loan) < 30) :
    print(f"""DF결과값 미달인 상태입니다 확인해주세요.""")
    exit(0)