import time
import pandas as pd
import csv
import json
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


def setup_driver():
    """Selenium WebDriver를 설정하고 반환합니다."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(5)
    return driver


def scrape_store_details(driver, store_name_to_find, search_query):
    """
    주어진 검색어로 가게를 찾아 주소, 전화번호, 카테고리, 메뉴 정보를 반환합니다.
    """
    encoded_query = quote(search_query)
    search_url = f"https://map.naver.com/p/search/{encoded_query}"
    driver.get(search_url)
    time.sleep(2)

    try:
        # Iframe 진입
        driver.switch_to.default_content()
        try:
            WebDriverWait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
        except TimeoutException:
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "searchIframe")))

            list_container_selector = "#_pcmap_list_scroll_container"
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, list_container_selector)))

            search_results = driver.find_elements(By.CSS_SELECTOR, f"{list_container_selector} li")
            clicked = False
            clean_store_name = store_name_to_find.replace('(주)', '').replace('주식회사', '').strip()

            for result_element in search_results:
                try:
                    element_text = result_element.text
                    if clean_store_name in element_text:
                        result_element.find_element(By.CSS_SELECTOR, "a.tzwk0").click()
                        clicked = True
                        break
                except NoSuchElementException:
                    continue

            if not clicked: return None

            driver.switch_to.default_content()
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))

        time.sleep(1.5)

        iframe_html = driver.find_element(By.XPATH, "/html").get_attribute('outerHTML')
        soup = BeautifulSoup(iframe_html, "html.parser")

        script_tag = soup.find('script', string=lambda t: t and 'window.__APOLLO_STATE__' in t)
        if not script_tag: return None

        json_str_raw = script_tag.string
        start_index = json_str_raw.find('{', json_str_raw.find('window.__APOLLO_STATE__'))

        brace_count, is_in_string, end_index = 0, False, -1
        for i, char in enumerate(json_str_raw[start_index:]):
            if char == '"' and json_str_raw[start_index + i - 1] != '\\': is_in_string = not is_in_string
            if not is_in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
            if brace_count == 0:
                end_index = start_index + i
                break

        if end_index == -1: return None
        json_str = json_str_raw[start_index: end_index + 1]
        all_data = json.loads(json_str)

        place_id = next((key.split(':')[1] for key in all_data if key.startswith("PlaceDetailBase:")), None)
        if not place_id: return None

        base_key = f"PlaceDetailBase:{place_id}"
        store_info = all_data.get(base_key, {})

        # ==========================================================
        # ▼▼▼▼▼▼▼▼▼▼ 메뉴를 하나의 문자열로 합치는 부분 ▼▼▼▼▼▼▼▼▼▼
        # ==========================================================
        menu_list = []
        for key, menu in all_data.items():
            if key.startswith(f"Menu:{place_id}"):
                menu_name = menu.get('name', '')
                menu_price = menu.get('price', '')
                menu_list.append(f"{menu_name}: {menu_price}원")

        # 리스트의 모든 항목을 줄바꿈(\n)으로 연결
        menus_string = "\n".join(menu_list)
        # ==========================================================

        return {
            '주소': store_info.get('address'),
            '전화번호': store_info.get('phone'),
            '카테고리': store_info.get('category'),
            '메뉴': menus_string  # 합쳐진 메뉴 문자열을 반환
        }
    except Exception as e:
        print(f"'{search_query}' 처리 중 오류 발생: {e}")
        return None


if __name__ == "__main__":
    # 1. 입력 파일 읽기
    input_filename = "store_list_new.csv"
    try:
        df = pd.read_csv(input_filename, encoding='utf-8')
    except Exception:
        df = pd.read_csv(input_filename, encoding='cp949')

    # 전체 실행 시 아래 줄을 주석 처리
    df = df.head(5)

    # 2. 출력 파일 준비
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"output_results_{timestamp}.csv"

    with open(output_filename, 'w', newline='', encoding='utf-8-sig') as f:
        wr = csv.writer(f)
        wr.writerow(['입력_가맹점명', '입력_기본주소', '입력_전화번호', '크롤링_주소', '크롤링_전화번호', '크롤링_카테고리', '크롤링_메뉴'])

        driver = setup_driver()
        for index, row in df.iterrows():
            store_name = str(row.get('가맹점명', '')).strip()
            address = str(row.get('가맹점기본주소', ''))
            phone = str(row.get('가맹점전화번호', ''))

            search_query = store_name
            print(f"({index + 1}/{len(df)}) '{search_query}' 정보 수집 중...")

            scraped_info = scrape_store_details(driver, store_name, search_query)

            # 3. CSV 파일에 한 줄로 쓰기
            if scraped_info:
                wr.writerow([
                    store_name, address, phone,
                    scraped_info.get('주소'),
                    scraped_info.get('전화번호'),
                    scraped_info.get('카테고리'),
                    scraped_info.get('메뉴')  # 여기에 합쳐진 메뉴 문자열이 들어감
                ])
            else:
                wr.writerow([store_name, address, phone, '정보 수집 실패', '', '', ''])

            print("-" * 50)

        driver.quit()

    print(f"✅ 모든 작업 완료! 결과가 '{output_filename}' 파일에 저장되었습니다.")