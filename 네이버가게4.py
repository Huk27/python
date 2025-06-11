import time
import pandas as pd
import csv
import json
import random
from datetime import datetime
from urllib.parse import quote
import os
from difflib import SequenceMatcher

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# --- 설정값 ---
INPUT_FILENAME = "store_list_new2.csv"
MAX_RETRIES = 2
REQUEST_DELAY_MIN = 2
REQUEST_DELAY_MAX = 4

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
]


def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver


def get_address_similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def scrape_store_details(driver, store_name_to_find, original_address, search_query):
    encoded_query = quote(search_query)
    search_url = f"https://map.naver.com/p/search/{encoded_query}"
    driver.get(search_url)
    time.sleep(2.5)

    try:
        driver.switch_to.default_content()
        try:
            WebDriverWait(driver, 7).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
            print("INFO: 상세 페이지로 바로 이동했습니다.")
        except TimeoutException:
            print("INFO: 검색 목록 페이지로 판단. 목록 분석을 시작합니다.")
            driver.switch_to.default_content()
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "searchIframe")))

            list_container_selector = "#_pcmap_list_scroll_container"
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, list_container_selector)))

            search_results = driver.find_elements(By.CSS_SELECTOR, f"{list_container_selector} li")

            candidates = []
            clean_store_name = store_name_to_find.replace('(주)', '').replace('주식회사', '').strip()

            for result_element in search_results:
                try:
                    # ==========================================================
                    # ▼▼▼ [최종 필터] '가게 이름' 태그가 있는지 먼저 확인 ▼▼▼
                    # ==========================================================
                    name_span = result_element.find_element(By.CSS_SELECTOR, "span.YwYLL")
                    displayed_name = name_span.text
                    # ==========================================================

                    element_text = result_element.text
                    lines = element_text.splitlines()
                    if not lines: continue

                    address_snippet = ""
                    for line in lines[1:]:
                        if any(keyword in line for keyword in ['동', '로', '길', '읍', '면', '리']):
                            address_snippet = line
                            break
                    if not address_snippet and len(lines) > 1:
                        address_snippet = lines[1]
                    if not address_snippet: continue

                    skip_keywords = ["공인중개사", "부동산", "약국", "모텔", "렌터카", "게스트하우스"]
                    if any(k in displayed_name for k in skip_keywords):
                        continue

                    name_score = get_address_similarity(clean_store_name, displayed_name)
                    address_score = get_address_similarity(original_address, address_snippet)
                    final_score = (address_score * 0.5) + (name_score * 0.5)

                    candidates.append({'score': final_score, 'element': result_element, 'name': displayed_name})
                except NoSuchElementException:
                    # 이름 태그(span.YwYLL)가 없는 li는 가게가 아니므로 무시
                    continue
                except Exception:
                    continue

            if not candidates:
                print("INFO: 분석할 후보 가게 목록을 찾지 못했습니다.")
                return None

            best_match = sorted(candidates, key=lambda x: x['score'], reverse=True)[0]

            if best_match['score'] > 0.4:
                print(f"INFO: 가장 유사한 가게 '{best_match['name']}'를 선택 (점수: {best_match['score']:.2f}). 클릭합니다...")
                best_match['element'].find_element(By.TAG_NAME, "a").click()
            else:
                print(f"INFO: 검색 목록에서 '{store_name_to_find}'와 주소가 비슷한 가게를 찾지 못했습니다 (최고점수: {best_match['score']:.2f}).")
                return None

            driver.switch_to.default_content()
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))

        time.sleep(2)

        iframe_html = driver.find_element(By.XPATH, "/html").get_attribute('outerHTML')
        soup = BeautifulSoup(iframe_html, "html.parser")

        address = soup.select_one("span.LDgIH").text if soup.select_one("span.LDgIH") else ""
        phone = soup.select_one("span.xlx7Q").text if soup.select_one("span.xlx7Q") else ""
        category = soup.select_one("span.lnJFt").text if soup.select_one("span.lnJFt") else ""

        unique_menus = set()
        html_menu_elements = soup.select("li.ipNNM, li.gl2cc")
        for item in html_menu_elements:
            name_element = item.select_one("span.lPzHi, span.VQvNX")
            price_element = item.select_one("div.G7Uac, div.gl2cc")
            if name_element and price_element:
                unique_menus.add(f"{name_element.get_text(strip=True)}: {price_element.get_text(strip=True)}")

        script_tag = soup.find('script', string=lambda t: t and 'window.__APOLLO_STATE__' in t)
        if script_tag:
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
            if end_index != -1:
                json_str = json_str_raw[start_index: end_index + 1]
                all_data = json.loads(json_str)
                place_id = next((key.split(':')[1] for key in all_data if key.startswith("PlaceDetailBase:")), None)
                if place_id:
                    for key, menu in all_data.items():
                        if key.startswith(f"Menu:{place_id}"):
                            price_str = f"{menu.get('price', '')}원" if str(
                                menu.get('price', '')).isdigit() else menu.get('price', '')
                            unique_menus.add(f"{menu.get('name', '')}: {price_str}")

        menus_string = ",".join(sorted(list(unique_menus)))
        return {'주소': address, '전화번호': phone, '카테고리': category, '메뉴': menus_string}

    except Exception as e:
        if isinstance(e, WebDriverException): raise
        print(f"'{search_query}' 처리 중 오류 발생: {e}")
        return None


if __name__ == "__main__":
    try:
        df = pd.read_csv(INPUT_FILENAME, encoding='utf-8')
    except FileNotFoundError:
        print(f"오류: 입력 파일 '{INPUT_FILENAME}'을 찾을 수 없습니다. 파일을 'CSV UTF-8' 형식으로 저장했는지 확인해주세요.")
        exit()
    except UnicodeDecodeError:
        print(f"오류: 파일 인코딩이 'UTF-8'이 아닙니다. 엑셀에서 'CSV UTF-8'로 저장했는지 다시 확인해주세요.")
        exit()
    except Exception as e:
        print(f"파일 읽기 오류: {e}")
        exit()

    df.fillna('', inplace=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"output_results_FINAL_{timestamp}.csv"

    file_exists = os.path.isfile(output_filename)
    with open(output_filename, 'a', newline='', encoding='utf-8-sig') as f:
        wr = csv.writer(f)
        if not file_exists:
            wr.writerow(['입력_가맹점명', '입력_기본주소', '입력_전화번호', '크롤링_주소', '크롤링_전화번호', '크롤링_카테고리', '크롤링_메뉴'])

        driver = setup_driver()
        for index, row in df.iterrows():
            scraped_info = None
            for attempt in range(MAX_RETRIES):
                try:
                    original_store_name = str(row.get('가맹점명', ''))
                    original_address = str(row.get('가맹점기본주소', ''))
                    phone = str(row.get('가맹점전화번호', ''))

                    temp_name = original_store_name
                    if ',' in temp_name:
                        temp_name = temp_name.split(',')[-1]

                    remove_list = ['(주)', '주식회사', '파리크라상', '에프지케이', '티엠컴퍼니']
                    for term in remove_list:
                        temp_name = temp_name.replace(term, '')
                    cleaned_name = temp_name.strip()

                    print(f"\n({index + 1}/{len(df)}) '{cleaned_name}' 정보 수집 중... (시도 {attempt + 1})")


                    search_query_1 = f"제주 {cleaned_name}"
                    print(f"--- 1차 검색 시도: '{search_query_1}'")
                    try:
                        scraped_info = scrape_store_details(driver, cleaned_name, original_address, search_query_1)
                    except Exception as e:
                        print(f"1차시도중 ERROR 발생: {e} ")
                    if scraped_info: break

                    search_query_2 = f"{original_address} {cleaned_name}" if original_address else cleaned_name
                    print(f"--- 2차 검색 시도: {search_query_2}")
                    scraped_info = scrape_store_details(driver, cleaned_name, original_address, search_query_2)
                    if scraped_info: break

                    if attempt < MAX_RETRIES - 1:
                        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

                except WebDriverException as e:
                    print(f"ERROR: WebDriver 오류. 드라이버를 재시작합니다.")
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = setup_driver()
                    if attempt < MAX_RETRIES - 1: print("INFO: 잠시 후 재시도합니다...")
                    continue

            if scraped_info:
                print("INFO: 정보 수집 성공!")
                wr.writerow(
                    [original_store_name, original_address, phone, scraped_info.get('주소'), scraped_info.get('전화번호'),
                     scraped_info.get('카테고리'), scraped_info.get('메뉴')])
            else:
                print(f"ERROR: 모든 재시도 실패. '{original_store_name}' 정보를 저장하지 못했습니다.")
                wr.writerow([original_store_name, original_address, phone, '정보 수집 최종 실패', '', '', ''])
            print("-" * 50)

            if (index + 1) % 100 == 0:
                f.flush()
                print(f"================== INFO: {index + 1}건 처리 완료. 파일에 중간 저장되었습니다. ==================")

        driver.quit()

    print(f"\n✅ 모든 작업 완료! 결과가 '{output_filename}' 파일에 저장되었습니다.")