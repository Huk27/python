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
# 기존 import 구문들 아래에 이 라인을 추가하세요.
from selenium.common.exceptions import ElementClickInterceptedException

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

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2, # CSS 비활성화 추가
    }
    options.add_experimental_option("prefs", prefs)

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
        # Iframe 전환 및 가게 선택 로직
        try:
            driver.switch_to.default_content()
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
                    name_span = result_element.find_element(By.CSS_SELECTOR, "span.YwYLL")
                    displayed_name = name_span.text
                    element_text = result_element.text
                    lines = element_text.splitlines()
                    if not lines: continue
                    address_snippet = ""
                    for line in lines[1:]:
                        if any(keyword in line for keyword in ['동', '로', '길', '읍', '면', '리']):
                            address_snippet = line
                            break
                    if not address_snippet and len(lines) > 1: address_snippet = lines[1]
                    if not address_snippet: continue
                    skip_keywords = ["공인중개사", "부동산", "약국", "모텔", "렌터카", "게스트하우스"]
                    if any(k in displayed_name for k in skip_keywords): continue
                    name_score = get_address_similarity(clean_store_name, displayed_name)
                    address_score = get_address_similarity(original_address, address_snippet)
                    final_score = (address_score * 0.5) + (name_score * 0.5)
                    candidates.append({'score': final_score, 'element': result_element, 'name': displayed_name})
                except NoSuchElementException:
                    continue
                except Exception:
                    continue
            if not candidates: return None
            best_match = sorted(candidates, key=lambda x: x['score'], reverse=True)[0]
            if best_match['score'] > 0.4:
                print(f"INFO: 가장 유사한 가게 '{best_match['name']}'를 선택 (점수: {best_match['score']:.2f}). 클릭합니다...")
                name_element_to_click = best_match['element'].find_element(By.CSS_SELECTOR, "span.YwYLL")
                name_element_to_click.click()
            else:
                return None
            driver.switch_to.default_content()
            try:
                close_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.N7pZE")))
                close_button.click()
                time.sleep(2)
                print("INFO: 왼쪽 검색 패널을 닫아 클릭 방해 요소를 제거했습니다.")
            except (TimeoutException, NoSuchElementException):
                print("INFO: 왼쪽 검색 패널 닫기 버튼을 찾을 수 없거나, 이미 닫혀있습니다.")
                pass
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
        except Exception as e:
            print(f"가게 선택 과정에서 오류 발생: {e}")
            return None

        # 페이지 로딩 안정성 확보
        try:
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "span.GHAhO")))
            print("INFO: 상세 페이지 로딩이 안정된 것을 확인했습니다.")
        except TimeoutException:
            print("ERROR: 상세 페이지 로딩에 실패했거나 구조가 다릅니다.")
            return None

            # 1. 기본 정보 수집
        try:
            address = WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.LDgIH"))).text
        except TimeoutException:
            address = ""
        try:
            phone = driver.find_element(By.CSS_SELECTOR, "span.xlx7Q").text
        except NoSuchElementException:
            phone = ""
        try:
            category = driver.find_element(By.CSS_SELECTOR, "span.lnJFt").text
        except NoSuchElementException:
            category = ""

        # 2. '메뉴' 탭 클릭
        try:
            menu_tab_xpath = "//a[span[text()='메뉴']]"
            menu_tab = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, menu_tab_xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", menu_tab)
            time.sleep(1.0)
            menu_tab.click()
            print("INFO: '메뉴' 탭을 일반 방식으로 클릭했습니다.")
            time.sleep(2.5)
        except ElementClickInterceptedException:
            print("WARNING: 일반 클릭이 방해받아 JavaScript 강제 클릭을 시도합니다.")
            driver.execute_script("arguments[0].click();", menu_tab)
            print("INFO: '메뉴' 탭을 JavaScript로 클릭했습니다.")
            time.sleep(2.5)
        except (TimeoutException, NoSuchElementException):
            print("WARNING: '메뉴' 탭을 찾을 수 없거나 클릭할 수 없습니다.")
            pass

        # 3. '더보기' 버튼 모두 클릭
        try:
            more_buttons_xpath = "//a[contains(., '더보기')] | //button[contains(., '더보기')]"
            for _ in range(5):
                more_buttons = driver.find_elements(By.XPATH, more_buttons_xpath)
                if not more_buttons:
                    print("INFO: 클릭할 '더보기' 버튼이 더 이상 없습니다.")
                    break
                clicked = False
                for button in more_buttons:
                    try:
                        if button.is_displayed():
                            driver.execute_script("arguments[0].click();", button)
                            print("INFO: '더보기' 버튼을 클릭했습니다.")
                            time.sleep(2.0)
                            clicked = True
                            break
                    except Exception:
                        continue
                if not clicked: break
        except Exception as e:
            print(f"WARNING: '더보기' 버튼 처리 중 예외 발생: {e}")
            pass

        # 4. 스크롤 로직
        print("INFO: 모든 메뉴를 부드럽게 로드하기 위해 페이지를 점진적으로 스크롤합니다...")
        try:
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2.0)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    print("INFO: 페이지 맨 아래에 도달하여 스크롤을 마칩니다.")
                    break
                last_height = new_height
        except Exception as e:
            print(f"WARNING: 스크롤 중 오류 발생: {e}")
            pass

        # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼ [핵심 수정] "구조 분석" + "내용 분석" 하이브리드 최종 로직 ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
        # 5. 메뉴 정보 수집
        menus_list = []

        # 1순위: 구조가 명확한 디자인 패턴부터 검색 (정확도 높음)
        # 이 선택자들은 실제 HTML 분석에 기반한, 가장 최신의 안정적인 선택자들입니다.
        specific_patterns = [
            # '파리바게뜨 공항점' 등 이미지 그리드 메뉴 구조
            {"item": "li.Pr1D3", "name": "div.GwPRO", "price": "div.G7Uac"},
            # '고집돌우럭' 등 리스트 메뉴 구조
            {"item": "li.E2jtL", "name": "span.lPzHi", "price": "div.GXS1X"},
        ]

        for i, pattern in enumerate(specific_patterns):
            menu_items = driver.find_elements(By.CSS_SELECTOR, pattern["item"])
            if menu_items:
                print(f"INFO: [1순위-패턴 {i + 1}] '{pattern['item']}' 디자인과 일치. {len(menu_items)}개 메뉴를 분석합니다.")
                for item in menu_items:
                    try:
                        name = item.find_element(By.CSS_SELECTOR, pattern["name"]).text
                        price = item.find_element(By.CSS_SELECTOR, pattern["price"]).text
                        if name and price:
                            for word in ['사진', '대표', '인기']:
                                if name.startswith(word): name = name[len(word):].strip()
                            menu_entry = f"{name.strip()}: {price.strip()}"
                            if menu_entry not in menus_list: menus_list.append(menu_entry)
                    except NoSuchElementException:
                        continue
                if menus_list: break

        # 2순위: 위 특정 패턴에 해당하지 않으면, 범용적인 ul/li 구조의 내용을 분석 (봉이네 등)
        if not menus_list:
            print("INFO: [2순위] 특정 패턴 불일치. 범용 ul > li 구조의 내용을 분석합니다.")
            menu_uls = driver.find_elements(By.CSS_SELECTOR, "ul")
            for ul in menu_uls:
                try:
                    menu_items_in_ul = ul.find_elements(By.TAG_NAME, "li")
                    for item in menu_items_in_ul:
                        item_text = item.text
                        if not item_text or '원' not in item_text or '\n' not in item_text: continue

                        lines = [line.strip() for line in item_text.split('\n') if line.strip()]
                        if len(lines) < 2: continue

                        price = lines[-1]
                        if not ('원' in price and any(c.isdigit() for c in price.replace(',', ''))): continue

                        name = " ".join(lines[:-1])

                        if name and price:
                            for word in ['사진', '대표', '인기']:
                                if name.startswith(word): name = name[len(word):].strip()
                            menu_entry = f"{name.strip()}: {price.strip()}"
                            if menu_entry not in menus_list: menus_list.append(menu_entry)
                except Exception:
                    continue
        # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲ [핵심 수정] ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

        menus_string = ", ".join(menus_list)
        if menus_string:
            print(f"INFO: 메뉴 수집 성공! ({len(menus_list)}개) -> {menus_string}")
        else:
            print("WARNING: 메뉴 정보를 수집하지 못했습니다.")

        return {'주소': address, '전화번호': phone, '카테고리': category, '메뉴': menus_string}

    except WebDriverException as e:
        raise e
    except Exception as e:
        print(f"'{search_query}' 처리 중 예측하지 못한 오류 발생: {e}")
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

            if (index + 1) % 10 == 0:
                f.flush()
                print(f"================== INFO: {index + 1}건 처리 완료. 파일에 중간 저장되었습니다. ==================")

        driver.quit()

    print(f"\n✅ 모든 작업 완료! 결과가 '{output_filename}' 파일에 저장되었습니다.")