import queue
import re
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import pandas as pd
import undetected_chromedriver as ucd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

QUERY = "компьютер"
TARGET_CNT = 1000
NUM_SESSIONS = 2
XPATH_EXPRESSIONS = {
    "raiting": [
        "/html/body/div[1]/div/div[1]/div[4]/div[3]/div[1]/div[1]/div[2]/div/div[2]/div[1]/a/div"
    ],
    "price(RUB)": [
        "/html/body/div[1]/div/div[1]/div[4]/div[3]/div[2]/div[1]/div[3]/div/div[1]/div/div/div[1]/div[1]/button/span/div/div[1]",
        "/html/body/div[1]/div/div[1]/div[4]/div[3]/div[2]/div[1]/div[2]/div/div[1]/div/div/div[1]/div[1]/button/span/div/div[1]/div/div/span",
    ],
}
CHARACTERISTICS_STR_SUB = {
    "Артикул": "articul",
    "Процессор": "CPU",
    "Частота процессора, ГГц": "CPU frequency(GHZ)",
    "Число ядер процессора": "CPU cores",
    "Оперативная память": "RAM(GB)",
    "Тип памяти": "RAM type",
    "Общий объем SSD, ГБ": "SSD(GB)",
    "Видеокарта": "GPU",
    "Видеопамять": "VRAM(GB)",
    "Мощность блока питания, Вт": "power supply(W)",
}
PAIRED_ATTRIBUTES = {
    "CPU cores": ("CPU", {}),
    "CPU frequency(GHZ)": ("CPU", {}),
    "VRAM(GB)": ("GPU", {}),
}


def vram_fmt(text):
    buf = re.sub(r"\D", "", text)
    return buf if buf != "" else "-/-"


def rating_fmt(text):
    match = re.search(r"\d+(\.\d+)?", text)
    if match:
        return match.group()
    return "-/-"


def cpu_fmt(text):
    return text.lower().replace(" ", "")


def gpu_fmt(text):
    match = re.search(r"^[^\(]+", text)
    if match:
        cleaned_text = match.group().strip().lower()
        return cleaned_text.replace(" ", "")
    return "-/-"


DATA_FORMAT = {
    "price(RUB)": lambda s: re.sub(r"\D", "", s),
    "RAM(GB)": lambda s: re.sub(r"\D", "", s),
    "VRAM(GB)": vram_fmt,
    "raiting": rating_fmt,
    "CPU": cpu_fmt,
    "GPU": gpu_fmt,
}

target_url = "https://www.ozon.ru"
section_characteristics_id = "section-characteristics"
section_characteristics_xpath1 = "/html/body/div[1]/div/div[1]/div[6]/div/div[1]/div[3]/div[2]/div/div/div[3]/div/div[2]/div[3]/div[2]"
section_characteristics_xpath2 = "/html/body/div[1]/div/div[1]/div[6]/div/div[1]/div[3]/div[2]/div/div/div[3]/div/div[2]/div[3]/div[3]"
next_page_xpath = (
    "/html/body/div[1]/div/div[1]/div[2]/div[2]/div[2]/div[4]/div[2]/div/div/a"
)


result = []
gdl = Lock()
produced_cnt = 0


def driver_init(headless=False):
    user_agent = UserAgent().random
    chrome_options = Options()
    chrome_options.add_argument(f"user-agent={user_agent}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument(f"user-agent={user_agent}")
    driver = ucd.Chrome(options=chrome_options)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": """
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """
        },
    )
    return driver


def main_QUERY(driver, search_QUERY):
    main_driver.get(target_url)
    time.sleep(5)
    search_box = driver.find_element(By.NAME, "text")
    search_box.send_keys(search_QUERY)
    search_box.send_keys(Keys.RETURN)


def scrolldown(driver, deep):
    for _ in range(deep):
        driver.execute_script("window.scrollBy(0, 500)")
        time.sleep(0.1)


def nextpage(driver):
    next_page_ref = driver.find_element(By.XPATH, next_page_xpath).get_attribute("href")
    driver.get(next_page_ref)
    time.sleep(5)


def extruct_links(driver):
    main_page_html = BeautifulSoup(driver.page_source, "html.parser")
    links = set()
    result_containers_yj4_23 = (
        main_page_html.find("div", {"class": "container"})
        .find_all("div", {"class": "e1"})[1]
        .find_all("div", {"class": "c8"})[1]
        .find("div", {"class": "e7s"})
        .find("div", {"id": "paginatorContent"})
        .find_all("div", {"class": "widget-search-result-container y8j_23"})
    )
    for container_y4j_23 in result_containers_yj4_23:
        y4j_23 = container_y4j_23.find("div", {"class": "jy9_23"})
        for j4q_23_qj4_23 in y4j_23.find_all("div", {"class": "rj3_23 r3j_23"}):
            refs = [
                "s2j_23 js3_23 tile-hover-target sj3_23",
                "js0_23 j0s_23 tile-hover-target",
            ]
            for ref in refs:
                try:
                    links.add(
                        j4q_23_qj4_23.find("div", {"class": "jr4_23"})
                        .find("a", {"class": ref})
                        .get("href")
                    )
                except:
                    pass
    return links


def extruct_data_impl(sess_queue, url):
    driver = sess_queue.get()
    driver.get(target_url + url)
    data = {}
    scrolldown(driver, 10)
    time.sleep(0.1)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, section_characteristics_id))
    )
    try:
        for k, v in XPATH_EXPRESSIONS.items():
            found = False
            for vi in v:
                try:
                    element = driver.find_element(By.XPATH, vi)
                    if element:
                        f = DATA_FORMAT.get(k, lambda x: x)
                        data[k] = f(element.text)
                        found = True
                        break
                except:
                    pass
            if not found:
                data[k] = "-/-"
        characteristics1 = driver.find_element(By.XPATH, section_characteristics_xpath1)
        characteristics2 = driver.find_element(By.XPATH, section_characteristics_xpath2)
        characteristics1_html = characteristics1.get_attribute("outerHTML")
        characteristics2_html = characteristics2.get_attribute("outerHTML")
        characteristics1_soup = BeautifulSoup(characteristics1_html, "html.parser")
        characteristics2_soup = BeautifulSoup(characteristics2_html, "html.parser")
        not_used = set(CHARACTERISTICS_STR_SUB.values())
        for dl_tag in characteristics1_soup.find_all(
            "dl"
        ) + characteristics2_soup.find_all("dl"):
            dl_text = dl_tag.get_text(separator="$", strip=True)
            kv = dl_text.split("$")
            if kv[0] in CHARACTERISTICS_STR_SUB.keys():
                col_name = CHARACTERISTICS_STR_SUB[kv[0]]
                f = DATA_FORMAT.get(col_name, lambda x: x)
                try:
                    data[col_name] = f(kv[1])
                except:
                    data[col_name] = kv[1]
                not_used.remove(col_name)
        for i in not_used:
            data[i] = "-/-"
        with gdl:
            global produced_cnt
            result.append(data)
            produced_cnt += 1
        print(f"One more done. Total: {produced_cnt}")
    except:
        pass
    finally:
        sess_queue.put(driver)


def extruct_data(urls, num_session, sess_queue):
    with ThreadPoolExecutor(max_workers=num_session) as executor:
        executor.map(lambda url: extruct_data_impl(sess_queue, url), urls)


main_driver = driver_init()
main_QUERY(main_driver, QUERY)
scrolldown(main_driver, 100)


while TARGET_CNT > produced_cnt:
    nextpage(main_driver)
    scrolldown(main_driver, 100)
    links = list(extruct_links(main_driver))
    print(f"Extruction complited -> {str(len(links))} links were engathered")
    session_queue = queue.Queue()
    for _ in range(NUM_SESSIONS):
        session = driver_init(headless=True)
        session_queue.put(session)
        print("Session invoked successfully")
    extruct_data(links, NUM_SESSIONS, session_queue)
    while not session_queue.empty():
        session = session_queue.get()
        print("Session closed")
        session.quit()

df = pd.DataFrame(result)

for row in result:
    for k, v in PAIRED_ATTRIBUTES.items():
        if row[k] != "-/-" and row[v[0]] != "-/-":
            v[1][row[v[0]]] = row[k]

for row in result:
    for k, v in PAIRED_ATTRIBUTES.items():
        if row[k] == "-/-":
            row[k] = v[1].get(row[v[0]], "-/-")

df = pd.DataFrame(result)

most_frequent = {}
for column in df.columns:
    filtered_column = df[column][df[column] != "-/-"]
    if not filtered_column.empty:
        most_frequent[column] = filtered_column.mode()[0]

for column in df.columns:
    df[column] = df[column].replace("-/-", most_frequent[column])


df.to_csv("scraped_data.csv", index=False)
