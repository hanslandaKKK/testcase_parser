import json
import asyncio
import re
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright, Playwright

async def get_detailed_format(client, url: str) -> str:
    try:
        ua = UserAgent(browsers=['Firefox', 'Chrome'])
        headers = {"User-Agent": f"{ua.random}"}
        
        response = await client.get(url, timeout=10, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            elements = soup.select('.dl_element')
            for el in elements:
                spans = el.find_all('span')
                if len(spans) >= 2:
                    label = spans[0].get_text(strip=True)
                    value = spans[1].get_text(strip=True)
                    if "Формат" in label:
                        return re.sub(r'[^0-9*.,]', '', value)
    except Exception as e:
        print(f"Ошибка на {url}: {e}")
    return ""

async def transform_item(client, raw_item: dict) -> dict:
    gid = str(raw_item.get("ID", ""))
    side = raw_item.get("PROPERTY_SIDE_VALUE")
    code = raw_item.get("CODE")
    url = f'https://boards.by/banner/{code}/?side={side}'
    
    # size = await get_detailed_format(client, url) 
    # В json формате не возращаются данные о размерах, но можно добавить их парсинг с каждой страницы, но это очень долго даже с асинхронном, поэтому я добавил ссылку, где можно изучить в случае чего. В небольших НП оно быстро собирает, но в минске с 1000+ будет долго по времени.   

    return {
        "gid": gid,
        "address": raw_item.get("NAME", ""),
        "name": side if side else gid,  
        "lon": float(raw_item.get("PROPERTY_LONGITUDE_VALUE") or 0),
        "lat": float(raw_item.get("PROPERTY_LATITUDE_VALUE") or 0),
        "construction_format": raw_item.get("PROPERTY_TYPE_VALUE", ""),
        "display_type": None, 
        "lighting": None, 
        "size": None,       
        "material": None, 
        "url": url 
    }

async def process_raw_data(raw_data):
    items_to_process = raw_data.values() if isinstance(raw_data, dict) else (raw_data if isinstance(raw_data, list) else [])
    sem = asyncio.Semaphore(10) 
    
    async def sem_task(client, item):
        async with sem:
            return await transform_item(client, item)

    async with httpx.AsyncClient() as client:
        tasks = [sem_task(client, item) for item in items_to_process if isinstance(item, dict) and "ID" in item]
        return await asyncio.gather(*tasks)

def save_data(data_list, filename_base, format_choice):
    if format_choice in ["1", "3"]:
        filename = f"{filename_base}.json"
        final_output = {"construction_sides": [i for i in data_list if i]}
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, ensure_ascii=False, indent=4)
        print(f"[!] Файл {filename} сохранен.")

    if format_choice in ["2", "3"]:
        filename = f"{filename_base}.xlsx"
        df = pd.DataFrame([i for i in data_list if i])
        df.to_excel(filename, index=False)
        print(f"[!] Файл {filename} сохранен.")

async def run(p: Playwright) -> None:
    browser = await p.chromium.launch(headless=False)
    page = await browser.new_page()

    print("Ожидайте пожалуйста, загрузка сайта...")
    try:
        await page.goto("https://boards.by", wait_until="load", timeout=60000)
        await page.wait_for_selector(".ms-parent")
        await page.click(".ms-parent") 
        
        items_locator = page.locator(".ms-drop ul li:not(.group) label")
        await items_locator.first.wait_for(state="visible")
        all_items = await items_locator.all()
        cities_elements = all_items[:42]
        
        print(f"\nНайдено городов: {len(cities_elements)}")
        for idx, el in enumerate(cities_elements, 1):
            print(f"{idx}. {await el.inner_text()}")
        print("43. Собрать данные по ВСЕМ городам")

        city_choice = input("\nВведите номер города или 43: ").strip()
        
        print("\nВыберите формат выгрузки:")
        print("1 - JSON\n2 - EXCEL\n3 - ОБА")
        format_choice = input("Ваш выбор: ").strip()

        target_data = {}
        filename_prefix = ""

        if city_choice == "43":
            filename_prefix = "all_cities"
            for el in cities_elements:
                city_name = await el.inner_text()
                try:
                    async with page.expect_response("**/map.php*", timeout=20000) as resp:
                        await el.click()
                        res_json = await (await resp.value).json()
                        if isinstance(res_json, dict):
                            target_data.update(res_json)
                            print(f"[+] Добавлен: {city_name.strip()}")
                    await page.wait_for_timeout(300)
                except Exception as e:
                    print(f"[-] Пропуск {city_name.strip()}: {e}")
            
        elif city_choice.isdigit() and 1 <= int(city_choice) <= 42:
            target_el = cities_elements[int(city_choice)-1]
            city_name = (await target_el.inner_text()).strip()
            filename_prefix = city_name
            print(f"Сбор данных для: {city_name}...")
            async with page.expect_response("**/map.php*") as resp:
                await target_el.click()
                target_data = await (await resp.value).json()

        if target_data:
            transformed_list = await process_raw_data(target_data)
            save_data(transformed_list, filename_prefix, format_choice)
            print(f"\nГотово! Обработано билбордов: {len(transformed_list)}")

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await browser.close()

async def main():
    async with async_playwright() as p:
        await run(p)

if __name__ == "__main__":
    asyncio.run(main())