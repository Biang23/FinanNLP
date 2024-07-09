# -*- coding:utf-8 -*-
# @author: Young
# @date: 2024-07-09 16:04:50

import re
import random
import time
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import pandas as pd
from tqdm import tqdm

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

def check_date(cur_date: str, pre_date: str) -> bool:
    if not pre_date:
        return True
    cur_month, cur_day = int(cur_date.split(" ")[0].split("-")[0]), int(cur_date.split(" ")[0].split("-")[-1])
    pre_month, pre_day = int(pre_date.split(" ")[0].split("-")[0]), int(pre_date.split(" ")[0].split("-")[-1])

    if abs(cur_month-pre_month) > 1:
        return False
    elif cur_month == pre_month and abs(cur_day-pre_day) > 5:
        return False
    elif cur_month != pre_month and (cur_day not in [28, 29, 30, 31, 1, 2] or pre_day not in [28, 29, 30, 31, 1, 2]):
        return False
    else:
        return True

def eastmoney_guba_spider(code, time_frame):
    flag = True

    if code == 'sh000001':
        code = 'zssh000001'
    elif 'sh' in code:
        code = code.replace('sh', '')

    time_split = [split for split in range(0, 6000, 30)]
    for start in time_split:
        results = defaultdict(list)
        for page in tqdm(range(start, start+30)):
            if flag:
                page += 1
                url = "http://guba.eastmoney.com/list," + str(code) + "_" + str(page) + ".html"

                headers = {
                    'User-Agent': random.choice(user_agents),
                    'Referer': 'https://www.example.com/',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0',
                    'DNT': '1',
                }
                try:
                    res = requests.get(url, headers=headers, timeout=10).text

                except:
                    continue
                bs = BeautifulSoup(res, 'html.parser')
                for i, script in enumerate(bs.find('script', string=re.compile(r'var article_list'))):

                    match = re.search(r'var article_list\s*=\s*(\{.*?\});', script.string, re.DOTALL)
                    if match:
                        article_list_json = match.group(1)

                        article_list = json.loads(article_list_json)
                    else:
                        print(f"article_list not found in index {i} in {url}")
                        continue

                    for article in article_list['re']:
                        try:
                            results['context'].append(article['post_title'])
                            results['date_time'].append(article['post_display_time'])
                        except:
                            continue 
            
            else:
                break
            time.sleep(random.randint(8, 15))

        print(datetime.now())
    
        df = pd.DataFrame(results)
        df.to_csv(f"data/{code}_{len(df)}_{start}-{start+30}_version2.csv", index=False)
        print(datetime.now())
        print("全部存储完成。")
        del df
        del results
        time.sleep(30)


if __name__ == "__main__":
    eastmoney_guba_spider('sh000001', 3)
