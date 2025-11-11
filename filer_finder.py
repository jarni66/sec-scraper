import requests
import os
import json
import argparse

parser = argparse.ArgumentParser(description="Split a JSON list into multiple chunked JSON files.")
parser.add_argument("--chunk")
args = parser.parse_args()

with open(f"cik_chunks/chunk_{args.chunk}.json", "r", encoding="utf-8") as f:
    chunk_data = json.load(f)

def get_filer(cik):
    headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,ko;q=0.8,id;q=0.7,de;q=0.6,de-CH;q=0.5',
            'origin': 'https://www.sec.gov',
            'priority': 'u=1, i',
            'referer': 'https://www.sec.gov/',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mario bot project resnatim@gmail.com',
        }
    response = requests.get(f'https://data.sec.gov/submissions/CIK{cik}.json', headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        filer_name = response_data.get('name')
        print(cik, response.status_code)

        return filer_name
    else:
        print(cik, response.status_code)
    
def run_chunk():
    for c in chunk_data:
        c['filer_name'] = get_filer(c.get('cik'))

        with open(f"cik_chunks/chunk_{args.chunk}_update.json", "w", encoding="utf-8") as f:
            json.dump(chunk_data, f, ensure_ascii=False, indent=4)


run_chunk()