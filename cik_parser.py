import requests
import json

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
        return filer_name
    else:
        print(response.status_code)
        return ""


with open("cik-lookup-data.txt", "r") as f:
    lines = f.readlines()

with open("cik_all.json", "r", encoding="utf-8") as f:
    cik_dumps = json.load(f)

print("cik_all.json len:", len(cik_dumps))
all_cik = [i.get('cik') for i in cik_dumps]
print(all_cik[:100])
for line in lines:
    name = line.split(':')[0].strip()
    cik = line.split(':')[1].strip()
    if cik not in all_cik:
        print("GET CIK :",cik)
        cik_dumps.append(
            {
                "name" : name,
                "cik" : cik,
                "filer_name" :get_filer(cik)
            }
        )
        with open("cik_all2.json", "w", encoding="utf-8") as f:
            json.dump(cik_dumps, f, indent=4)
    else:
        print("EXIST CIK :",cik)