import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from datetime import datetime
import numpy as np
import re
import xml.etree.ElementTree as ET
import config
import traceback
import llm_parser
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import update_gdrive

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
        'user-agent': 'Mario bot project nizar.rizax@gmail.com',
    }



class ProcessCIK:
    def __init__(self, cik: str, cik_file:str):

        self.cik = cik
        self.business_address = ''
        self.mailing_address = ''

        self.records = []
        self.cik_file = f"cik_forms/{cik_file}.csv"

    
    def save_form(self):
        if self.records:
            df = pd.DataFrame(self.records)
            op_path = self.cik_file

            # If file exists → append without headers
            if os.path.exists(op_path):
                df.to_csv(op_path, mode="a", index=False, header=False)
            else:
                df.to_csv(op_path, index=False)


    def cik_form(self):
        """
        This function use to get company forms data by using CIK.
        """

        session = requests.session()
        response = session.get(f'https://data.sec.gov/submissions/CIK{self.cik}.json', headers=headers)
        print("Get json forms", response.status_code)
        if response.status_code == 200:
            # If return success the result will in json format
            form_result = response.json()

            # State business and mailing address in empty string, in case there is no data.
            
            try:
                self.business_address = ', '.join([i for i in list(form_result['addresses']['business'].values()) if i ])
            except:
                pass
            
            try:
                self.mailing_address = ', '.join([i for i in list(form_result['addresses']['mailing'].values()) if i ])
            except:
                pass

            # Load json data to dataframe for easier iteration
            df_forms = pd.DataFrame(form_result['filings']['recent'])
            df_forms['cik'] = self.cik
            df_forms['business_address'] = self.business_address
            df_forms['mailing_address'] = self.mailing_address
            df_forms = df_forms[df_forms['form'].str.contains('13F')]
            
            records = df_forms.to_dict(orient="records")
            print(f"Length ({self.cik}) ACSN to process", len(records))
            for r in records:
                process_acsn = ProcessACSN(r)
                process_acsn.run()
                self.records.append(process_acsn.record)

        else:
            print(response.status_code)

    def run(self):
        self.cik_form()
        self.save_form()


class ProcessACSN:
    def __init__(self, row : dict):
        self.record = row
        self.acsn = self.record.get('accessionNumber')
        self.cik = self.record.get('cik').zfill(10)
        self.info_table_records = []
        self.full_txt_url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}.txt"
        rep_date = self.record.get("reportDate").replace('-','_')
        self.file_prefix = f"{self.cik}_{self.acsn.replace('-','_')}_{rep_date}"

        self.raw_path = f"raw_txt/{rep_date}"
        os.makedirs(self.raw_path, exist_ok=True)

        file_name = self.file_prefix + '.parquet'
        self.op_path = f"sec_forms/{rep_date}/{file_name}"

     


    def save_data(self):
        """
        Save result data to parquet file to the bucket
        """
        col_names = [
                "name_of_issuer",
                "title_of_class",
                "cusip",
                "figi",
                "value",
                "shares_or_percent_amount",
                "shares_or_percent_type",
                "put_call",
                "investment_discretion",
                "other_manager",
                "voting_authority_sole",
                "voting_authority_shared",
                "voting_authority_none",
        ]
        
        rep_date = self.record.get("reportDate").replace('-','_')
        file_name = self.file_prefix + '.parquet'
        date_path = f"sec_forms/{rep_date}"
        os.makedirs(date_path, exist_ok=True)


        op_path = f"sec_forms/{rep_date}/{file_name}"
        
        df = pd.DataFrame(self.info_table_records, columns=col_names, dtype=str)
        # df = df.replace("", np.nan)
        df = df.replace("", np.nan).infer_objects(copy=False)

        df = df.dropna(subset=['name_of_issuer'])

        df['business_address'] = self.record.get("business_address")
        df['mailing_address'] = self.record.get("mailing_address")
        df['report_date'] = self.record.get("reportDate")
        df['filling_date'] = self.record.get("filingDate")
        df['acsn'] = self.record.get("accessionNumber")
        df['cik'] = self.record.get("cik")
        df['value_multiplies'] = self.record.get("value_multiplies")
        df['scraping_url'] = self.full_txt_url
        df['scraping_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            cols_to_clean = [
                "value", 
                "shares_or_percent_amount", 
                "voting_authority_sole",
                "voting_authority_shared",
                "voting_authority_none"]

            for col in cols_to_clean:
                df[col] = df[col].str.replace(",", "")

            # Checking column if not exist fill with nan
            for column, dtype in config.bq_dtype.items():
                if column not in df.columns:
                    df[column] = np.nan

            try:
                df = df.astype(config.bq_dtype)
                print(f"SAVED {op_path}", len(df))
                
                df.to_parquet(self.op_path, index=False)
                self.record['upload_to_bucket_status'] = ""
                self.record['table_path'] = op_path
            except Exception as e:
                err = traceback.format_exc()
                print(f"SAVED {op_path}", len(df))
                df = pd.DataFrame(columns=df.columns.tolist())
                df.to_parquet(self.op_path, index=False)
                self.record['upload_to_bucket_status'] = err
                self.record['table_path'] = op_path

            
        except Exception as e:
            err = traceback.format_exc()
            error = f"Error in save_data : {str(err)}"
            print(error)
            df.to_csv(f"failed/{file_name.replace('.parquet','.csv')}", index=False)
            self.record['table_path'] = f"failed/{file_name.replace('.parquet','.csv')}"
            self.record['upload_to_bucket_status'] = error

            
    def check_value_multiplies(self,url):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            text_body = response.text
            match = re.search(r'x\$(\d+)', text_body)
            if match:
                return int(match.group(1))
            # Check for [thousands]
            if "[thousands]" in text_body.lower():
                return 1000
            return 1
        else:
            print("Check value multiplies",response.status_code)

            return 1


    def get_html_info_table(self):
        url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}-index.html"
        response = requests.get(url, headers=headers)
        result = {
            "info_html" : "",
            "complete_txt" :""
        }
        if response.status_code == 200:
            text_body = response.text
            soup = BeautifulSoup(text_body, 'html.parser')
            table = soup.find('table')
            if table:
                all_tr = table.find_all('tr')
                for tr in all_tr:
                    a_tag = tr.find('a')
                    tr_str = str(tr).lower()
                    if ('table' in tr_str) and ('.html' in a_tag.text):
                        result['info_html'] = "https://www.sec.gov" + a_tag.attrs.get('href')
                        # return "https://www.sec.gov" + a_tag.attrs.get('href')

                a_tag_last = all_tr[-1].find('a')
                if a_tag_last:
                    result['complete_txt'] = "https://www.sec.gov" + a_tag_last.attrs.get('href')
        else:
            print("Get html info table url", response.status_code)

        return result

    def has_no_alphabet(self, s) -> bool:
        """
        Check if a string has no alphabet characters.
        
        Args:
            s (str): Input string
            english_only (bool): If True, only checks A-Z letters.
                                If False, includes all Unicode alphabets.
        
        Returns:
            bool: True if no alphabet characters found, False otherwise.
        """
        return not re.search(r"[A-Za-z]", s)
       
    def get_parser(self,req_text):
        records = [
            self.parser(req_text),
            self.parser2(req_text),
            self.parser3(req_text),
            # self.parser4(req_text),
        ]
        result = []
        for i in range(len(records)):
            
            if len(records[i]) != 0:
                self.record['parser'] = i+1
                result = records[i]
                print(f"Parser {i+1} : ", len(records[i]))
                break
        if result:
            return result
        else:
            self.record['parser'] = 4
            return self.parser_llm(req_text)
            # return []

    def parser(self, req_text):
        # self.record['parser'] = 1
        match = re.search(r'(<[\w:]*informationTable[\s\S]*?</[\w:]*informationTable>)', req_text)
        if not match:
            return []

        xml_block = match.group(1)

        # Parse XML
        root = ET.fromstring(xml_block)

        # Detect namespace (works for default or prefixed)
        def get_namespace(tag):
            if tag[0] == "{":
                return tag[1:].split("}")[0]
            return ""

        ns_uri = get_namespace(root.tag)
        ns = {"ns": ns_uri} if ns_uri else {}

        # Helper to build full tag path with namespace
        def ns_tag(tag_name):
            return f"ns:{tag_name}" if ns else tag_name

        data = []
        for info in root.findall(ns_tag("infoTable"), ns):
            entry = {
                "name_of_issuer": info.findtext(ns_tag("nameOfIssuer"), default="", namespaces=ns).strip(),
                "title_of_class": info.findtext(ns_tag("titleOfClass"), default="", namespaces=ns).strip(),
                "cusip": info.findtext(ns_tag("cusip"), default="", namespaces=ns).strip(),
                "figi": info.findtext(ns_tag("figi"), default="", namespaces=ns).strip(),
                "value": info.findtext(ns_tag("value"), default="", namespaces=ns).strip(),
                "shares_or_percent_amount": info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamt"), ns).text.strip() if info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamt"), ns) is not None else "",
                "shares_or_percent_type": info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamtType"), ns).text.strip() if info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamtType"), ns) is not None else "",
                "put_call": info.findtext(ns_tag("putCall"), default="", namespaces=ns).strip(),
                "investment_discretion": info.findtext(ns_tag("investmentDiscretion"), default="", namespaces=ns).strip(),
                "other_manager": info.findtext(ns_tag("otherManager"), default="", namespaces=ns).strip(),
                "voting_authority_sole": info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("Sole"), default="0", namespaces=ns).strip(),
                "voting_authority_shared": info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("Shared"), default="0", namespaces=ns).strip(),
                "voting_authority_none":info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("None"), default="0", namespaces=ns).strip(),
            }
            entry['value'] = entry['value'].replace(',','')
            entry['shares_or_percent_amount'] = entry['shares_or_percent_amount'].replace(',','')
            entry['voting_authority_sole'] = entry['voting_authority_sole'].replace(',','')
            entry['voting_authority_shared'] = entry['voting_authority_shared'].replace(',','')
            entry['voting_authority_none'] = entry['voting_authority_none'].replace(',','')
            data.append(entry)

        return data

    def parser2(self, req_text):

        records = []
        try:
            # self.record['parser'] = 3
            # table_texts = re.findall(r"<TABLE>(.*?)</TABLE>", req_text, flags=re.S)
            table_texts = re.findall(r"<table>(.*?)</table>", req_text, flags=re.S | re.I)

            # match = re.search(r"<TABLE>([\s\S]*?)</TABLE>", req_text, re.IGNORECASE)
            if not table_texts:
                return []
            for table_str in table_texts:
                # table_block = match.group(1)
                if 'issuer' in table_str.lower():
                    lines = table_str.strip("\n").splitlines()

                    # Find where the header line starts
                    header_index = None
                    for i, line in enumerate(lines):
                        if "<c>" in line.strip().lower():
                            header_index = i
                            # line += "  <C>"
                            lines[i] = line
                            break
                    
                    if header_index is None:
                        raise ValueError("No header line with <S> found.")

                    # Slice the lines to only include table section
                    table_lines = lines[header_index:]
                    header_line = table_lines[0]

                    # Get positions of <C> markers
                    col_positions = [m.start() for m in re.finditer("<", header_line)]

                    data_lines = table_lines[1:]

                    # 1️⃣ Uniform row length: find longest row
                    max_len = max(len(line) for line in data_lines)

                    # 2️⃣ Filter out rows that are < 30% of max_len (after stripping)
                    threshold = max_len * 0.3
                    data_lines = [line for line in data_lines if len(line.strip()) >= threshold]

                    # 3️⃣ Pad all remaining rows with spaces to max_len
                    data_lines = [line.ljust(max_len) for line in data_lines]

                    # with open("data_lines.json", "w", encoding="utf-8") as f:
                    #     json.dump(data_lines, f, ensure_ascii=False, indent=4)

                    entry_list = [
                            "name_of_issuer",
                            "title_of_class",
                            "cusip",
                            "value",
                            "shares_or_percent_amount",
                            "shares_or_percent_type",
                            "put_call",
                            "investment_discretion",
                            "other_manager",
                            "voting_authority_sole",
                            "voting_authority_shared",
                            "voting_authority_none",
                    ]

                    # print("COL :",col_positions)
                    # print("COL LEN:",len(col_positions))
                    for row in data_lines:
                        if not row.strip():
                            continue
                        entry = {
                            "name_of_issuer": "",
                            "title_of_class": "",
                            "cusip":"",
                            "figi": "",
                            "value": "0",
                            "shares_or_percent_amount": "0",
                            "shares_or_percent_type": "",
                            "put_call": "",
                            "investment_discretion": "",
                            "other_manager": "",
                            "voting_authority_sole": "0",
                            "voting_authority_shared": "0",
                            "voting_authority_none": "0",
                        }
                        row += "     "
                        for i, start in enumerate(col_positions):

                            try:
                                # print(i, start)
                                left = start
                                # expand left only if the current left is not after a space
                                if left > 0 and not row[left].isspace():
                                    while left > 0 and not row[left-1].isspace():
                                        left -= 1


                                # right boundary
                                if i+1 < len(col_positions):
                                    right = col_positions[i+1]
                                    # move left until previous char is space
                                    while right > left and not row[right].isspace():
                                        right -= 1
                                else:
                                    # last column: take everything to end
                                    right = len(row)

                                    
                                value = row[left:right].strip()

                                entry[entry_list[i]] = value
                            except Exception as e:
                                pass
                        # print(entry)
                        entry['value'] = entry['value'].replace(',','')
                        entry['value'] = entry['value'] if entry['value'].strip() != "" else '0'
                        entry['value'] = float(entry['value'])

                        entry['shares_or_percent_amount'] = entry['shares_or_percent_amount'].replace(',','')
                        entry['shares_or_percent_amount'] = entry['shares_or_percent_amount'] if entry['shares_or_percent_amount'].strip() != "" else '0'
                        entry['shares_or_percent_amount'] = float(entry['shares_or_percent_amount'])

                        entry['voting_authority_sole'] = entry['voting_authority_sole'].replace(',','')
                        entry['voting_authority_sole'] = entry['voting_authority_sole'] if entry['voting_authority_sole'].strip() != "" else '0'
                        entry['voting_authority_sole'] = float(entry['voting_authority_sole'])

                        entry['voting_authority_shared'] = entry['voting_authority_shared'].replace(',','')
                        entry['voting_authority_shared'] = entry['voting_authority_shared'] if entry['voting_authority_shared'].strip() != "" else '0'
                        entry['voting_authority_shared'] = float(entry['voting_authority_shared'])

                        entry['voting_authority_none'] = entry['voting_authority_none'].replace(',','')
                        entry['voting_authority_none'] = entry['voting_authority_none'] if entry['voting_authority_none'].strip() != "" else '0'
                        entry['voting_authority_none'] = float(entry['voting_authority_none'])

                        voting_check = entry['voting_authority_sole'] + entry['voting_authority_shared'] + entry['voting_authority_none']
                        
                        if voting_check != 0:
                            records.append(entry)
            
        except:
            pass
        
        print("Parser 2 len :", len(records))
        return records

    def parser3(self, req_text):
        records = []
        # self.record['parser'] = 3
        try:
            # table_texts = re.findall(r"<TABLE>(.*?)</TABLE>", req_text, flags=re.S)
            table_texts = re.findall(r"<table>(.*?)</table>", req_text, flags=re.S | re.I)
            # match = re.search(r"<TABLE>([\s\S]*?)</TABLE>", req_text, re.IGNORECASE)
            if not table_texts:
                return []
            # table_block = match.group(1)
            for table_str in table_texts:
                if 'issuer' in table_str.lower():
                    lines = table_str.strip("\n").splitlines()

                    # Find where the header line starts
                    header_index = None
                    for i, line in enumerate(lines):
                        if "<c>" in line.strip().lower():
                            header_index = i
                            # line += "  <C>"
                            lines[i] = line + "  <C>"
                            break
                    
                    if header_index is None:
                        raise ValueError("No header line with <S> found.")

                    # Slice the lines to only include table section
                    table_lines = lines[header_index:]

                    # Get positions of <C> markers
                    data_lines = table_lines[1:]

                    # 1️⃣ Uniform row length: find longest row
                    max_len = max(len(line) for line in data_lines)

                    # 2️⃣ Filter out rows that are < 30% of max_len (after stripping)
                    threshold = max_len * 0.3
                    data_lines = [line for line in data_lines if len(line.strip()) >= threshold]

                    # 3️⃣ Pad all remaining rows with spaces to max_len
                    data_lines = [line.ljust(max_len) for line in data_lines]


                    def _strip_wrapping_quotes(line: str) -> str:
                        ln = line.strip()
                        # remove leading/trailing quotes and optional trailing comma if present
                        if ln.startswith('"') and ln.endswith('",'):
                            return ln[1:-2].rstrip()
                        if ln.startswith('"') and ln.endswith('"'):
                            return ln[1:-1]
                        return ln

                    def parse_line(line: str) -> dict:
                        ln = _strip_wrapping_quotes(line)

                        # 1) split into name, class, cusip, rest (name/class separated by 2+ spaces)
                        m = re.match(r'^\s*(?P<name>.+?)\s{2,}(?P<title>.+?)\s{2,}(?P<cusip>\S+)\s*(?P<rest>.*)$', ln)
                        if not m:
                            raise ValueError(f"Line didn't match expected layout: {line!r}")

                        name = m.group('name').strip()
                        title = m.group('title').strip()
                        cusip = m.group('cusip').strip()
                        rest = m.group('rest')

                        # 2) from rest, pull the first two numeric tokens (value, shares) if present
                        m2 = re.match(r'^\s*([\d,]+)?\s*([\d,]+)?\s*(.*)$', rest)
                        value = (m2.group(1) or "").strip()
                        shares_amt = (m2.group(2) or "").strip()
                        tail = m2.group(3)

                        # 3) collect all numeric tokens that remain (these are the trailing numbers)
                        numeric_tokens = re.findall(r'[\d,]+', tail)

                        # 4) remove the trailing numeric tokens from tail to isolate text tokens
                        if numeric_tokens:
                            tail = re.sub(r'(?:\s+[\d,]+){%d}\s*$' % len(numeric_tokens), '', tail)
                        text_part = tail.strip()

                        # 5) classify short text tokens:
                        #    - shares_or_percent_type: only recognize common tokens (SH, PRN, etc.)
                        #    - put_call: PUT/CALL/P/C
                        #    - the rest (joined) => investment_discretion
                        shares_or_percent_type = ""
                        put_call = ""
                        investment_discretion = ""
                        if text_part:
                            tokens = text_part.split()
                            for t in tokens:
                                up = t.upper()
                                if not shares_or_percent_type and up in {"SH", "PRN", "SHARES", "PRF"}:
                                    shares_or_percent_type = t
                                    continue
                                if not put_call and up in {"PUT", "CALL", "P", "C"}:
                                    put_call = t
                                    continue
                                # otherwise treat as part of investment_discretion (can be multi-word)
                                if investment_discretion:
                                    investment_discretion += " " + t
                                else:
                                    investment_discretion = t

                        # 6) assign trailing numeric tokens from the right:
                        #    other_manager, voting_authority_sole, voting_authority_shared, voting_authority_none
                        other_manager = ""
                        voting_authority_sole = ""
                        voting_authority_shared = ""
                        voting_authority_none = ""
                        if numeric_tokens:
                            voting_authority_none = numeric_tokens[-1]
                        if len(numeric_tokens) >= 2:
                            voting_authority_shared = numeric_tokens[-2]
                        if len(numeric_tokens) >= 3:
                            voting_authority_sole = numeric_tokens[-3]
                        if len(numeric_tokens) >= 4:
                            other_manager = numeric_tokens[-4]

                        return {
                            "name_of_issuer": name,
                            "title_of_class": title,
                            "cusip": cusip,
                            "value": value,
                            "shares_or_percent_amount": shares_amt,
                            "shares_or_percent_type": shares_or_percent_type,
                            "put_call": put_call,
                            "investment_discretion": investment_discretion,
                            "other_manager": other_manager,
                            "voting_authority_sole": voting_authority_sole,
                            "voting_authority_shared": voting_authority_shared,
                            "voting_authority_none": voting_authority_none,
                        }
                    
                    
                    for line in data_lines:
                        try:
                            entry = parse_line(line)
                            if entry.get('name_of_issuer','').lower() != "total":
                                entry['figi'] = ''

                                entry['value'] = entry['value'].replace(',','')
                                entry['value'] = entry['value'] if entry['value'].strip() != "" else '0'
                                entry['value'] = float(entry['value'])

                                entry['shares_or_percent_amount'] = entry['shares_or_percent_amount'].replace(',','')
                                entry['shares_or_percent_amount'] = entry['shares_or_percent_amount'] if entry['shares_or_percent_amount'].strip() != "" else '0'
                                entry['shares_or_percent_amount'] = float(entry['shares_or_percent_amount'])


                                entry['voting_authority_sole'] = entry['voting_authority_sole'].replace(',','')
                                entry['voting_authority_sole'] = entry['voting_authority_sole'] if entry['voting_authority_sole'].strip() != "" else '0'
                                entry['voting_authority_sole'] = float(entry['voting_authority_sole'])


                                entry['voting_authority_shared'] = entry['voting_authority_shared'].replace(',','')
                                entry['voting_authority_shared'] = entry['voting_authority_shared'] if entry['voting_authority_shared'].strip() != "" else '0'
                                entry['voting_authority_shared'] = float(entry['voting_authority_shared'])


                                entry['voting_authority_none'] = entry['voting_authority_none'].replace(',','')
                                entry['voting_authority_none'] = entry['voting_authority_none'] if entry['voting_authority_none'].strip() != "" else '0'
                                entry['voting_authority_none'] = float(entry['voting_authority_none'])

                                voting_check = entry['voting_authority_sole'] + entry['voting_authority_shared'] + entry['voting_authority_none']
                        
                                if voting_check != 0:
                                    records.append(entry)

                                # records.append(entry)
                        except:
                            pass
                
        except:
            pass
        return records

    def parser_llm(self, req_text):
        table_res = []
        
        table_texts = re.findall(r"<table>(.*?)</table>", req_text, flags=re.S | re.I)
        if table_texts:
            for table_str in table_texts:
                if ('issuer' in table_str.lower()) and ('cusip' in table_str.lower()):
                    print("Running extract llm")
                    try:
                        extract_res = asyncio.run(llm_parser.get_records(table_str))
                        table_res += extract_res
                    except Exception as e:
                        print("Error llm parser :", e)

        elif ('issuer' in req_text.lower()) and ('cusip' in req_text.lower()):
            print("Running extract llm")
            try:
                extract_res = asyncio.run(llm_parser.get_records(req_text))
                table_res += extract_res
            except Exception as e:
                print("Error llm parser :", e)

        for entry in table_res:
            entry['value'] = entry['value'].replace(',','')
            entry['value'] = entry['value'] if entry['value'].strip() != "" else '0'
            entry['value'] = float(entry['value'])

            entry['shares_or_percent_amount'] = entry['shares_or_percent_amount'].replace(',','')
            entry['shares_or_percent_amount'] = entry['shares_or_percent_amount'] if entry['shares_or_percent_amount'].strip() != "" else '0'
            entry['shares_or_percent_amount'] = float(entry['shares_or_percent_amount'])


            entry['voting_authority_sole'] = entry['voting_authority_sole'].replace(',','')
            entry['voting_authority_sole'] = entry['voting_authority_sole'] if entry['voting_authority_sole'].strip() != "" else '0'
            entry['voting_authority_sole'] = float(entry['voting_authority_sole'])


            entry['voting_authority_shared'] = entry['voting_authority_shared'].replace(',','')
            entry['voting_authority_shared'] = entry['voting_authority_shared'] if entry['voting_authority_shared'].strip() != "" else '0'
            entry['voting_authority_shared'] = float(entry['voting_authority_shared'])


            entry['voting_authority_none'] = entry['voting_authority_none'].replace(',','')
            entry['voting_authority_none'] = entry['voting_authority_none'] if entry['voting_authority_none'].strip() != "" else '0'
            entry['voting_authority_none'] = float(entry['voting_authority_none'])

        return table_res

    def get_info_table(self):
        self.record['raw_txt'] = ''
        url_text = self.full_txt_url
        response = requests.get(url_text, headers=headers)
        if response.status_code == 200:
            text_body = response.text
            raw_path = f"{self.raw_path}/{self.file_prefix}.txt"
            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(text_body)

            self.record['raw_txt'] = raw_path
            # print("raw text :", text_body[:100])
            info_table_records = self.get_parser(text_body)
            return info_table_records
        else:
            print("Get info table full text", response.status_code)


    def run(self):
        print("Process :",self.cik, self.acsn)
        self.record['bq_status'] = 'process'
        try:
            index_res = self.get_html_info_table()
            if index_res.get("complete_txt"):
                self.full_txt_url = index_res.get("complete_txt")

            url_html_info_table = index_res.get('info_html')
            if url_html_info_table:
                self.record['url_html_info_table'] = url_html_info_table
                self.record['value_multiplies'] = self.check_value_multiplies(url_html_info_table)
            else:
                self.record['url_html_info_table'] = ""
                self.record['value_multiplies'] = self.check_value_multiplies(self.full_txt_url)

            self.info_table_records = self.get_info_table()
            self.record['info_table_len'] = len(self.info_table_records)
            self.save_data()
            self.record['process'] = ''
        except Exception as e:
            print("error process", e)
            err = traceback.format_exc()
            self.record['process'] = str(err)

    def run_check_multiplies(self):
        index_res = self.get_html_info_table()
        if index_res.get("complete_txt"):
            self.full_txt_url = index_res.get("complete_txt")


        url_html_info_table = index_res.get('info_html')
        if url_html_info_table:
            self.record['url_html_info_table'] = url_html_info_table
            self.record['value_multiplies'] = self.check_value_multiplies(url_html_info_table)
        else:
            self.record['url_html_info_table'] = ""
            self.record['value_multiplies'] = self.check_value_multiplies(self.full_txt_url)


    def run_acsn(self):
        try:
            index_res = self.get_html_info_table()
            if index_res.get("complete_txt"):
                self.full_txt_url = index_res.get("complete_txt")

            url_html_info_table = index_res.get('info_html')
            if url_html_info_table:
                self.record['url_html_info_table'] = url_html_info_table
                self.record['value_multiplies'] = self.check_value_multiplies(url_html_info_table)
            else:
                self.record['url_html_info_table'] = ""
                self.record['value_multiplies'] = self.check_value_multiplies(self.full_txt_url)

            self.info_table_records = self.get_info_table()
            self.record['info_table_len'] = len(self.info_table_records)
            self.save_data()
            self.record['process'] = ''
        except Exception as e:
            err = traceback.format_exc()
            print("error process", err)
            
            self.record['process'] = str(err)

class RunACSN:
    def __init__(self,master_file:str, exisitng_file:str):
        self.df_master = pd.read_csv(master_file, dtype=str)
        self.df_master['cik'] = self.df_master['cik'].str.zfill(10)
        self.df_existing = pd.read_csv(exisitng_file, dtype=str)
        self.df_existing['cik'] = self.df_existing['cik'].str.zfill(10)
        self.runner_file = "sec_runner_acsn.csv"

    def get_torun_list(self):
        df_torun = self.df_master[~self.df_master['accessionNumber'].isin(self.df_existing['acsn'])]
        df_torun.drop_duplicates(subset=['accessionNumber'], inplace=True)
        print("Total to run :", len(df_torun))
        return df_torun.to_dict(orient="records")

    def process_record_acsn(self, record):
        cik = record.get('cik').zfill(10)
        acsn = record.get('accessionNumber')
        report_date = record.get('reportDate')
        
        file_name = f"sec_forms/{report_date.replace('-','_')}/{cik}_{acsn.replace('-','_')}_{report_date.replace('-','_')}.parquet"

        file_path = Path(file_name)
        if file_path.exists():
            print("File exist :", file_name)
            update_gdrive.upload_single_file(file_name)
        else:
            print("Processing :", file_name)
            process = ProcessACSN(record)
            process.run_acsn()
            update_gdrive.upload_single_file(file_name)
            return process.record

    def run(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        lock = threading.Lock()


        torun_list = self.get_torun_list()
        print("Length to run:", len(torun_list))

        # half = len(torun_list) // 2
        # list1 = torun_list[:half]
        # list2 = torun_list[half:]
        # with open("sec_master_filling_to_run.json", "r") as f:
        #     list1 = json.load(f)
        # with open("data_part1.json", "w", encoding="utf-8") as f2:
        #     json.dump(list2, f2, indent=4, ensure_ascii=False)

        def process_record(record):
            result = self.process_record_acsn(record)
            if result:
                with lock:
                    op_path = self.runner_file
                    df_record = pd.DataFrame([result])
                    if os.path.exists(op_path):
                        df_record.to_csv(op_path, mode="a", index=False, header=False)
                    else:
                        df_record.to_csv(op_path, index=False)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(process_record, record) for record in torun_list]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing record: {e}")

class GetFillingMaster:
    def __init__(self, cik_list : list):
        self.cik_list = cik_list
        self.master_file = "sec_master_filling_priority.csv"
        self.existing_ciks = self.get_existing_ciks()

    def get_existing_ciks(self):
        """Get list of CIKs that already exist in the master file"""
        existing_ciks = set()
        if os.path.exists(self.master_file):
            try:
                df_master = pd.read_csv(self.master_file, dtype=str)
                if 'cik' in df_master.columns:
                    existing_ciks = set(df_master['cik'].astype(str).str.zfill(10))
                    print(f"Found {len(existing_ciks)} existing CIKs in master file")
                else:
                    print("No 'cik' column found in master file")
            except Exception as e:
                print(f"Error reading master file: {e}")
        else:
            print("Master file does not exist, will create new one")
        return existing_ciks

    def filter_new_ciks(self):
        """Filter out CIKs that already exist in the master file"""
        # Convert input CIKs to 10-digit format for comparison
        input_ciks = [str(cik).zfill(10) for cik in self.cik_list]
        
        # Find CIKs that don't exist in master file
        new_ciks = [cik for cik in input_ciks if cik not in self.existing_ciks]
        
        print(f"Original CIK list: {len(self.cik_list)}")
        print(f"Existing CIKs in master file: {len(self.existing_ciks)}")
        print(f"New CIKs to process: {len(new_ciks)}")
        
        if len(new_ciks) < len(self.cik_list):
            excluded_ciks = [cik for cik in input_ciks if cik in self.existing_ciks]
            print(f"Excluded CIKs (already exist): {excluded_ciks[:10]}{'...' if len(excluded_ciks) > 10 else ''}")
        
        return new_ciks

    def cik_form(self,cik):
        """
        This function use to get company forms data by using CIK.
        """
        session = requests.session()
        response = session.get(f'https://data.sec.gov/submissions/CIK{cik}.json', headers=headers)
        print("Get json forms", response.status_code)
        if response.status_code == 200:
            # If return success the result will in json format
            form_result = response.json()

            # State business and mailing address in empty string, in case there is no data.
            
            try:
                self.business_address = ', '.join([i for i in list(form_result['addresses']['business'].values()) if i ])
            except:
                pass
            
            try:
                self.mailing_address = ', '.join([i for i in list(form_result['addresses']['mailing'].values()) if i ])
            except:
                pass

            # Load json data to dataframe for easier iteration
            df_forms = pd.DataFrame(form_result['filings']['recent'])
            df_forms['cik'] = cik
            df_forms['business_address'] = self.business_address
            df_forms['mailing_address'] = self.mailing_address
            df_forms = df_forms[df_forms['form'].str.contains('13F')]
            
            return df_forms

        else:
            print(response.status_code)


    def run(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        lock = threading.Lock()

        # Filter out CIKs that already exist in master file
        new_ciks = self.filter_new_ciks()
        
        if not new_ciks:
            print("No new CIKs to process. All CIKs already exist in master file.")
            return

        def process_cik(cik):
            df_forms = self.cik_form(cik)
            if df_forms is not None and not df_forms.empty:
                with lock:
                    op_path = self.master_file
                    if os.path.exists(op_path):
                        df_forms.to_csv(op_path, mode="a", index=False, header=False)
                    else:
                        df_forms.to_csv(op_path, index=False)

        print(f"Processing {len(new_ciks)} new CIKs...")
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_cik, cik) for cik in new_ciks]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing CIK: {e}")


