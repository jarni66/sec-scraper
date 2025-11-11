import os
import json
import sec
import pandas as pd


with open("cik_list2.json", "r", encoding="utf-8") as f:
    cik_list = json.load(f)

df = pd.read_csv("sec_filling_batch1.csv",dtype=str).fillna('').drop_duplicates(subset='accessionNumber')
exist_cik_list1 = list(df['cik'].unique())

df2 = pd.read_csv("cik_forms/cik_forms.csv",dtype=str).fillna('').drop_duplicates(subset='accessionNumber')
exist_cik_list2 = list(df2['cik'].unique())

df3 = pd.read_csv("cik_forms/cik_forms2.csv",dtype=str).fillna('').drop_duplicates(subset='accessionNumber')
exist_cik_list3 = list(df3['cik'].unique())


exist_cik_list = exist_cik_list1 + exist_cik_list2 + exist_cik_list3

print("Existing cik :", len(exist_cik_list))


to_run = [i for i in cik_list if i not in exist_cik_list]


print("To run cik :", len(to_run))
start = 1000
end = 2000
for cik in to_run[start:end]:
    print("Process CIK :", cik)
    process_cik = sec.ProcessCIK(cik,f"cik_run_{start}_{end}")
    process_cik.run()
