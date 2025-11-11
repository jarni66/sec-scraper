import pandas as pd
import sec


df_rerun = pd.read_csv('batch2.csv', dtype=str)
df_rerun = df_rerun[df_rerun['info_table_len'] == "0"]

results = []

for idx, row in df_rerun.iterrows():
    row_dict = row.to_dict()
    tf = row_dict['raw_txt']
    with open(tf, "r", encoding="utf-8") as f:
        raw_str = f.read()
    
    if "issuer" in raw_str.lower():

        row_dict['cik'] = row_dict['cik'].zfill(10)
        process = sec.ProcessACSN(row_dict)
        process.run()
        results.append(process.record)
        df_save = pd.DataFrame(results)
        df_save.to_csv("rerun/rerun1.csv", index=False)