import pandas as pd
import config
import numpy as np
import sec





df = pd.read_csv("sec_filling_fix_error.csv",dtype=str).fillna('').drop_duplicates(subset='accessionNumber')

cols = df.columns[:18] 

for idx, row in df.iterrows():
    if row['upload_to_bucket_status'].startswith('Error'):
        print("Process ACSN", row['accessionNumber'])
        row_dict = row[cols].to_dict()
        run_acsn = sec.ProcessACSN(row_dict)
        run_acsn.run()
        
        for k, v in run_acsn.record.items():
            df.at[idx, k] = v


df.to_csv("sec_filling_fix_error2.csv",index=False)
