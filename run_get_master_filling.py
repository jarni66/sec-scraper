import sec
import json
import update_gdrive

# with open("to_run.json", "r", encoding="utf-8") as f:
#     cik_list = json.load(f)

# cik_list = []
# with open("most_important_ciks.txt", "r", encoding="utf-8") as f:
#     for line in f:
#         cik_list.append(line.strip().zfill(10))  
# runner = sec.GetFillingMaster(cik_list)
# runner.run()


runner_acsn = sec.RunACSN("sec_master_filling_priority.csv", "merged_sec_forms_file_list.csv")
runner_acsn.run()


# update_gdrive.merge_sec_files_csv()