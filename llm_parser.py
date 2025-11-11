from custom_runners import CRunner
from agents import Agent
from pydantic import BaseModel, Field
from dotenv import load_dotenv
load_dotenv() # Load environment variables from .env file
from typing import List, Optional
import json
import re
import asyncio

extract_system = """
You are an expert SEC 13F table parser.
Your task is to extract structured data from raw text tables of 13F filings.
The input will be a block of text representing one or more rows of a 13F filing table.
You must output a JSON object for each row, strictly following the schema below:

```json
{
    "name_of_issuer": string,              // full company/issuer name
    "title_of_class": string,              // security class (e.g., COM, ADR, SH, CL A, etc.)
    "cusip": string,                       // 9-character CUSIP if available, else ""
    "figi": string,                        // always return ""
    "value": string,                       // numeric; may include commas (e.g., "1,234,643"), else "0"
    "shares_or_percent_amount": string,    // numeric; may include commas, else "0"
    "shares_or_percent_type": string,      // type (e.g., SH, PRN), else ""
    "put_call": string,                    // "Put" or "Call" if specified, else ""
    "investment_discretion": string,       // discretion field (e.g., SOLE, DEFINED, SHARED), else ""
    "other_manager": string,               // name or number of other manager, else ""
    "voting_authority_sole": string,       // numeric; may include commas, else "0"
    "voting_authority_shared": string,     // numeric; may include commas, else "0"
    "voting_authority_none": string        // numeric; may include commas, else "0"
}
```

### Rules:

* Always return valid JSON (an array if multiple rows).
* **Numeric fields** (`value`, `shares_or_percent_amount`, `voting_authority_sole`, `voting_authority_shared`, `voting_authority_none`):

  * Must contain only digits and optional commas (e.g., `"12,345"`, `"2,000,001"`).
  * Do **not** include words, symbols, or units.
  * If the field is missing, contains non-numeric text (e.g., `"SOLE"`, `"N/A"`), or is invalid → return `"0"`.
* **String fields**:

  * If missing, `"N/A"`, `"n/a"`, or invalid → return `""`.
* Do not infer or fabricate information beyond what is in the table.
* Preserve string values exactly as written in the table (except stripping extra whitespace).

"""


extract_agent = Agent(
    name="Extract Agent",
    instructions=extract_system,
    model="gpt-5-nano",
)

class Sec13FEntry(BaseModel):
    name_of_issuer: str = Field("", description="Full company/issuer name")
    title_of_class: str = Field("", description="Security class (e.g., COM, ADR, SH, CL A, etc.)")
    cusip: str = Field("", description="9-character CUSIP if available, otherwise empty string")
    figi: str = Field("", description="Financial Instrument Global Identifier, leave empty if unavailable")
    value: str = Field("0", description="Numeric value from 'Value (x$1000)' or 'Value' column, stored as string, value is numeric")
    shares_or_percent_amount: str = Field("0", description="Number of shares or principal amount, as string, value is numeric")
    shares_or_percent_type: str = Field("", description="Type (e.g., SH, PRN), if available")
    put_call: str = Field("", description="Put or Call designation, otherwise empty string")
    investment_discretion: str = Field("", description="Investment discretion (e.g., Sole, Defined, etc.)")
    other_manager: str = Field("", description="Other manager reference, if available, otherwise empty string")
    voting_authority_sole: str = Field("0", description="Sole voting authority amount, as string, value is numeric")
    voting_authority_shared: str = Field("0", description="Shared voting authority amount, as string, value is numeric")
    voting_authority_none: str = Field("0", description="None voting authority amount, as string, value is numeric")

class TableRes(BaseModel):
    result: List[Sec13FEntry] = Field("", description="List of records")

async def get_records(table_str):
    prompt = f"Parse this text table into json list.\nTable text : {table_str}"
    res_obj = CRunner(
        agent = extract_agent,
        prompt = prompt,
        format_output=TableRes
    )

    await res_obj.run_async()
    res = res_obj.output
    with open("test_llm2.json", "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2) 
    return res['result']


# import re
# with open("raw_txt/0000914976_0000950133_00_001369_1999_12_31.txt") as f:
#     text = f.read()

# # Extract first <TABLE> ... </TABLE>
# table_texts = re.findall(r"<TABLE>(.*?)</TABLE>", text, flags=re.S)
# table1 = table_texts[1]

# asyncio.run(get_records(table1))
