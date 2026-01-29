import os
import re
import pandas as pd
import duckdb
import mysql.connector
from dotenv import load_dotenv

def clean_money(s: str | None):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    # remove currency symbols and spaces
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None

def clean_percent(s: str | None):
    if s is None:
        return None
    s = str(s).strip()  
    if s == "":
        return None
    s = s.replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None

def clean_int_commas(s: str | None):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    s = s.replace(",", "").strip()
    try:
        # some tables may contain decimals like '24,117,862.00'
        return int(float(s))
    except ValueError:
        return None

def fetch_table_to_df(cur, table_name: str) -> pd.DataFrame:
    cur.execute(f"SELECT * FROM `{table_name}`")
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)

def main():
    load_dotenv()

    mysql_cfg = dict(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "2545"),
        database=os.getenv("MYSQL_DB", "game_steam"),
    )

    print("Connecting to MySQL:", {k: v for k, v in mysql_cfg.items() if k != "password"})
    conn = mysql.connector.connect(**mysql_cfg)
    cur = conn.cursor()

    # 1) list tables
    cur.execute("SHOW TABLES")
    tables = [t[0] for t in cur.fetchall()]
    print(f"Found {len(tables)} tables in `{mysql_cfg['database']}`")
    print("Example tables:", tables[:10])

    duck_path = "game_steam.duckdb"
    dcon = duckdb.connect(duck_path)

    for t in tables: # This loop was not indented correctly
        df = fetch_table_to_df(cur, t)
    
        # แสดงตัวอย่าง 3 แถวแรกของแต่ละตาราง
        print(f"\n=== {t} | rows={len(df)} cols={len(df.columns)} ===")
        print(df.head(3).to_string(index=False))
    
        # เขียนลง DuckDB เป็นคนละตาราง
        dcon.register("tmp_df", df)
        dcon.execute(f'CREATE OR REPLACE TABLE "{t}" AS SELECT * FROM tmp_df')

    dcon.close()
    print(f"✅ Done. DuckDB file created: {duck_path}")

    # 3) write into DuckDB (local file)
    duck_path = "game_steam.duckdb"
    dcon = duckdb.connect(duck_path)

    dcon.close()
    cur.close()
    conn.close()

    print(f"✅ Done. DuckDB file created: {duck_path}")

if __name__ == "__main__":
    main()
