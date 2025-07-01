import pandas as pd
import psycopg2
from io import StringIO

def parse_multiple_dates(df, date_columns):
    for col in date_columns:
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                df[col] = pd.to_datetime(df[col], format=fmt)
                break
            except:
                continue
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def load_md_account_d(csv_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute("CALL logs.write_log('ds.load_md_account_d', 'start', NULL, NULL, 'Начало загрузки MD_ACCOUNT_D')")

    try:
        df = pd.read_csv(csv_path, dtype=str, sep=';')
        df.columns = [col.lower() for col in df.columns]
        df = parse_multiple_dates(df, ['data_actual_date', 'data_actual_end_date'])

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO ds.md_account_d (
                    data_actual_date, data_actual_end_date, account_rk,
                    account_number, char_type, currency_rk, currency_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (data_actual_date, account_rk) DO UPDATE
                SET data_actual_end_date = EXCLUDED.data_actual_end_date,
                    account_number = EXCLUDED.account_number,
                    char_type = EXCLUDED.char_type,
                    currency_rk = EXCLUDED.currency_rk,
                    currency_code = EXCLUDED.currency_code
            """, tuple(row))

        cur.execute("CALL logs.write_log('ds.load_md_account_d', 'finish', NULL, NULL, 'Конец загрузки MD_ACCOUNT_D')")
        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.execute("CALL logs.write_log('ds.load_md_account_d', 'error', %s, NULL, 'Ошибка при загрузке MD_ACCOUNT_D')", (str(e),))
        conn.commit()
        raise

    finally:
        cur.close()
        conn.close()

conn_params = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'iladuro',
    'password': 'ilia2004'
}

load_md_account_d('/Users/iladuro/Desktop/etl_project/data/md_account_d.csv', conn_params)
