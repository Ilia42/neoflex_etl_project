import pandas as pd
import psycopg2
from io import StringIO
import time

def parse_multiple_dates(df, date_columns):
    for col in date_columns:
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
            try:
                df[col] = pd.to_datetime(df[col], format=fmt)
                break
            except:
                continue
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def load_ft_posting_f(csv_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute("CALL logs.write_log('ds.load_ft_posting_f', 'start', NULL, NULL, 'Начало загрузки FT_POSTING_F')")
    conn.commit()
    time.sleep(5)

    try:
        df = pd.read_csv(csv_path, dtype=str, sep=';')

        df = parse_multiple_dates(df, ['OPER_DATE'])

        cur.execute("TRUNCATE ds.ft_posting_f")

        df = df.where(pd.notna(df), None)
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cur.copy_expert("""
            COPY ds.ft_posting_f (
                oper_date,
                credit_account_rk,
                debet_account_rk,
                credit_amount,
                debet_amount
            ) FROM STDIN WITH CSV
        """, buffer)

        cur.execute("CALL logs.write_log('ds.load_ft_posting_f', 'finish', NULL, NULL, 'Конец загрузки FT_POSTING_F')")
        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.execute("CALL logs.write_log('ds.load_ft_posting_f', 'error', %s, NULL, 'Ошибка при загрузке FT_POSTING_F')", (str(e),))
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

load_ft_posting_f('/Users/iladuro/Desktop/etl_project/data/ft_posting_f.csv', conn_params)
