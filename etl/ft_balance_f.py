import pandas as pd
import psycopg2
from io import StringIO

def parse_date_column(df, col_name):
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            df[col_name] = pd.to_datetime(df[col_name], format=fmt)
            return df
        except:
            continue
    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
    return df

def load_ft_balance_f(csv_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute("CALL logs.write_log('ds.load_ft_balance_f', 'start', NULL, NULL, 'Начало загрузки FT_BALANCE_F')")

    try:
        df = pd.read_csv(csv_path, dtype=str, sep=';')
        df = parse_date_column(df, 'ON_DATE')

        df['ON_DATE'] = df['ON_DATE'].dt.strftime('%Y-%m-%d')

        df['ACCOUNT_RK'] = df['ACCOUNT_RK'].astype(int)
        df['CURRENCY_RK'] = df['CURRENCY_RK'].astype(int)
        df['BALANCE_OUT'] = df['BALANCE_OUT'].astype(float)

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO ds.ft_balance_f (
                    on_date, account_rk, currency_rk, balance_out
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (on_date, account_rk) DO UPDATE
                SET currency_rk = EXCLUDED.currency_rk,
                    balance_out = EXCLUDED.balance_out
            """, tuple(row))

        cur.execute("CALL logs.write_log('ds.load_ft_balance_f', 'finish', NULL, NULL, 'Конец загрузки FT_BALANCE_F')")
        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.execute("CALL logs.write_log('ds.load_ft_balance_f', 'error', %s, NULL, 'Ошибка при загрузке FT_BALANCE_F')", (str(e),))
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


load_ft_balance_f('/Users/iladuro/Desktop/etl_project/data/ft_balance_f.csv', conn_params)
