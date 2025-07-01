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

def load_md_exchange_rate_d(csv_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Лог: начало
    cur.execute("CALL logs.write_log('ds.load_md_exchange_rate_d', 'start', NULL, NULL, 'Начало загрузки MD_EXCHANGE_RATE_D')")

    try:
        # Чтение CSV
        df = pd.read_csv(csv_path, dtype=str, sep=';')

        # Парсинг дат
        df = parse_multiple_dates(df, ['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE'])

        # Обход строк
        for _, row in df.iterrows():
            # Заменяем NaN на None
            values = [None if pd.isna(x) else x for x in row]
            cur.execute("""
                INSERT INTO ds.md_exchange_rate_d (
                    data_actual_date,
                    data_actual_end_date,
                    currency_rk,
                    reduced_cource,
                    code_iso_num
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (data_actual_date, currency_rk) DO UPDATE
                SET
                    data_actual_end_date = EXCLUDED.data_actual_end_date,
                    reduced_cource = EXCLUDED.reduced_cource,
                    code_iso_num = EXCLUDED.code_iso_num
            """, values)

        # Лог: конец
        cur.execute("CALL logs.write_log('ds.load_md_exchange_rate_d', 'finish', NULL, NULL, 'Конец загрузки MD_EXCHANGE_RATE_D')")
        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.execute("CALL logs.write_log('ds.load_md_exchange_rate_d', 'error', %s, NULL, 'Ошибка при загрузке MD_EXCHANGE_RATE_D')", (str(e),))
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

load_md_exchange_rate_d('/Users/iladuro/Desktop/файлы (1)/md_exchange_rate_d.csv', conn_params)


