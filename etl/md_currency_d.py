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

def load_md_currency_d(csv_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Лог: начало
    cur.execute("CALL logs.write_log('ds.load_md_currency_d', 'start', NULL, NULL, 'Начало загрузки MD_CURRENCY_D')")

    try:
        # Чтение CSV
        df = pd.read_csv(csv_path, dtype=str, sep=';', encoding='windows-1252')
        df.columns = [col.lower() for col in df.columns]

        # Парсинг дат
        df = parse_multiple_dates(df, ['data_actual_date', 'data_actual_end_date'])

        # Обработка UPSERT для каждой строки
        for _, row in df.iterrows():
            # Заменяем NaN на None
            values = [None if pd.isna(x) else x for x in row]
            cur.execute("""
                INSERT INTO ds.md_currency_d (
                    currency_rk,
                    data_actual_date,
                    data_actual_end_date,
                    currency_code,
                    code_iso_char
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (currency_rk, data_actual_date) DO UPDATE
                SET
                    data_actual_end_date = EXCLUDED.data_actual_end_date,
                    currency_code = EXCLUDED.currency_code,
                    code_iso_char = EXCLUDED.code_iso_char
            """, values)

        # Лог: конец
        cur.execute("CALL logs.write_log('ds.load_md_currency_d', 'finish', NULL, NULL, 'Конец загрузки MD_CURRENCY_D')")
        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.execute("CALL logs.write_log('ds.load_md_currency_d', 'error', %s, NULL, 'Ошибка при загрузке MD_CURRENCY_D')", (str(e),))
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

load_md_currency_d('/Users/iladuro/Desktop/файлы (1)/md_currency_d.csv', conn_params)
