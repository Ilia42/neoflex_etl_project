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

def load_md_ledger_account_s(csv_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute("CALL logs.write_log('ds.load_md_ledger_account_s', 'start', NULL, NULL, 'Начало загрузки MD_LEDGER_ACCOUNT_S')")

    try:
        df = pd.read_csv(csv_path, dtype=str, sep=';')

        df = parse_multiple_dates(df, ['START_DATE', 'END_DATE'])

        # Преобразуем NaN в None для PostgreSQL
        df = df.where(pd.notna(df), None)

        # Список всех колонок, которые ожидаются в базе
        db_columns = [
            'chapter', 'chapter_name', 'section_number', 'section_name',
            'subsection_name', 'ledger1_account', 'ledger1_account_name',
            'ledger_account', 'ledger_account_name', 'characteristic',
            'is_resident', 'is_reserve', 'is_reserved', 'is_loan',
            'is_reserved_assets', 'is_overdue', 'is_interest',
            'pair_account', 'start_date', 'end_date',
            'is_rub_only', 'min_term', 'min_term_measure',
            'max_term', 'max_term_measure', 'ledger_acc_full_name_translit',
            'is_revaluation', 'is_correct'
        ]
        # Приводим имена колонок в датафрейме к нижнему регистру
        df.columns = [col.lower() for col in df.columns]
        for _, row in df.iterrows():
            # Для отсутствующих колонок подставляем None
            values = [None if col not in df.columns else (None if pd.isna(row[col]) else row[col]) for col in db_columns]
            cur.execute(f"""
                INSERT INTO ds.md_ledger_account_s (
                    {', '.join(db_columns)}
                ) VALUES ({', '.join(['%s'] * len(db_columns))})
                ON CONFLICT (ledger_account, start_date) DO UPDATE
                SET
                    {', '.join([f'{col} = EXCLUDED.{col}' for col in db_columns if col not in ['ledger_account', 'start_date']])}
            """, values)

        cur.execute("CALL logs.write_log('ds.load_md_ledger_account_s', 'finish', NULL, NULL, 'Конец загрузки MD_LEDGER_ACCOUNT_S')")
        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.execute("CALL logs.write_log('ds.load_md_ledger_account_s', 'error', %s, NULL, 'Ошибка при загрузке MD_LEDGER_ACCOUNT_S')", (str(e),))
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

load_md_ledger_account_s('/Users/iladuro/Desktop/файлы (1)/md_ledger_account_s.csv', conn_params)
