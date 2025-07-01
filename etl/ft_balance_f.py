import pandas as pd
import psycopg2
from io import StringIO

def parse_date_column(df, col_name):
    # Пробуем преобразовать дату, если формат неизвестен, можно добавить несколько попыток
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            df[col_name] = pd.to_datetime(df[col_name], format=fmt)
            return df
        except:
            continue
    # Если не удалось распарсить, пробуем без формата
    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
    return df

def load_ft_balance_f(csv_path, conn_params):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Лог - старт
    cur.execute("CALL logs.write_log('ds.load_ft_balance_f', 'start', NULL, NULL, 'Начало загрузки FT_BALANCE_F')")

    try:
        # Считаем csv в pandas, дата как строка
        df = pd.read_csv(csv_path, dtype=str, sep=';')

        # Обрабатываем дату
        df = parse_date_column(df, 'ON_DATE')

        # Очищаем таблицу
        cur.execute("DELETE FROM ds.ft_balance_f")

        # Загрузка через COPY из StringIO
        # Заменяем NaN на None
        df = df.where(pd.notna(df), None)
        buffer = StringIO()
        # Сохраняем датафрейм в csv в буфер, без индекса, с заголовком, разделитель ','
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cur.copy_expert("COPY ds.ft_balance_f (ON_DATE, account_rk, currency_rk, balance_out) FROM STDIN WITH CSV", buffer)

        # Пауза 5 секунд (если нужно)
        cur.execute("SELECT pg_sleep(5)")

        # Лог - финиш
        cur.execute("CALL logs.write_log('ds.load_ft_balance_f', 'finish', NULL, NULL, 'Конец загрузки FT_BALANCE_F')")

        conn.commit()

    except Exception as e:
        conn.rollback()
        # Лог - ошибка
        cur.execute("CALL logs.write_log('ds.load_ft_balance_f', 'error', %s, NULL, 'Ошибка при загрузке FT_BALANCE_F')", (str(e),))
        conn.commit()
        raise

    finally:
        cur.close()
        conn.close()

# Пример параметров подключения
conn_params = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'iladuro',
    'password': 'ilia2004'
}

# Запуск
load_ft_balance_f('/Users/iladuro/Desktop/файлы (1)/ft_balance_f.csv', conn_params)
