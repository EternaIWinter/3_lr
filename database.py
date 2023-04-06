import psycopg2
from psycopg2 import Error
from config import currencies

def create_table():
    try:
        con = psycopg2.connect( dbname='3_lr',
                            user="postgres", 
                            password='174QcT',
                            port="5432", 
                            host='127.0.0.1')
        cursor = con.cursor()
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "exchange_rate" (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    currency CHAR(3) NOT NULL,
                    rate REAL NOT NULL,
                    UNIQUE (date, currency)
                )
            """)
        con.commit()
    except (Exception, Error) as err:
        print("Ошибка при работе с PostgreSQL", err)

def insert_rates(date, rates):
    try:
        con = psycopg2.connect( dbname='3_lr',
                            user="postgres", 
                            password='174QcT',
                            port="5432", 
                            host='127.0.0.1')
        cursor = con.cursor()
        for currency, rate in rates.items():
            cursor.execute(f"""
                    INSERT INTO "exchange_rate" (date, currency, rate)
                    VALUES ('{date}', '{currency}', {rate})
                    ON CONFLICT (date, currency) DO UPDATE
                    SET rate = EXCLUDED.rate
                """)
            con.commit()
    except (Exception, Error) as err:
        print("Ошибка при работе с PostgreSQL", err)

def get_rates(day_start, day_finish):
    try:
        con = psycopg2.connect( dbname='3_lr',
                            user="postgres", 
                            password='174QcT',
                            port="5432", 
                            host='127.0.0.1')
        cursor = con.cursor()
        str_curren = ', '.join(f"'{currency}'" for currency in currencies)
        query = f"""
            SELECT currency, MIN(rate), MAX(rate), AVG(rate)
            FROM "exchange_rate"
            WHERE date >= %s AND date <= %s AND currency IN ({str_curren})
            GROUP BY currency;
            """
        cursor.execute(query, (day_start, day_finish))
        return cursor.fetchall()
    except (Exception, Error) as err:
        print("Ошибка при работе с PostgreSQL", err)

def get_one_rate(day):
    try:
        con = psycopg2.connect( dbname='3_lr',
                            user="postgres", 
                            password='174QcT',
                            port="5432", 
                            host='127.0.0.1')
        cursor = con.cursor()
        str_curren = ', '.join(f"'{currency}'" for currency in currencies)
        query = f"""
            SELECT currency, rate
            FROM "exchange_rate"
            WHERE date = '{day}' AND currency IN ({str_curren});
            """
        cursor.execute(query)
        return cursor.fetchall()
    except (Exception, Error) as err:
        print("Ошибка при работе с PostgreSQL", err)
