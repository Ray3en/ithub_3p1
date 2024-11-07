import psycopg2
import pandas as pd

# Создание подключения к PostgreSQL
conn = psycopg2.connect(database = "home",
                        host =     "212.8.247.94",
                        user =     "",
                        password = "",
                        port =     "5432")

# Отключение автокоммита
conn.autocommit = False

# Создание курсора
cursor = conn.cursor()

####################################################
def createTable():
    try: 
        cursor.execute( '''
            CREATE TABLE st.tgrn_products_scd2 ( 
                name text,
                price int,
                start_dt timestamp default current_timestamp,
                end_dt timestamp default '2999-12-31 23:59:59'
            )
        ''')
    except psycopg2.errors.DuplicateTable as e: 
        print('Таблица уже создана', e)
    conn.commit()


# Допишите функцию таким образом, чтобы сначала она закрывала текущую версию продукта, а также открывала новую версию 
def updateProduct(name, price):
    cursor.execute('''
        update st.tgrn_products_scd2
        set end_dt = current_timestamp
        where 
            1=1
            and name = %s
            and end_dt = '2999-12-31 23:59:59'
    ''', [name])
    cursor.execute('''
        insert into st.tgrn_products_scd2 (name, price, start_dt) values (%s,%s, current_timestamp + interval '1 seconds')
    ''', [name, price])
    conn.commit()


def selectTable(table):
    cursor.execute( f''' 
        select 
            name,
            price,
            start_dt::text, 
            end_dt::text 
        from st.tgrn_{table} ''')
    result = cursor.fetchall()
    for row in result:
        print(row)

def selectProducts(date):
    cursor.execute( f''' 
        select 
            name,
            price
        from st.tgrn_products_scd2 
        where %s between start_dt and end_dt
        ''', [date])
    result = cursor.fetchall()
    for row in result:
        print(row)

def actualProducts():
    cursor.execute( f''' 
        select 
            name,
            price
        from st.tgrn_products_scd2 
        where end_dt = '2999-12-31 23:59:59'
        ''')
    result = cursor.fetchall()
    for row in result:
        print(row)


# createTable()
# updateProduct('Чай', 500)
selectTable('products_scd2')
# selectProducts('2024-11-07 18:10:48')
# actualProducts()



cursor.close()
conn.close()