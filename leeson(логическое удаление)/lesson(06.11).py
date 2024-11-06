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
            CREATE TABLE st.tgrn_clients ( 
                id serial primary key, 
                name text,
                category text,
                deleted_flg char(1) default 'N' check(deleted_flg in ('Y', 'N'))
            )
        ''')
    except psycopg2.errors.DuplicateTable as e: 
        print('Таблица уже создана', e)
    conn.commit()



def selectTable(table):
    cursor.execute( f''' select * from st.tgrn_{table} where deleted_flg = 'N' ''')
    result = cursor.fetchall()
    for row in result:
        print(row)

def deleteClient(id):
        cursor.execute(''' 
                    update st.tgrn_clients
                    set deleted_flg = 'Y'
                    where id = %s
                ''', [id])
        conn.commit()

def restoreClient(id):
        cursor.execute(''' 
                    update st.tgrn_clients
                    set deleted_flg = 'I'
                    where id = %s
                ''', [id])
        conn.commit()

def insertClient(name, category):
        cursor.execute(''' 
                    insert into st.tgrn_clients
                    (name, category) values (%s, %s)
                ''', [name, category])
        conn.commit()

# createTable()
# insertClient('Petr', '1')
# insertClient('Dima', '2')
# insertClient('Ekaterina', '3')

# deleteClient(1)
restoreClient(1)
selectTable('clients')
# Cоздайте функцию которая позволит записать нового клиента в таблицу (указав имя и категорию)
# Создайте функцию которая реализует ЛОГИЧЕКИЕ удаление записи (указав ID записи)
# Создайте функцю (restore) которая по ID восстановит пользователя (снова сделаем его активным)
# Доработайте функцию selectTable таким образом, чтобы вы получали список АКТИВНЫХ пользователей