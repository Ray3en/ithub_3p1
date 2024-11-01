import psycopg2
import pandas as pd

conn = psycopg2.connect(database = "home",
                        host =     "212.8.247.94",
                        user =     "",
                        password = "",
                        port =     "5432")

conn.autocommit = False
cursor = conn.cursor()

# Создайте функцию, которая будет реулазиовывтаь удаление записи по ID

def createTable():
    try: 
        cursor.execute( '''
            CREATE TABLE st.tgrn_testtable ( 
                id int, 
                val varchar(10) 
            )
        ''')
    except psycopg2.errors.DuplicateTable as e: 
        print('Таблица уже создана', e)
    conn.commit()
    
def createRow(id, val):
    cursor.execute( ''' 
        insert into st.tgrn_testtable(id, val) values(%s, %s)
    ''', (id, val))
    conn.commit()

def selectTable(table):
    cursor.execute( f''' select * from st.tgrn_{table}''')
    result = cursor.fetchall()
    for row in result:
        print(row)
 
def selectOrCreate(id):
    cursor.execute( ' select * from st.tgrn_testtable where id = %s', [id])
    row = cursor.fetchone() 
    if not row:
        print('Записи не существует, создайте новую')
        value = input('Введите новое значение для записи')
        createRow(id, value)
    else: 
        print(row)

def deleteRow(id):
        cursor.execute( ' delete from st.tgrn_testtable where id = %s', [id])
        conn.commit()

# createTable()
# createRow(1, 'A')
# createRow(2, 'B')
# createRow(3, 'H')
# createRow(4, 'L')
# createRow(5, 'K')
# createRow(6, 'O')

# selectOrCreate(10)
# deleteRow(10)
# selectTable('testtable')


# Чтение и запуск скрипта script.sql
fd = open('script.sql')
script = fd.read().split(';\n')

for command in script:
    # if command != '':
    #     cursor.execute(command)
    print(command)

selectTable('testtable_2')
conn.commit()
cursor.close()
conn.close()
