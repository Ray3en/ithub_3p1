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

# Выполнение SQL кода в базе данных без возврата результата
cursor.execute( "CREATE TABLE st.xxxx_testtable( id int, val varchar(10) )" )
cursor.execute( "INSERT INTO st.xxxx_testtable( id, val ) VALUES ( 1, 'ABC' )" )
conn.commit()

# Выполнение SQL кода в базе данных с возвратом результата
cursor.execute( "SELECT * FROM st.xxxx_testtable" )
records = cursor.fetchall()

for row in records:
	print( row )

####################################################

# Формирование DataFrame
names = [ x[0] for x in cursor.description ]
df = pd.DataFrame( records, columns = names )

# Запись в файл
df.to_excel( 'pandas_pg_out.xlsx', sheet_name='sheet1', header=True, index=False )

####################################################

# Чтение из файла
df = pd.read_excel( 'pandas.xlsx', sheet_name='sheet1', header=0, index_col=None )

# Запись DataFrame в таблицу базы данных
cursor.executemany( "INSERT INTO st.xxxx_testtable( id, val ) VALUES( %s, %s )", df.values.tolist() )
conn.commit()

# Закрываем соединение
cursor.close()
conn.close()