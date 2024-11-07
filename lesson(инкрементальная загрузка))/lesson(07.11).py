import psycopg2
import pandas as pd
from sqlalchemy import create_engine 

# Создание подключения к PostgreSQL
conn = psycopg2.connect(database = "home",
                        host =     "212.8.247.94",
                        user =     "",
                        password = "",
                        port =     "5432")

conn.autocommit = False

cursor = conn.cursor()

engine = create_engine(
        'postgresql+psycopg2://....'
        # 'postgresql+psycopg2://user:password@hostname/database_name'
)

def csv2sql(filePath, tableName):
    df = pd.read_csv(filePath)
    df.to_sql(tableName, engine, if_exists='replace', schema='st', index=False)

def restoreTables():
    cursor.execute('drop view if exists st.tgrn_v_auto ')
    cursor.execute('drop table if exists st.tgrn_new_auto ')
    # cursor.execute('drop table if exists st.tgrn_auto_hist ')

    conn.commit()

def init():
    cursor.execute('''
        create table if not exists st.tgrn_auto_hist (
            id serial primary key,
            model varchar(128),
            transmission varchar(128),
            body_type varchar(128),
            drive_type varchar(128),
            color varchar(128),
            production_year varchar(128),
            auto_key int,
            engine_capacity varchar(128),
            horsepower varchar(128),
            engine_type varchar(128),
            price int,
            milage int,
            start_dt timestamp default current_timestamp,
            end_dt timestamp default '2999-12-31 23:59:59'
        )
        ''')
    conn.commit()
    
    cursor.execute(''' 
    create view st.tgrn_v_auto as 
        select
            id,
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage
        from st.tgrn_auto_hist
        where current_timestamp between start_dt and end_dt
    ''')
    conn.commit()

def createTeableNewRows():
    cursor.execute('''
    create table if not exists st.tgrn_new_auto as 
        select
            t1.*
        from st.tgrn_t_auto t1
        left join st.tgrn_v_auto t2
        on t1.auto_key = t2.auto_key
        where t2.auto_key is null
    ''')
    conn.commit()

def showTable(tableName):
    print('_-'*10+tableName+'_-'*10)
    cursor.execute(f'select * from {tableName}')
    for row in cursor.fetchall():
        print(row)

restoreTables()
init()
csv2sql('data.csv', 'tgrn_t_auto')
createTeableNewRows()
# showTable('st.tgrn_t_auto')
showTable('st.tgrn_new_auto')
cursor.close()
conn.close()