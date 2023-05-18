from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from config import *


engine = create_engine(f'{DB_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

Session = sessionmaker(bind=engine)
session = Session()

inspector = inspect(engine)
all_tables = inspector.get_table_names()

for table_name in all_tables:
    columns = inspector.get_columns(table_name)
    for column in columns:
        if column['name'].startswith(COLUMN_NAME_DETAIL) or column['name'].endswith(COLUMN_NAME_DETAIL):
            query =  text(f"UPDATE {table_name} SET {column['name']} = REPLACE({column['name']}, '{OLD_VALUE}', '{NEW_VALUE}')")
            session.execute(query)
            session.commit()

session.close()
