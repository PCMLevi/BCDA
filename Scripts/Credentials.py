from pathlib import Path
from sqlalchemy import create_engine, text
import os
import pyodbc


CLIENT_ID = "2f404a43-f953-4eb1-a0de-d55b075bb856"
CLIENT_SECRET = "157786d3e10ee9fb71e0d0c4b5bfc0a2fa546ee6983e03baeb0939a439d3af4ddbeddfe35a4945ad"


engine_DEV_Test_pyodbc = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=Dev-Test;"
    "DATABASE=BCDA_Staging;"
    "UID=sa;"
    "PWD=Jesus&411;"
)

engine_DEV_Final_pyodbc = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=Dev-Final;"
    "DATABASE=BCDA_Staging;"
    "UID=sa;"
    "PWD=Jesus&411;"
)

engine_PROD_Final_pyodbc = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=Prod-Final;"
    "DATABASE=BCDA_Staging;"
    "UID=sa;"
    "PWD=Jesus&411;"
)

engine_PROD_Test_pyodbc =(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=Prod-Test;"
    "DATABASE=BCDA_Staging;"
    "UID=sa;"
    "PWD=Jesus&411;"
)

engine_DEV_Test = create_engine(
    "mssql+pyodbc://sa:Jesus&411@Dev-Test/BCDA_Staging?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True
)

engine_DEV_Final = create_engine(
    "mssql+pyodbc://sa:Jesus&411@Dev-Final/BCDA_Staging?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True
)


engine_PROD_Test = create_engine(
    "mssql+pyodbc://sa:Jesus&411@Prod-Test/BCDA_Staging?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True
)

engine_PROD_Final = create_engine(
    "mssql+pyodbc://sa:Jesus&411@Prod-Final/BCDA_Staging?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True
)

