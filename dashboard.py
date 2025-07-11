import streamlit as st
import time
import plotly.express as px

import pyodbc, struct
import pandas as pd
import logging
from azure import identity
AZURE_SQL_CONNECTIONSTRING='Driver={ODBC Driver 18 for SQL Server};Server=tcp:mssqldatabaseserver.database.windows.net,1433;Database=mssqldatatest;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'


st.set_page_config(layout="wide")

st.title("Sales Dashboard")

if st.button('Refresh Data'):
    with st.spinner('Refreshing data...'):
        time.sleep(1) 

col1, col2, col3 = st.columns([2, 2, 2])



def get_connection():

    try:
        credential = identity.DefaultAzureCredential()
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
        conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        logging.info("Connection established successfully.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Azure SQL: {e}")



def SalesVolume():
    mycursor = None
    try:
        conn = get_connection()
        mycursor = conn.cursor()

        sql_query = """
                    SELECT Product_line AS Products, SUM(Total) AS Total_Sales
                    FROM mssqldatatest.dbo.sales
                    GROUP BY Product_line
                    ORDER BY Total_Sales DESC;
                    """
        mycursor.execute(sql_query)
        columns = [column[0] for column in mycursor.description]
        result = mycursor.fetchall()
      
        # Convert to DataFrame
        df = pd.DataFrame.from_records(result, columns=columns)
        return df
    
    except pyodbc.Error as e:
        st.error(f"Database error: {e}")
        return None
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None
    finally:
        mycursor.close()

def PaymentwiseSales():
    try:
        conn = get_connection()
        mycursor = conn.cursor()

        sql_query = """
                    SELECT Payment, SUM(Total) AS Total_Sales
                    FROM mssqldatatest.dbo.sales
                    GROUP BY Payment
                    ORDER BY Total_Sales DESC;
                    """
        mycursor.execute(sql_query)
        columns = [column[0] for column in mycursor.description]
        result = mycursor.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame.from_records(result, columns=columns)
        return df
    except pyodbc.Error as e:
        st.error(f"Database error: {e}")
        return None
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None
    finally:
        mycursor.close()

def CityWiseSales():
    try:
        conn = get_connection()
        mycursor = conn.cursor()

        sql_query = """
                    SELECT City, SUM(Total) AS Total_Sales
                    FROM mssqldatatest.dbo.sales
                    GROUP BY City
                    ORDER BY Total_Sales DESC;
                    """
        mycursor.execute(sql_query)
        columns = [column[0] for column in mycursor.description]
        result = mycursor.fetchall()
        
        # Convert to DataFrame
        df = pd.DataFrame.from_records(result, columns=columns) 
        return df
    except pyodbc.Error as e:
        st.error(f"Database error: {e}")
        return None 
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None
    finally:
        mycursor.close()

def ProductWiseSales():
    try:
        conn = get_connection()
        mycursor = conn.cursor()

        sql_query = """
                    SELECT City, Product_line AS Product, SUM(Total) AS Total_Sales
                    FROM mssqldatatest.dbo.sales
                    GROUP BY City, Product_line
                    ORDER BY Total_Sales DESC;
                    """
        mycursor.execute(sql_query)
        columns = [column[0] for column in mycursor.description]
        result = mycursor.fetchall()
        # Convert to DataFrame
        df = pd.DataFrame.from_records(result, columns=columns) 
        return df
    except pyodbc.Error as e:
        st.error(f"Database error: {e}")
        return None
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None
    finally:
        if mycursor:
            mycursor.close()

with col1:
    data = SalesVolume()
    fig = px.pie(data, names='Products', values='Total_Sales', title='Sales Volume by Product Line')
    st.plotly_chart(fig, use_container_width=True)

with col2:
    data = CityWiseSales()
    fig = px.bar(data, x='City', y='Total_Sales', title='City-wise Sales')
    st.plotly_chart(fig, use_container_width=True)


with col3:
    data = PaymentwiseSales()
    fig = px.bar_polar(data, r='Total_Sales', theta='Payment', title='Payment-wise Sales')
    st.plotly_chart(fig, use_container_width=True)


