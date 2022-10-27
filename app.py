import time  # to simulate a real time data, time loop

import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development
import pymysql
from sqlalchemy import create_engine


st.set_page_config(
    page_title="ETH Stats Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("ETH Metrics - Unified Dashboard")

DB_CONN_URL = st.secrets["DB_CONN_URL"]

# read csv from a URL
@st.experimental_memo(ttl=86400)
def get_data() -> pd.DataFrame:
    
    sql_get_el_cl_mapping = """
        SELECT 
            user_agent_name AS consensus_client,
            client_name     AS execution_client,
            count(*)        AS total_nodes
        FROM 
            nodewatch nw
            INNER JOIN ethernodes en
                ON nw.ip = en.ip
        WHERE  
            en.client_name != ''
        GROUP BY 
            consensus_client,
            execution_client
        ORDER BY 
            total_nodes DESC 
    """
    engine = create_engine(DB_CONN_URL, pool_recycle=3600)
    df_el_cl_mapping = pd.read_sql(sql_get_el_cl_mapping, engine)
    return df_el_cl_mapping

rad = st.sidebar.radio("Navigation",["Network","Depositors","Merge Impact"])
if rad == "Network":

    tab1, tab2, tab3 = st.tabs(["Client Pairings", "Hosting Diversity", "Trending Clients"])

    with tab1:
    

        df = get_data()
        # creating a single-element container
        placeholder = st.empty()

        el_clients = df.execution_client.unique()
        cl_clients = df.consensus_client.unique()
        total_el_clients = len(el_clients)
        total_cl_clients = len(cl_clients)

        for i in range(max(total_el_clients, total_cl_clients)):

            with placeholder.container():

                fig_col1, fig_col2 = st.columns(2)
                with fig_col1:
                    fig_1 = px.pie(df[df.execution_client.isin(el_clients[:i+1])], values='total_nodes', names='execution_client', title='Execution Client Stats')
                    st.plotly_chart(fig_1, use_container_width=True)

                with fig_col2:
                    fig_2 = px.pie(df[df.consensus_client.isin(cl_clients[:i+1])], values='total_nodes', names='consensus_client', title='Consensus Client Stats')
                    st.plotly_chart(fig_2, use_container_width=True)


                fig_3 = px.treemap(df, path=['execution_client', 'consensus_client'], values='total_nodes', title='Client Pairings')
                st.plotly_chart(fig_3, use_container_width=True)

                st.markdown("### Detailed Data View")
                st.dataframe(df)
                time.sleep(0.5)

    with tab2:
       st.header("coming soon")
       st.image("https://static.streamlit.io/examples/dog.jpg", width=200)

    with tab3:
       st.header("coming soon")




        
        

