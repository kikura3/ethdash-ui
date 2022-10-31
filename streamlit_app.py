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

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            footer:after {
                content:'Developed with ❤ by kikura.eth @ twigblock.com'; 
                visibility: visible;
                display: block;
                position: relative;
                padding: 5px;
                top: 2px;
            }
            </style>
            """

st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

DB_CONN_URL = st.secrets["DB_CONN_URL"]

@st.experimental_memo(ttl=86400)
def get_cl_hosting_diversity_data() -> pd.DataFrame:

    sql = """
    SELECT
    consensus_client,
    CASE
        WHEN total_nodes < 10 THEN 'Others'
        ELSE hosting_provider_name
    END AS hosting_provider_name,
    hosting_provider_name AS asn,
    SUM(total_nodes) AS total_nodes,
    MIN(last_updated) As last_updated
    FROM
    ui_consensus_hosting_info
    WHERE
    hosting_provider_name IS NOT NULL
    GROUP BY
    consensus_client,
    hosting_provider_name
    """
    return run_query(sql)


def run_query(sql):

    engine = create_engine(DB_CONN_URL, pool_recycle=3600)
    df = pd.read_sql(sql, engine)
    return df

@st.experimental_memo(ttl=86400)
def get_client_info() -> pd.DataFrame:
    
    sql = """
    SELECT * 
    FROM ui_client_info
    """
    return run_query(sql)

@st.experimental_memo(ttl=86400)
def get_first_proposal_clients() -> pd.DataFrame:
    
    sql_get_first_proposals = """
    SELECT * FROM ui_validator_first_proposals
    """
    return run_query(sql_get_first_proposals)

@st.experimental_memo(ttl=86400)
def get_proposals_by_client() -> pd.DataFrame:
    
    sql_get_proposals_by_client = """
    SELECT * FROM ui_proposals_by_client
    """
    return run_query(sql_get_proposals_by_client)

@st.experimental_memo(ttl=86400)
def get_client_performance() -> pd.DataFrame:
    
    sql_query = """
    SELECT * FROM ui_client_performance
    """
    engine = create_engine(DB_CONN_URL, pool_recycle=3600)
    df_data = pd.read_sql(sql_query, engine)
    return df_data

@st.experimental_memo(ttl=86400)
def get_depositor_performance() -> pd.DataFrame:
    
    sql_query = """
    SELECT * FROM ui_depositor_performance
    """
    return run_query(sql_query)

@st.experimental_memo(ttl=86400)
def get_depositor_performance_apr() -> pd.DataFrame:
    
    df_data = get_depositor_performance()
    df_grouped_data = df_data.groupby(['depositor_label']).agg(
        num_validators = pd.NamedAgg(column="apr", aggfunc="count"),
        avg_apr = pd.NamedAgg(column="apr", aggfunc="mean")
    ).reset_index().sort_values("avg_apr", ascending=False)

    return df_grouped_data

@st.experimental_memo(ttl=86400)
def get_depositor_performance_apr_quartiles() -> pd.DataFrame:
    
        df_data = get_depositor_performance()

        def q25(x):
            return x.quantile(0.25)

        def q75(x):
            return x.quantile(0.75)

        df_grouped_data = df_data.groupby("depositor_label").agg(
            min_apr = pd.NamedAgg(column="apr", aggfunc="min"),
            max_apr = pd.NamedAgg(column="apr", aggfunc="max"),
            median_apr = pd.NamedAgg(column="apr", aggfunc="median"),
            apr_25pct = pd.NamedAgg(column="apr", aggfunc=q25),
            apr_75pct = pd.NamedAgg(column="apr", aggfunc=q75)
        ).reset_index()

        df_grouped_data = df_grouped_data.set_index('depositor_label').stack().reset_index()
        df_grouped_data.columns = ['Depositor','Name', 'APR']

        return df_grouped_data

@st.experimental_memo(ttl=86400)
def get_client_performance_apr() -> pd.DataFrame:
    
    df_data = get_client_performance()
    df_grouped_data = df_data.groupby(['client']).agg(
        num_validators = pd.NamedAgg(column="validator", aggfunc="count"),
        median_apr = pd.NamedAgg(column="apr", aggfunc="median")
    ).reset_index().sort_values("median_apr", ascending=False)

    return df_grouped_data

@st.experimental_memo(ttl=86400)
def get_client_performance_apr_quartiles() -> pd.DataFrame:
    
        df_client_performance = get_client_performance()

        def q25(x):
            return x.quantile(0.25)

        def q75(x):
            return x.quantile(0.75)

        df_grouped_data = df_client_performance.groupby("client").agg(
            min_apr = pd.NamedAgg(column="apr", aggfunc="min"),
            max_apr = pd.NamedAgg(column="apr", aggfunc="max"),
            median_apr = pd.NamedAgg(column="apr", aggfunc="median"),
            apr_25pct = pd.NamedAgg(column="apr", aggfunc=q25),
            apr_75pct = pd.NamedAgg(column="apr", aggfunc=q75)
        ).reset_index()

        df_grouped_data = df_grouped_data.set_index('client').stack().reset_index()
        df_grouped_data.columns = ['Client','Name', 'APR']

        return df_grouped_data

@st.experimental_memo(ttl=86400)
def get_staking_client_distribution() -> pd.DataFrame:
    
    sql_ui_staking_client_distribution = """
    SELECT * FROM ui_staking_client_distribution
    """

    engine = create_engine(DB_CONN_URL, pool_recycle=3600)
    df_data = pd.read_sql(sql_ui_staking_client_distribution, engine)
    return df_data

@st.experimental_memo(ttl=86400)
def get_depositor_staking() -> pd.DataFrame:
    
    sql_ui_depositor_staking = """
    SELECT * FROM ui_depositor_staking
    """

    engine = create_engine(DB_CONN_URL, pool_recycle=3600)
    df_data = pd.read_sql(sql_ui_depositor_staking, engine)
    return df_data

@st.experimental_memo(ttl=86400)
def get_staking_overview() -> pd.DataFrame:
    
    sql_query = """
    SELECT * FROM ui_staking_overview
    """

    return run_query(sql_query)

@st.experimental_memo(ttl=86400)
def get_weekly_validator_signups() -> pd.DataFrame:
    
    sql_query = """
    SELECT * FROM dn_validators_signup_weekly
    """

    return run_query(sql_query)

@st.experimental_memo(ttl=86400)
def get_weekly_depositor_signups() -> pd.DataFrame:
    
    sql_query = """
    SELECT * FROM dn_depositors_signup_weekly
    """

    return run_query(sql_query)

@st.experimental_memo(ttl=86400)
def get_empty_block_stats() -> pd.DataFrame:
    
    sql_query = """
    SELECT * FROM dn_block_stats_empty_missed
    """

    return run_query(sql_query)

def set_fig_caption(text):
    st.markdown("<p style='text-align: center; font-size:12px;'>{}</p>".format(text), unsafe_allow_html=True)

client_color = {
            'Lighthouse': '#00dbeb',
            'Lodestar': '#ffa71a',
            'Nimbus': '#f5009b',
            'Prysm': '#a323d1',
            'Teku': '#4e00de'
        }

mt1, mt2, mt3 = st.tabs(["Client", "Staking", "On Chain"])
with mt1:
#rad = st.sidebar.radio("Navigation",["Clients","Staking","Merge Impact"])
#if rad == "Clients":

    client_tab1, tab2, tab3, tab4 = st.tabs(["Client Pairings", "Hosting Diversity", "Trending Clients", "Client Effectiveness"])

    with client_tab1:
    
        df = get_client_info()

        st.metric("Total Consensus Nodes Found", df.total_nodes.sum(), help="crawler running since 26-Oct-22")

        consensus_clients_fig = px.pie(df, values='total_nodes', names='consensus_client', title='Consensus Client Distribution')
        st.plotly_chart(consensus_clients_fig, use_container_width=True)

        last_updated = df.last_updated.min()
        fig_caption = "Data Source : <a href=''>CL crawler</a> ; Last Updated : {} ; Update Frequency: Daily</h1>".format(last_updated.date())
        set_fig_caption(fig_caption)

        client_pairing_figs = px.treemap(df[(~df.execution_client.isna()) & (df.execution_client!='')], path=['execution_client', 'consensus_client'], values='total_nodes', title='EL-CL Client Pairings')
        st.plotly_chart(client_pairing_figs, use_container_width=True)

        st.markdown("<p style='text-align: center; font-size:12px;'> IP matches for {} nodes</p>".format(df[(~df.execution_client.isna()) & (df.execution_client!='')].total_nodes.sum()), unsafe_allow_html=True)

        st.markdown("<p style='text-align: center; font-size:12px;'> \
            Data Source : <a href=''>EL & CL crawler</a> ; Last Updated : {} ; Update Frequency: Daily</p>".format(last_updated.date()), unsafe_allow_html=True)


    with tab2:

        df = get_cl_hosting_diversity_data()

        st.metric("Unique Hosting Providers Found", len(df.asn.unique()), help="excluding ISPs")

        cl_hosting_fig = px.pie(df, values='total_nodes', names='hosting_provider_name', title='Consensus Hosting Diversity')
        cl_hosting_fig.update_traces(textposition='inside')
        cl_hosting_fig.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')

        st.plotly_chart(cl_hosting_fig, use_container_width=True)

        fig_caption = "Data Source : <a href=''>CL crawler</a> ; Last Updated : {} ; Update Frequency: Daily</h1>".format(last_updated.date())
        set_fig_caption(fig_caption)


        cl_host_pairings = px.treemap(df, path=['hosting_provider_name', 'consensus_client'], values='total_nodes', title='Consensus Client - Hosting Provider : Affinity')
        st.plotly_chart(cl_host_pairings, use_container_width=True)

        fig_caption = "Data Source : <a href=''>CL crawler</a> ; Last Updated : {} ; Update Frequency: Daily</h1>".format(last_updated.date())
        set_fig_caption(fig_caption)

    with tab3:

        

        df = get_first_proposal_clients()
        trending_fig = px.line(df, x='first_proposal_month', y='total_validators',color='predicted_client', markers=True, color_discrete_map=client_color, title="Client Breakdown (by validator's first proposal)")
        trending_fig.update_layout(xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
        trending_fig.update_layout(xaxis_title="Month", yaxis_title="New Validators", legend_title="Client")
        st.plotly_chart(trending_fig, use_container_width=True)

        last_updated = df.last_updated.min()
        fig_caption = "Data Source : <a href=''>Client prediction by Blockprint</a> ; Last Updated : {} ; Update Frequency: Monthly</h1>".format(last_updated.date())
        set_fig_caption(fig_caption)

        df_2 = get_proposals_by_client()
        fig = px.area(df_2, x="proposal_month", y="total_proposals", color="predicted_client", 
                    color_discrete_map=client_color,pattern_shape="predicted_client", pattern_shape_sequence=[".", "x", "+","-",'|'],
                    title="Client Breakdown (by block proposals)")
        fig.update_layout(xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
        fig.update_layout(xaxis_title="Month", yaxis_title="Total Proposals", legend_title="Client")
        st.plotly_chart(fig, use_container_width=True)

        fig_caption = "Data Source : <a href=''>Client prediction by Blockprint</a> ; Last Updated : {} ; Update Frequency: Monthly</h1>".format(last_updated.date())
        set_fig_caption(fig_caption)

    with tab4:
        col1, col2, col3 = st.columns(3)
        with col2:
            df_client_performance_apr = get_client_performance_apr()
            df_client_performance_apr = df_client_performance_apr.set_index('client')
            st.text("Client Performance (Last 31 days)")
            df_client_performance_apr.rename(columns= {
                'num_validators': 'Total Validators',
                'median_apr': 'Median APR',
            }, inplace=True)
            st.dataframe(df_client_performance_apr.style.text_gradient(subset='Median APR', cmap="Greens", vmin=-5, vmax=5).format({'Median APR': "{:.2%}"}))
        
        st.text("Client Performance (Last 31 days) Distribution")
        df_grouped_data = get_client_performance_apr_quartiles()
        df_grouped_data['APR'] = df_grouped_data['APR'] * 100
        fig = px.box(df_grouped_data, x="Client", y="APR", color="Client", color_discrete_map=client_color)
        fig.update_layout(xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
        fig.update_layout(yaxis_title="APR %")
        st.plotly_chart(fig, use_container_width=True)

    

with mt2:
#if rad == "Staking":

    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Entity Distribution", "Entity Performance", "Entity Diversity"])

    with tab1:

        df_staking_overview = get_staking_overview()

        depositor_change = str(round(df_staking_overview.depositor_change*100,1).iloc[0]) + " %"
        validator_change = str(round(df_staking_overview.validator_change*100,1).iloc[0]) + " %"

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Depositors", str(int(df_staking_overview.total_depositors/1000)) + "K+", depositor_change, help="test")
        col2.metric("Total Validators", str(int(df_staking_overview.num_validators/1000)) + "K+", validator_change)
        col3.metric("Total Eth Deposited", str(round(df_staking_overview.eth_deposited/1000000,1).iloc[0]) + "M+", validator_change)

        col1, col2 = st.columns(2)

        with col1:

            df_validators = get_weekly_validator_signups()

            fig = px.bar(df_validators, x='first_deposit_week', y='validators_signed_up', color='validators_signed_up', title="Validator Signups")
            fig.update_layout(yaxis_title="Total Validators", xaxis_title='Week')
            st.plotly_chart(fig, use_container_width=True)

        with col2:

            df_depositors = get_weekly_depositor_signups()

            fig = px.bar(df_depositors, x='week', y='depositor_count', color='depositor_count', title="Depositor Signups")
            fig.update_layout(yaxis_title="Total Depositors", xaxis_title='Week')
            st.plotly_chart(fig, use_container_width=True)


    with tab2:

        df_data = get_depositor_staking()

        fig = px.pie(df_data, values='total_eth_deposited', names='depositor_label', title='Depositor Staking Distribution')
        fig.update_traces(textposition='inside')
        fig.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')
        st.plotly_chart(fig, use_container_width=True)

        df_data['total_eth_deposited'] = df_data['total_eth_deposited'].astype(int)
        df_data['eth_deposited_last_30days'] = df_data['eth_deposited_last_30days'].astype(int)

        df_data = df_data[~df_data.depositor_type.isna()]
        df_data.set_index('depositor_label', inplace=True)

        df_data['Change %'] = (df_data['eth_deposited_last_30days']) / df_data['total_eth_deposited']

        df_data.rename(columns = {
            'depositor_type' : 'Depositor Type',
            'total_eth_deposited':'Total ETH Deposited',    
            'eth_deposited_last_30days':'Total ETH Deposited (Last 30 Days)',    
        }, inplace=True)

        fig_col1, fig_col2 = st.columns(2)

        with fig_col1:
            fig1_cols = ['Depositor Type', 'Total ETH Deposited','Total ETH Deposited (Last 30 Days)' ]
            st.text("Trending by ETH deposited(Last 30 Days)")
            st.dataframe(df_data[fig1_cols].sort_values('Total ETH Deposited (Last 30 Days)', ascending=False).head(5)
            .style.set_properties(**{'color': 'green'}, subset=['Total ETH Deposited (Last 30 Days)'])
            .format({'Total ETH Deposited': "{:,}", 'Total ETH Deposited (Last 30 Days)': "{:,}"}))           

        with fig_col2:
            st.text("Trending by Growth(Change in Last 30 Days)")
            st.dataframe(df_data.sort_values('Change %', ascending=False).head(5)
            .style.set_properties(**{'color': 'green'}, subset=['Change %'])
            .format({'Total ETH Deposited': "{:,}", 'Total ETH Deposited (Last 30 Days)': "{:,}"})
            .format({'Change %': "{:.2%}"}))

    with tab3:

        col1, col2, col3 = st.columns(3)
        with col2:
            df_depositor_performance_apr = get_depositor_performance_apr()
            df_depositor_performance_apr = df_depositor_performance_apr.set_index('depositor_label')
            st.text("Depositor Performance (Last 31 days)")
            df_depositor_performance_apr.rename(columns= {
                'num_validators': 'Total Validators',
                'avg_apr': 'Average APR',
            }, inplace=True)
            st.dataframe(df_depositor_performance_apr.style.text_gradient(subset='Average APR', cmap="Greens", vmin=-5, vmax=5).format({'Average APR': "{:.2%}"}), width=500)

        st.text("Depositor Performance (Last 31 days) Distribution")
        df_grouped_data = get_depositor_performance_apr_quartiles()
        df_grouped_data['APR'] = df_grouped_data['APR'] * 100
        fig = px.box(df_grouped_data, x="Depositor", y="APR", color="Depositor")
        fig.update_layout(xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
        fig.update_layout(yaxis_title="APR %")
        st.plotly_chart(fig, use_container_width=True)

    with tab4:

        st.text("Staking Entity Client Distribution")
        df_data = get_staking_client_distribution()

        df_2 = df_data.pivot(index="staking_entity", columns="client", values="tot_validators").reset_index().fillna(0)
        df_2['Total Validators'] = df_2['Lighthouse'] + df_2['Lodestar'] + df_2['Nimbus'] + df_2['Prysm'] + df_2['Teku'] 
        
        df_2.set_index('staking_entity', inplace=True)

        

        subset_cols = ['Lighthouse','Prysm','Teku', 'Nimbus', 'Lodestar']

        for col in subset_cols:
            df_2[col] = df_2[col].astype(int)

        df_2['Total Validators'] = df_2['Total Validators'].astype(int)

        def gini(x):
            # Mean absolute difference.
            mad = np.abs(np.subtract.outer(x, x)).mean()
            # Relative mean absolute difference
            rmad = mad / np.mean(x)
            # Gini coefficient is half the relative mean absolute difference.
            return 0.5 * rmad

        def get_client_diversity_coefficient(x):

            return  gini(list(x))

        df_2['Diversity Coefficient'] = df_2.apply(lambda x: get_client_diversity_coefficient(x[subset_cols]),axis=1)


        cols = ['Diversity Coefficient','Lighthouse','Prysm','Teku', 'Nimbus', 'Lodestar', 'Total Validators']

        #st.dataframe(df_2[cols].sort_values('Diversity Coefficient').style.highlight_min(color='red',axis=1, subset=subset_cols).highlight_max(color='green', axis=1, subset=subset_cols)
        #.format({'Diversity Coefficient': "{:.2f}"}), height=1024)

        st.dataframe(df_2[cols].sort_values('Diversity Coefficient').style.background_gradient(cmap='RdYlGn',axis=1, subset=subset_cols)
        .format({'Diversity Coefficient': "{:.2f}"}), height=1024)

        #st.dataframe(df_2.style.bar(align='mid', color=['red','green']))

with mt3:
#if rad == "Staking":

    tab1, tab2 = st.tabs(["Block Stats", "Coming soon!"])

    with tab1:
        df_data = get_empty_block_stats()

        col1, col2 = st.columns(2)

        with col1:

            fig = px.bar(df_data, x="day", y="empty_blocks", title="Empty blocks (Last 3 Months)")

            fig.update_layout(yaxis_title="Empty Blocks", xaxis_title='Day')
            fig.update_layout(xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
            st.plotly_chart(fig, use_container_width=True)

        with col2:

            fig = px.bar(df_data, x="day", y="missed_slots", title="Missed Slots (Last 3 Months)")

            fig.update_layout(yaxis_title="Missed Slots", xaxis_title='Day')
            fig.update_layout(xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
            st.plotly_chart(fig, use_container_width=True)