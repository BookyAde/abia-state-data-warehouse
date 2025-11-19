import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import plotly.express as px

load_dotenv()

# Connection
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

# Load full analytical view
@st.cache_data
def load_data():
    query = """
    SELECT 
        l.lga_name,
        s.school_name,
        s.school_type,
        s.ownership,
        f.enrollment_total,
        f.enrollment_male,
        f.enrollment_female,
        f.female_enrollment_pct,
        f.teachers_total,
        f.student_teacher_ratio,
        f.classrooms
    FROM dwh.fact_education f
    JOIN dwh.dim_school s ON f.school_key = s.school_key
    JOIN dwh.dim_lga l ON f.lga_key = l.lga_key
    """
    return pd.read_sql(query, engine)

df = load_data()

# === DASHBOARD STARTS HERE ===
st.set_page_config(page_title="Abia State Education Dashboard", layout="wide")
st.title("🏫 Abia State Education Data Warehouse - Live Dashboard")
st.markdown("**A mini data warehouse project | Landing → Staging → Star Schema → Analytics**")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Schools", len(df))
col2.metric("Total Students", df['enrollment_total'].sum())
col3.metric("Avg Student-Teacher Ratio", round(df['student_teacher_ratio'].mean(), 1))
col4.metric("Avg Female Enrollment %", f"{df['female_enrollment_pct'].mean():.1f}%")

st.markdown("---")

# Filters
st.sidebar.header("Filters")
selected_lga = st.sidebar.multiselect("Select LGA(s)", options=sorted(df['lga_name'].unique()), default=df['lga_name'].unique())
selected_ownership = st.sidebar.multiselect("Ownership", options=['Public', 'Private'], default=['Public', 'Private'])

filtered = df[df['lga_name'].isin(selected_lga) & df['ownership'].isin(selected_ownership)]

# Charts
colA, colB = st.columns(2)

with colA:
    fig1 = px.bar(filtered.groupby('lga_name')['student_teacher_ratio'].mean().round(2).reset_index(),
                  x='lga_name', y='student_teacher_ratio', title="Average Student-Teacher Ratio by LGA",
                  color='student_teacher_ratio', color_continuous_scale='Reds')
    st.plotly_chart(fig1, use_container_width=True)

with colB:
    fig2 = px.pie(filtered['ownership'].value_counts().reset_index(), values='count', names='ownership',
                  title="Public vs Private Schools")
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")
st.subheader("Detailed School Data")
st.dataframe(filtered.reset_index(drop=True), use_container_width=True)

st.success("Your full data warehouse is live! Use the filters on the left to explore.")