import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime
import os

# ========== STREAMLIT CLOUD CONFIG ==========
# Utilise les secrets pour MongoDB (Atlas Cloud)
@st.cache_resource(ttl=10)
def get_mongo_client():
    try:
        # Pour Streamlit Cloud: utilise MONGO_URI depuis les secrets
        MONGO_URI = st.secrets.get("MONGO_URI", "mongodb://localhost:27017")
        return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    except:
        # Fallback pour développement local
        return MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)


# ========== PAGE CONFIGURATION ==========
st.set_page_config(
    page_title="Valorant Analytics Dashboard",
    page_icon="🎯",  # Émoji au lieu de .ico pour Streamlit Cloud
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== CUSTOM CSS FOR MODERN LOOK ==========
st.markdown("""
<style>
    .main { background-color: #0F172A; }

    .metric-card {
        background: linear-gradient(135deg, #1E293B 0%, #334155 100%);
        border-radius: 12px;
        padding: 20px;
        border-left: 4px solid #3B82F6;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }

    .dashboard-header {
        background: linear-gradient(90deg, #3B82F6 0%, #8B5CF6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.8rem;
        font-weight: 800;
        margin-bottom: 0.5rem;
    }

    .section-header {
        color: #F1F5F9;
        font-size: 1.5rem;
        font-weight: 600;
        margin-top: 2rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #3B82F6;
    }

    .stat-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #60A5FA;
    }

    .stat-label {
        font-size: 0.9rem;
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
</style>
""", unsafe_allow_html=True)

# ========== SIDEBAR CONFIGURATION ==========
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <h2 style="color: #60A5FA; margin-bottom: 30px;">VALORANT ANALYTICS</h2>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"**Last Updated:** {datetime.now().strftime('%H:%M:%S')}")
    st.markdown("---")

    st.markdown("### 📊 Data Source")
    st.info("Real-time Valorant analytics dashboard with MongoDB")

    st.markdown("---")

    if st.button("🔄 Refresh Dashboard", type="primary", use_container_width=True):
        st.rerun()

# ========== TITLE & HEADER ==========
col_title1, col_title2, col_title3 = st.columns([2, 1, 1])

with col_title1:
    st.markdown('<h1 class="dashboard-header">VALORANT ANALYTICS DASHBOARD</h1>', unsafe_allow_html=True)
    st.markdown("### Professional Gaming Performance Dashboard")

with col_title3:
    try:
        client = get_mongo_client()
        client.server_info()
        st.success("✅ MongoDB Connected")
    except Exception as e:
        st.warning("⚠️ Using Sample Data")

# ========== MAIN DASHBOARD ==========
try:
    client = get_mongo_client()
    db = client.valorant_stats
    stats = list(db.realtime_stats.find({}, {'_id': 0}).limit(100))

    if stats:
        df = pd.DataFrame(stats)
        data_source = "MongoDB"
    else:
        # Fallback data for demo
        st.info("📊 Using sample data for demonstration")
        df = pd.DataFrame({
            'player': [f'Player{i}' for i in range(1, 21)],
            'kd_ratio': [1.2 + i * 0.1 for i in range(20)],
            'headshot_percentage': [25 + i * 3 for i in range(20)],
            'kills': [15 + i for i in range(20)],
            'deaths': [10 + i // 2 for i in range(20)],
            'tier': ['Gold', 'Platinum', 'Diamond'] * 7,
            'damage_per_kill': [120 + i * 5 for i in range(20)],
            'impact_score': [50 + i * 2 for i in range(20)]
        })
        data_source = "Sample Data"

except Exception as e:
    st.warning(f"⚠️ Database connection issue: {str(e)[:50]}... Using sample data.")
    df = pd.DataFrame({
        'player': [f'Player{i}' for i in range(1, 11)],
        'kd_ratio': [1.2, 1.5, 0.8, 2.1, 1.3, 1.7, 1.0, 1.4, 1.8, 0.9],
        'headshot_percentage': [25, 30, 20, 40, 28, 35, 22, 32, 38, 18],
        'kills': [15, 20, 10, 25, 18, 22, 12, 19, 24, 8]
    })
    data_source = "Sample Data"

# ========== TOP METRICS ROW ==========
st.markdown(f'<div class="section-header">📈 KEY PERFORMANCE INDICATORS ({data_source})</div>', unsafe_allow_html=True)

metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)

with metric_col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="stat-label">TOTAL PLAYERS</div>
        <div class="stat-value">{len(df)}</div>
    </div>
    """, unsafe_allow_html=True)

with metric_col2:
    avg_kd = df['kd_ratio'].mean() if 'kd_ratio' in df.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="stat-label">AVG K/D RATIO</div>
        <div class="stat-value">{avg_kd:.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with metric_col3:
    avg_hs = df['headshot_percentage'].mean() if 'headshot_percentage' in df.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="stat-label">AVG HEADSHOT %</div>
        <div class="stat-value">{avg_hs:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

with metric_col4:
    top_kd = df['kd_ratio'].max() if 'kd_ratio' in df.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="stat-label">TOP K/D RATIO</div>
        <div class="stat-value">{top_kd:.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with metric_col5:
    active_players = len(df[df['kd_ratio'] > 1.0]) if 'kd_ratio' in df.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="stat-label">POSITIVE K/D</div>
        <div class="stat-value">{active_players}</div>
    </div>
    """, unsafe_allow_html=True)

# ========== MAIN CONTENT AREA ==========
tab1, tab2, tab3 = st.tabs(["📊 Performance Analysis", "📋 Player Statistics", "🏆 Leaderboard"])

with tab1:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### K/D Ratio Distribution")
        if 'kd_ratio' in df.columns:
            fig_kd = px.histogram(df, x='kd_ratio', nbins=20,
                                  color_discrete_sequence=['#3B82F6'])
            fig_kd.update_layout(plot_bgcolor='rgba(0,0,0,0)',
                                 paper_bgcolor='rgba(0,0,0,0)',
                                 font_color='#F1F5F9')
            st.plotly_chart(fig_kd, use_container_width=True)

    with col2:
        st.markdown("#### Headshot Accuracy vs K/D Ratio")
        if 'headshot_percentage' in df.columns and 'kd_ratio' in df.columns:
            fig_scatter = px.scatter(df, x='headshot_percentage', y='kd_ratio',
                                     hover_data=['player'],
                                     color_discrete_sequence=['#10B981'])
            fig_scatter.update_layout(plot_bgcolor='rgba(0,0,0,0)',
                                      paper_bgcolor='rgba(0,0,0,0)',
                                      font_color='#F1F5F9')
            st.plotly_chart(fig_scatter, use_container_width=True)

with tab2:
    st.markdown("#### Player Statistics")

    display_cols = ['player', 'kd_ratio', 'headshot_percentage', 'kills']
    display_cols = [col for col in display_cols if col in df.columns]

    if display_cols:
        styled_df = df[display_cols].head(15).copy()

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "player": st.column_config.TextColumn("Player", width="medium"),
                "kd_ratio": st.column_config.NumberColumn("K/D", format="%.2f"),
                "headshot_percentage": st.column_config.NumberColumn("HS%", format="%.1f%%"),
            }
        )

with tab3:
    st.markdown("#### Top Performers")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### Top 5 K/D Ratio")
        if 'kd_ratio' in df.columns:
            top_kd = df[['player', 'kd_ratio']].nlargest(5, 'kd_ratio')
            for _, row in top_kd.iterrows():
                st.markdown(f"""
                <div style="background: #1E293B; padding: 10px; margin: 5px 0; border-radius: 8px;">
                    <strong>{row['player']}</strong>
                    <span style="color: #60A5FA; float: right;">{row['kd_ratio']:.2f}</span>
                </div>
                """, unsafe_allow_html=True)

    with col2:
        st.markdown("##### Top 5 Headshot %")
        if 'headshot_percentage' in df.columns:
            top_hs = df[['player', 'headshot_percentage']].nlargest(5, 'headshot_percentage')
            for _, row in top_hs.iterrows():
                st.markdown(f"""
                <div style="background: #1E293B; padding: 10px; margin: 5px 0; border-radius: 8px;">
                    <strong>{row['player']}</strong>
                    <span style="color: #10B981; float: right;">{row['headshot_percentage']:.1f}%</span>
                </div>
                """, unsafe_allow_html=True)

# ========== FOOTER ==========
st.markdown("---")
st.caption(f"**Valorant Analytics Dashboard** | {data_source} | Last update: {datetime.now().strftime('%H:%M:%S')}")