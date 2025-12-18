import streamlit as st
import pandas as pd
import random 
import plotly.express as px
import plotly.graph_objects as go

from pymongo import MongoClient
from datetime import datetime
from PIL import Image  # Ajoute cette importation

st.set_page_config(
    page_title="Valorant Analytics Dashboard",
    page_icon="⚔️",  # Émoji au lieu de logo.ico
    layout="wide",
    initial_sidebar_state="expanded"
)
# ========== CUSTOM CSS FOR MODERN LOOK ==========
st.markdown("""
<style>
    /* Main container styling */
    .main {
        background-color: #0F172A;
    }

    /* Card styling */
    .metric-card {
        background: linear-gradient(135deg, #1E293B 0%, #334155 100%);
        border-radius: 12px;
        padding: 20px;
        border-left: 4px solid #3B82F6;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }

    /* Header styling */
    .dashboard-header {
        background: linear-gradient(90deg, #3B82F6 0%, #8B5CF6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.8rem;
        font-weight: 800;
        margin-bottom: 0.5rem;
    }

    /* Subheader styling */
    .section-header {
        color: #F1F5F9;
        font-size: 1.5rem;
        font-weight: 600;
        margin-top: 2rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #3B82F6;
    }

    /* Statistic value styling */
    .stat-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #60A5FA;
    }

    /* Statistic label styling */
    .stat-label {
        font-size: 0.9rem;
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Styling for dataframes */
    .dataframe {
        background-color: #1E293B !important;
        border-radius: 8px;
    }

    /* Badge for tier */
    .tier-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
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

    # Last update time
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%H:%M:%S')}")

    st.markdown("---")

    # Data source info
    st.markdown("Data Source")
    st.info("""
    Real-time processing pipeline:
    1. CSV → Kafka Producer
    2. Spark Stream Processing
    3. MongoDB Storage
    4. Streamlit Visualization
    """)

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Refresh Dashboard", type="primary", use_container_width=True):
        st.rerun()

# ========== TITLE & HEADER ==========
col_title1, col_title2, col_title3 = st.columns([2, 1, 1])

with col_title1:
    st.markdown('<h1 class="dashboard-header">VALORANT REAL-TIME ANALYTICS</h1>', unsafe_allow_html=True)
    st.markdown("### Professional Gaming Performance Dashboard")

with col_title3:
    # System status indicator
    st.success("✅ Dashboard Active")


# ========== MAIN DASHBOARD ==========

# MongoDB connection
@st.cache_resource(ttl=10)
def get_mongo_client():
    import os
    # Option 1: Try using the cloud secret (for Streamlit Cloud)
    try:
        # For a standard connection string in secrets
        connection_string = st.secrets["MONGO_URI"]
    except:
        # Option 2: Fallback for local development with Docker
        connection_string = "mongodb://mongodb:27017"

    return MongoClient(connection_string, serverSelectionTimeoutMS=5000)


try:
    client = get_mongo_client()
    db = client.valorant_stats

    # Get data with error handling
    stats = list(db.realtime_stats.find({}, {'_id': 0}).limit(200))

    if not stats:
        st.warning("⏳ Running in DEMO mode with sample data")
        df = get_demo_data()  # Utilise les données de démo
    else:
        df = pd.DataFrame(stats)

    # ========== TOP METRICS ROW ==========
    st.markdown('<div class="section-header"> KEY PERFORMANCE INDICATORS</div>', unsafe_allow_html=True)

    metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)

    with metric_col1:
        st.markdown("""
        <div class="metric-card">
            <div class="stat-label">TOTAL PLAYERS</div>
            <div class="stat-value">""" + str(len(df)) + """</div>
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
    tab1, tab2, tab3, tab4 = st.tabs(
        [" Performance Analysis", " Player Statistics", " Rankings", "Advanced Metrics"])

    with tab1:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### K/D Ratio Distribution")
            if 'kd_ratio' in df.columns:
                fig_kd = px.histogram(df, x='kd_ratio', nbins=30,
                                      color_discrete_sequence=['#3B82F6'],
                                      opacity=0.8)
                fig_kd.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F1F5F9',
                    bargap=0.1
                )
                st.plotly_chart(fig_kd, use_container_width=True)

        with col2:
            st.markdown("#### Headshot Accuracy vs K/D Ratio")
            if 'headshot_percentage' in df.columns and 'kd_ratio' in df.columns:
                fig_scatter = px.scatter(df, x='headshot_percentage', y='kd_ratio',
                                         color='tier' if 'tier' in df.columns else None,
                                         hover_data=['player'],
                                         size='kills' if 'kills' in df.columns else None,
                                         color_discrete_sequence=px.colors.qualitative.Set3)
                fig_scatter.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F1F5F9'
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

    with tab2:
        st.markdown("Player Statistics Table")

        # Create styled dataframe
        display_cols = ['player', 'kd_ratio', 'headshot_percentage',
                        'damage_per_kill', 'impact_score', 'tier', 'kills', 'deaths']
        display_cols = [col for col in display_cols if col in df.columns]

        if display_cols:
            styled_df = df[display_cols].head(20).copy()

            # Format numeric columns
            if 'kd_ratio' in styled_df.columns:
                styled_df['kd_ratio'] = styled_df['kd_ratio'].map('{:.2f}'.format)
            if 'headshot_percentage' in styled_df.columns:
                styled_df['headshot_percentage'] = styled_df['headshot_percentage'].map('{:.1f}%'.format)
            if 'damage_per_kill' in styled_df.columns:
                styled_df['damage_per_kill'] = styled_df['damage_per_kill'].map('{:.0f}'.format)

            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "player": st.column_config.TextColumn("Player", width="medium"),
                    "kd_ratio": st.column_config.ProgressColumn(
                        "K/D Ratio",
                        format="%.2f",
                        min_value=0,
                        max_value=5,
                    ),
                    "headshot_percentage": st.column_config.ProgressColumn(
                        "HS%",
                        format="%.1f%%",
                        min_value=0,
                        max_value=100,
                    )
                }
            )

    with tab3:
        st.markdown("####  Top Performers Leaderboard")

        ranking_col1, ranking_col2, ranking_col3 = st.columns(3)

        with ranking_col1:
            st.markdown("##### Top 5 K/D Ratio")
            if 'kd_ratio' in df.columns:
                top_kd = df[['player', 'kd_ratio', 'kills', 'deaths']].nlargest(5, 'kd_ratio')
                for idx, row in top_kd.iterrows():
                    st.markdown(f"""
                    <div style="background: #1E293B; padding: 10px; margin: 5px 0; border-radius: 8px;">
                        <strong>{row['player']}</strong><br>
                        <span style="color: #60A5FA;">K/D: {row['kd_ratio']:.2f}</span> | 
                        <small>K: {row['kills']} D: {row['deaths']}</small>
                    </div>
                    """, unsafe_allow_html=True)

        with ranking_col2:
            st.markdown("##### Top 5 Headshot %")
            if 'headshot_percentage' in df.columns:
                top_hs = df[['player', 'headshot_percentage', 'kills', 'headshots']].nlargest(5, 'headshot_percentage')
                for idx, row in top_hs.iterrows():
                    st.markdown(f"""
                    <div style="background: #1E293B; padding: 10px; margin: 5px 0; border-radius: 8px;">
                        <strong>{row['player']}</strong><br>
                        <span style="color: #10B981;">HS%: {row['headshot_percentage']:.1f}%</span> | 
                        <small>HS: {row['headshots']} K: {row['kills']}</small>
                    </div>
                    """, unsafe_allow_html=True)

        with ranking_col3:
            st.markdown("##### Top 5 Impact Score")
            if 'impact_score' in df.columns:
                top_impact = df[['player', 'impact_score', 'kills', 'assists']].nlargest(5, 'impact_score')
                for idx, row in top_impact.iterrows():
                    st.markdown(f"""
                    <div style="background: #1E293B; padding: 10px; margin: 5px 0; border-radius: 8px;">
                        <strong>{row['player']}</strong><br>
                        <span style="color: #8B5CF6;">Impact: {row['impact_score']:.1f}</span> | 
                        <small>K: {row['kills']} A: {row['assists']}</small>
                    </div>
                    """, unsafe_allow_html=True)

    with tab4:
        adv_col1, adv_col2 = st.columns(2)

        with adv_col1:
            st.markdown("##### Damage Efficiency Analysis")
            if 'damage_per_kill' in df.columns and 'kd_ratio' in df.columns:
                fig_damage = px.scatter(df, x='damage_per_kill', y='kd_ratio',
                                        color='tier' if 'tier' in df.columns else None,
                                        size='kills' if 'kills' in df.columns else None,
                                        hover_data=['player'],

                                        color_discrete_sequence=px.colors.qualitative.Pastel)
                fig_damage.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F1F5F9',
                    title="Damage per Kill vs K/D Ratio Correlation"
                )
                st.plotly_chart(fig_damage, use_container_width=True)

        with adv_col2:
            st.markdown("##### Tier Performance Comparison")
            if 'tier' in df.columns and 'kd_ratio' in df.columns:
                tier_comparison = df.groupby('tier').agg({
                    'kd_ratio': 'mean',
                    'headshot_percentage': 'mean',
                    'damage_per_kill': 'mean'
                }).round(2).reset_index()

                fig_tier = go.Figure(data=[
                    go.Bar(name='Avg K/D', x=tier_comparison['tier'], y=tier_comparison['kd_ratio'],
                           marker_color='#3B82F6'),
                    go.Bar(name='Avg HS%', x=tier_comparison['tier'], y=tier_comparison['headshot_percentage'],
                           marker_color='#10B981'),
                ])
                fig_tier.update_layout(
                    barmode='group',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F1F5F9',
                    title="Performance Metrics by Tier"
                )
                st.plotly_chart(fig_tier, use_container_width=True)

    # ========== FOOTER ==========
    st.markdown("---")
    footer_col1, footer_col2, footer_col3 = st.columns([2, 1, 1])

    with footer_col1:
        st.caption(" **Valorant Real-Time Analytics Dashboard** ")

    with footer_col3:
        st.caption(f"Last data refresh: {datetime.now().strftime('%H:%M:%S')}")

except Exception as e:

    print("error")
