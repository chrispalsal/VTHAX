import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from databricks.sdk.core import Config
import os

# ============================================================
# VT Campus WiFi Connectivity Heatmap
# Student-facing app for checking eduroam connectivity quality
# ============================================================

st.set_page_config(page_title="VT WiFi Heatmap", page_icon="📡", layout="wide")

@st.cache_resource
def get_connection():
    """Connect to Databricks SQL using the app's service principal."""
    cfg = Config()
    host = cfg.host
    if host and host.startswith("https://"):
        host = host.replace("https://", "")
    elif host and host.startswith("http://"):
        host = host.replace("http://", "")
    
    # Use the SQL warehouse - try to find available warehouses
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    if not warehouses:
        st.error("No SQL warehouses available. Ask your workspace admin to create one.")
        st.stop()
    
    warehouse = warehouses[0]  # Use first available warehouse
    http_path = f"/sql/1.0/warehouses/{warehouse.id}"
    
    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
        _use_arrow_native_complex_types=False,
    )

@st.cache_data(ttl=60)
def run_query(query):
    """Execute a SQL query and return results as a pandas DataFrame."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

def get_zone_summary():
    """Get gold zone summary data."""
    return run_query("""
        SELECT zone_id, building, floor, hour_of_day, 
               measurement_count, avg_signal_pct, avg_rssi_dbm,
               avg_gw_latency_ms, avg_inet_latency_ms, avg_jitter_ms,
               avg_packet_loss_pct, avg_tcp_down_mbps, avg_tcp_up_mbps,
               teams_quality, last_updated
        FROM vt_connectivity.campus.gold_zone_summary
        ORDER BY zone_id, hour_of_day
    """)

def get_zones():
    """Get distinct zones."""
    return run_query("SELECT DISTINCT zone_id, building, floor FROM vt_connectivity.campus.gold_zone_summary ORDER BY zone_id")

# ============================================================
# APP UI
# ============================================================

st.title("📡 VT Campus WiFi Heatmap")
st.markdown("### Virginia Tech eduroam Connectivity Monitor")
st.markdown("Find the best spots on campus for Teams/Zoom calls, and see where WiFi is struggling.")

# Load data
with st.spinner("Loading connectivity data..."):
    try:
        df = get_zone_summary()
        zones_df = get_zones()
    except Exception as e:
        st.error(f"Failed to connect to Databricks: {e}")
        st.info("Make sure the app's service principal has access to the `vt_connectivity` catalog.")
        st.stop()

if df.empty:
    st.warning("No connectivity data found. Run the pipeline notebook first.")
    st.stop()

# Sidebar
st.sidebar.markdown("### 🎛️ Controls")
selected_building = st.sidebar.selectbox("Building", df["building"].unique() if "building" in df.columns else ["All"])

# Filter by building
if selected_building != "All":
    df_filtered = df[df["building"] == selected_building]
else:
    df_filtered = df

# ============================================================
# TAB 1: Heatmap
# ============================================================
tab1, tab2, tab3 = st.tabs(["🗺️ Heatmap", "✅ Teams Call Checker", "📊 Zone Details"])

with tab1:
    st.subheader("WiFi Signal Strength by Zone & Hour")
    
    metric = st.selectbox("Color metric", [
        "avg_signal_pct", "avg_inet_latency_ms", "avg_tcp_down_mbps", 
        "avg_packet_loss_pct", "avg_jitter_ms"
    ], format_func=lambda x: {
        "avg_signal_pct": "Signal Strength (%)",
        "avg_inet_latency_ms": "Internet Latency (ms)",
        "avg_tcp_down_mbps": "Download Speed (Mbps)",
        "avg_packet_loss_pct": "Packet Loss (%)",
        "avg_jitter_ms": "Jitter (ms)"
    }.get(x, x))
    
    # Pivot for heatmap
    pivot = df_filtered.pivot_table(index="hour_of_day", columns="zone_id", values=metric, aggfunc="mean")
    
    color_scale = "RdYlGn" if metric in ["avg_signal_pct", "avg_tcp_down_mbps"] else "RdYlGn_r"
    
    fig = px.imshow(
        pivot,
        labels=dict(x="Zone", y="Hour of Day", color=metric),
        x=pivot.columns,
        y=pivot.index,
        color_continuous_scale=color_scale,
        aspect="auto",
        title=f"{metric.replace('_', ' ').title()} - {selected_building}"
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Teams quality heatmap
    st.subheader("Teams Call Quality by Zone & Hour")
    quality_pivot = df_filtered.pivot_table(index="hour_of_day", columns="zone_id", values="teams_quality", aggfunc="first")
    
    # Convert quality to numeric for color
    quality_map = {"good": 3, "moderate": 2, "poor": 1}
    quality_numeric = quality_pivot.replace(quality_map)
    
    fig2 = px.imshow(
        quality_numeric,
        labels=dict(x="Zone", y="Hour of Day", color="Quality"),
        color_continuous_scale=[[0, "#ff4444"], [0.5, "#ffaa00"], [1, "#44aa44"]],
        aspect="auto",
        title="Teams Call Quality (Green=Good, Yellow=Moderate, Red=Poor)"
    )
    fig2.update_layout(height=400)
    st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# TAB 2: Teams Call Checker
# ============================================================
with tab2:
    st.subheader("Can I take a Teams call here?")
    st.markdown("Teams requires: ≥1.5 Mbps down, <150ms latency, <1% packet loss")
    
    col1, col2 = st.columns(2)
    with col1:
        selected_zone = st.selectbox("Select your zone", zones_df["zone_id"].unique() if not zones_df.empty else df_filtered["zone_id"].unique())
    with col2:
        selected_hour = st.slider("Hour of day", 7, 23, 14)
    
    zone_hour_data = df_filtered[
        (df_filtered["zone_id"] == selected_zone) & 
        (df_filtered["hour_of_day"] == selected_hour)
    ]
    
    if not zone_hour_data.empty:
        row = zone_hour_data.iloc[0]
        quality = row["teams_quality"]
        
        if quality == "good":
            st.success(f"✅ Yes! Zone `{selected_zone}` is great for a Teams call at {selected_hour}:00.")
        elif quality == "moderate":
            st.warning(f"⚠️ Zone `{selected_zone}` is moderate at {selected_hour}:00. You might experience some lag.")
        else:
            st.error(f"❌ Zone `{selected_zone}` is too poor for a Teams call at {selected_hour}:00. Find a better spot!")
        
        # Show metrics
        mcol1, mcol2, mcol3, mcol4 = st.columns(4)
        with mcol1:
            st.metric("Signal", f"{row['avg_signal_pct']:.0f}%")
        with mcol2:
            st.metric("Latency", f"{row['avg_inet_latency_ms']:.0f}ms")
        with mcol3:
            st.metric("Download", f"{row['avg_tcp_down_mbps']:.1f}Mbps")
        with mcol4:
            st.metric("Packet Loss", f"{row['avg_packet_loss_pct']:.1f}%")
        
        # Recommend better zones
        if quality != "good":
            st.markdown("#### 🏃 Better zones nearby:")
            better_zones = df_filtered[
                (df_filtered["hour_of_day"] == selected_hour) & 
                (df_filtered["teams_quality"] == "good") &
                (df_filtered["zone_id"] != selected_zone)
            ].nlargest(3, "avg_signal_pct")
            
            if not better_zones.empty:
                for _, bz in better_zones.iterrows():
                    st.markdown(f"- **{bz['zone_id']}** (Floor {bz['floor']}): Signal {bz['avg_signal_pct']:.0f}%, Latency {bz['avg_inet_latency_ms']:.0f}ms, {bz['avg_tcp_down_mbps']:.1f}Mbps down")
            else:
                st.info("No good zones found at this hour. Try a different time!")
    else:
        st.warning(f"No data for zone `{selected_zone}` at hour {selected_hour}.")

# ============================================================
# TAB 3: Zone Details
# ============================================================
with tab3:
    st.subheader("Zone Quality Summary")
    
    display_cols = ["zone_id", "floor", "hour_of_day", "avg_signal_pct", "avg_inet_latency_ms", 
                    "avg_jitter_ms", "avg_packet_loss_pct", "avg_tcp_down_mbps", "teams_quality"]
    display_cols = [c for c in display_cols if c in df_filtered.columns]
    
    st.dataframe(
        df_filtered[display_cols].rename(columns={
            "zone_id": "Zone",
            "floor": "Floor",
            "hour_of_day": "Hour",
            "avg_signal_pct": "Signal %",
            "avg_inet_latency_ms": "Latency ms",
            "avg_jitter_ms": "Jitter ms",
            "avg_packet_loss_pct": "Loss %",
            "avg_tcp_down_mbps": "Down Mbps",
            "teams_quality": "Teams Quality"
        }),
        use_container_width=True,
        hide_index=True
    )
    
    # Bar chart: average signal by zone
    st.subheader("Average Signal by Zone")
    zone_avg = df_filtered.groupby("zone_id")["avg_signal_pct"].mean().reset_index()
    zone_avg = zone_avg.sort_values("avg_signal_pct", ascending=True)
    
    fig3 = px.bar(
        zone_avg,
        x="avg_signal_pct",
        y="zone_id",
        orientation="h",
        labels={"avg_signal_pct": "Average Signal %", "zone_id": "Zone"},
        color="avg_signal_pct",
        color_continuous_scale="RdYlGn",
        title="Average WiFi Signal Strength by Zone"
    )
    fig3.update_layout(height=400)
    st.plotly_chart(fig3, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.markdown("Built for Virginia Tech students 🐾")
st.sidebar.markdown("Data from eduroam network monitoring")