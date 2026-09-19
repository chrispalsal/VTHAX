import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from databricks.sdk.core import Config
import datetime
import os

# ============================================================
# VT Campus WiFi Live - Single-page interactive app
# Map + AI Agent + Live Alerts in one view
# ============================================================

st.set_page_config(page_title="VT WiFi Sentinel", page_icon="📡", layout="wide")

# --- Cybersecurity dark mode styling ---
st.markdown("""
<style>
    .stApp {
        background: #0a0e14;
        background-image: 
            linear-gradient(rgba(0, 255, 255, 0.03) 1px, transparent 1px),
            linear-gradient(90deg, rgba(0, 255, 255, 0.03) 1px, transparent 1px);
        background-size: 40px 40px;
    }
    .stMarkdown h1 { color: #00ffff; font-family: 'Courier New', monospace; text-shadow: 0 0 10px rgba(0,255,255,0.3); }
    .stMarkdown h2 { color: #e0e0e0; font-family: 'Courier New', monospace; }
    .stMarkdown h3 { color: #00ff88; font-family: 'Courier New', monospace; }
    .stMarkdown p { color: #b0b0b0; }
    .alert-poor {
        background: linear-gradient(90deg, rgba(255,68,68,0.15), rgba(255,68,68,0.05));
        border: 1px solid #ff4444; box-shadow: 0 0 15px rgba(255,68,68,0.3);
        color: #ff6666; padding: 10px 16px; border-radius: 4px;
        font-size: 14px; font-weight: bold; margin: 8px 0;
        font-family: 'Courier New', monospace;
    }
    .alert-good {
        background: linear-gradient(90deg, rgba(0,255,136,0.15), rgba(0,255,136,0.05));
        border: 1px solid #00ff88; box-shadow: 0 0 15px rgba(0,255,136,0.3);
        color: #00ff88; padding: 10px 16px; border-radius: 4px;
        font-size: 14px; font-weight: bold; margin: 8px 0;
        font-family: 'Courier New', monospace;
    }
    .alert-moderate {
        background: linear-gradient(90deg, rgba(255,170,0,0.15), rgba(255,170,0,0.05));
        border: 1px solid #ffaa00; box-shadow: 0 0 15px rgba(255,170,0,0.3);
        color: #ffaa00; padding: 10px 16px; border-radius: 4px;
        font-size: 14px; font-weight: bold; margin: 8px 0;
        font-family: 'Courier New', monospace;
    }
    .zone-card {
        background: #0d1117; padding: 12px 16px; border-radius: 4px;
        border: 1px solid #1a3a5a; margin: 6px 0;
        font-family: 'Courier New', monospace;
    }
    .stMetric { font-family: 'Courier New', monospace; }
    .stMetric label { color: #00ffff !important; font-size: 11px !important; text-transform: uppercase; }
    .stMetric value { color: #ffffff !important; }
    div[data-testid="stChatMessage"] {
        font-family: 'Courier New', monospace;
        background: #0d1117 !important;
        border: 1px solid #1a3a5a !important;
    }
    .stChatInput textarea {
        font-family: 'Courier New', monospace !important;
        background: #0d1117 !important;
        border: 1px solid #1a3a5a !important;
    }
    .stButton > button {
        background: #0d1117 !important;
        border: 1px solid #00ffff !important;
        color: #00ffff !important;
        font-family: 'Courier New', monospace !important;
        font-size: 13px !important;
    }
    .stButton > button:hover {
        background: rgba(0,255,255,0.1) !important;
        box-shadow: 0 0 10px rgba(0,255,255,0.3) !important;
    }
    .stSelectbox label, .stSlider label {
        color: #00ffff !important;
        font-family: 'Courier New', monospace !important;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# Data layer
# ============================================================

@st.cache_resource
def get_connection():
    cfg = Config()
    host = cfg.host
    if host and host.startswith("https://"): host = host.replace("https://", "")
    elif host and host.startswith("http://"): host = host.replace("http://", "")
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    if not warehouses:
        st.error("No SQL warehouses available.")
        st.stop()
    return sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{warehouses[0].id}",
                       credentials_provider=lambda: cfg.authenticate, _use_arrow_native_complex_types=False)

@st.cache_data(ttl=30)
def run_query(query):
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

def get_outdoor_data():
    return run_query("""
        SELECT zone_id, zone_label, lat, lon, hour_of_day,
               AVG(signal_pct) as avg_signal, AVG(bandwidth_mbps) as avg_mbps,
               AVG(latency_ms) as avg_latency, AVG(jitter_ms) as avg_jitter,
               AVG(packet_loss_pct) as avg_loss, teams_quality
        FROM vt_connectivity.campus.outdoor_measurements
        GROUP BY zone_id, zone_label, lat, lon, hour_of_day, teams_quality
        ORDER BY zone_id, hour_of_day
    """)

def get_indoor_data():
    return run_query("""
        SELECT zone_id, building, floor, hour_of_day,
               AVG(avg_signal_pct) as avg_signal, AVG(avg_tcp_down_mbps) as avg_mbps,
               AVG(avg_inet_latency_ms) as avg_latency, teams_quality
        FROM vt_connectivity.campus.gold_zone_summary
        GROUP BY zone_id, building, floor, hour_of_day, teams_quality
        ORDER BY zone_id, hour_of_day
    """)

# ============================================================
# Load data
# ============================================================

with st.spinner("Loading campus connectivity data..."):
    try:
        outdoor_df = get_outdoor_data()
        indoor_df = get_indoor_data()
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.stop()

if outdoor_df.empty:
    st.warning("No data found.")
    st.stop()

# ============================================================
# Session state
# ============================================================

if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        {"role": "assistant", "content": "Hey! I'm your VT WiFi assistant. Ask me where to find the best connection, or set your location above to get live alerts."}
    ]
if "current_zone" not in st.session_state:
    st.session_state.current_zone = "drillfield"
if "recommended_zone" not in st.session_state:
    st.session_state.recommended_zone = None
if "selected_hour" not in st.session_state:
    st.session_state.selected_hour = datetime.datetime.now().hour
    if st.session_state.selected_hour < 7 or st.session_state.selected_hour > 23:
        st.session_state.selected_hour = 14

# Get zone list for dropdown
all_zones = outdoor_df[["zone_id", "zone_label", "lat", "lon"]].drop_duplicates().sort_values("zone_label")
zone_labels = dict(zip(all_zones["zone_id"], all_zones["zone_label"]))

# ============================================================
# TOP BAR - Title + Location Selector + Live Status
# ============================================================

col_title, col_loc, col_status = st.columns([0.4, 0.3, 0.3])

with col_title:
    st.markdown("# VT WiFi Sentinel")
    st.markdown("*eduroam zone monitoring // real-time threat detection*")

with col_loc:
    selected_zone_id = st.selectbox(
        "📍 Your location", all_zones["zone_id"].unique(),
        format_func=lambda z: zone_labels.get(z, z),
        index=list(all_zones["zone_id"]).index(st.session_state.current_zone) if st.session_state.current_zone in list(all_zones["zone_id"]) else 0,
        key="zone_selector"
    )
    st.session_state.current_zone = selected_zone_id

# Get current hour data
hour = st.session_state.selected_hour
outdoor_hour = outdoor_df[outdoor_df["hour_of_day"] == hour].copy()
zone_agg = outdoor_hour.groupby(["zone_id", "zone_label", "lat", "lon"]).agg({
    "avg_signal": "mean", "avg_mbps": "mean", "avg_latency": "mean",
    "avg_jitter": "mean", "avg_loss": "mean", "teams_quality": "first"
}).reset_index()

# Current zone status
current_zone_data = zone_agg[zone_agg["zone_id"] == st.session_state.current_zone]
current_quality = current_zone_data["teams_quality"].iloc[0] if not current_zone_data.empty else "unknown"
current_signal = current_zone_data["avg_signal"].iloc[0] if not current_zone_data.empty else 0
current_mbps = current_zone_data["avg_mbps"].iloc[0] if not current_zone_data.empty else 0
current_latency = current_zone_data["avg_latency"].iloc[0] if not current_zone_data.empty else 0

with col_status:
    if current_quality == "poor":
        st.markdown(f'<div class="alert-poor">⚠️ DEAD ZONE — Signal {current_signal:.0f}% | {current_mbps:.1f} Mbps | {current_latency:.0f}ms</div>', unsafe_allow_html=True)
    elif current_quality == "moderate":
        st.markdown(f'<div class="alert-moderate">⚠️ MODERATE — Signal {current_signal:.0f}% | {current_mbps:.1f} Mbps</div>', unsafe_allow_html=True)
    elif current_quality == "good":
        st.markdown(f'<div class="alert-good">✅ GOOD — Signal {current_signal:.0f}% | {current_mbps:.1f} Mbps | {current_latency:.0f}ms</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="alert-moderate">❓ No data for this zone</div>', unsafe_allow_html=True)

st.markdown("---")

# ============================================================
# MAIN LAYOUT - Map (left) + AI Agent (right)
# ============================================================

map_col, chat_col = st.columns([0.62, 0.38])

# --- MAP (LEFT) ---
with map_col:
    # Time + metric controls (compact, inline)
    ctrl1, ctrl2 = st.columns([0.6, 0.4])
    with ctrl1:
        hour_val = st.slider("🕐 Time of day", 7, 23, hour, key="hour_slider")
        st.session_state.selected_hour = hour_val
    with ctrl2:
        metric = st.selectbox("Metric", ["avg_signal", "avg_mbps", "avg_latency"],
            format_func=lambda x: {"avg_signal": "Signal %", "avg_mbps": "Bandwidth", "avg_latency": "Latency"}.get(x, x),
            key="metric_select")
    
    # Refresh data for selected hour
    outdoor_hour = outdoor_df[outdoor_df["hour_of_day"] == hour_val].copy()
    zone_agg = outdoor_hour.groupby(["zone_id", "zone_label", "lat", "lon"]).agg({
        "avg_signal": "mean", "avg_mbps": "mean", "avg_latency": "mean",
        "avg_jitter": "mean", "avg_loss": "mean", "teams_quality": "first"
    }).reset_index()
    
    # Build map figure - cybersecurity dark mode with neon zone markers
    quality_colors = {"good": "#00ff88", "moderate": "#ffaa00", "poor": "#ff4444"}
    
    fig = go.Figure()
    
    # Glow layer (large, semi-transparent markers for neon effect)
    for q, color in quality_colors.items():
        q_data = zone_agg[zone_agg["teams_quality"] == q]
        if not q_data.empty:
            fig.add_trace(go.Scattermapbox(
                lat=q_data["lat"], lon=q_data["lon"],
                mode="markers",
                marker=dict(size=22, color=color, opacity=0.2),
                hoverinfo="none", name=f"glow_{q}", showlegend=False
            ))
    
    # Main zone markers (color by quality, size by signal)
    for q, color in quality_colors.items():
        q_data = zone_agg[zone_agg["teams_quality"] == q]
        if not q_data.empty:
            fig.add_trace(go.Scattermapbox(
                lat=q_data["lat"], lon=q_data["lon"],
                mode="markers",
                marker=dict(
                    size=q_data["avg_signal"].abs() * 0.25 + 8,
                    color=color, opacity=0.9
                ),
                text=q_data.apply(lambda r: f"<b>{r['zone_label']}</b><br>━━━━━━━━━━━<br>SIG: {r['avg_signal']:.0f}% | BW: {r['avg_mbps']:.1f} Mbps<br>LAT: {r['avg_latency']:.0f}ms | LOSS: {r['avg_loss']:.1f}%<br>STATUS: {r['teams_quality'].upper()}", axis=1),
                hoverinfo="text", name=q.upper(), showlegend=True
            ))
    
    # Highlight current zone with pulsing red marker
    if not current_zone_data.empty:
        cz = current_zone_data.iloc[0]
        fig.add_trace(go.Scattermapbox(
            lat=[cz["lat"]], lon=[cz["lon"]],
            mode="markers",
            marker=dict(size=30, color="rgba(0,255,255,0.3)"),
            hoverinfo="none", showlegend=False
        ))
        fig.add_trace(go.Scattermapbox(
            lat=[cz["lat"]], lon=[cz["lon"]],
            mode="markers",
            marker=dict(size=14, color="#00ffff", opacity=0.9),
            text=f">>> YOU ARE HERE: {cz['zone_label']} <<<", hoverinfo="text", name="Your Location", showlegend=False
        ))
    
    # Highlight AI-recommended zone with cyan star
    if st.session_state.recommended_zone:
        rec_data = zone_agg[zone_agg["zone_id"] == st.session_state.recommended_zone]
        if not rec_data.empty:
            rz = rec_data.iloc[0]
            fig.add_trace(go.Scattermapbox(
                lat=[rz["lat"]], lon=[rz["lon"]],
                mode="markers",
                marker=dict(size=35, color="rgba(0,255,255,0.2)"),
                hoverinfo="none", showlegend=False
            ))
            fig.add_trace(go.Scattermapbox(
                lat=[rz["lat"]], lon=[rz["lon"]],
                mode="markers",
                marker=dict(size=20, symbol="circle", color="#00ffff", opacity=0.8),
                text=f">>> RECOMMENDED: {rz['zone_label']}<br>SIG: {rz['avg_signal']:.0f}% | BW: {rz['avg_mbps']:.1f} Mbps <<<", hoverinfo="text", name="Recommended", showlegend=False
            ))
            # Draw line from current to recommended
            if not current_zone_data.empty:
                fig.add_trace(go.Scattermapbox(
                    lat=[current_zone_data.iloc[0]["lat"], rz["lat"]],
                    lon=[current_zone_data.iloc[0]["lon"], rz["lon"]],
                    mode="lines", line=dict(width=2, color="#00ffff"),
                    hoverinfo="none", showlegend=False
                ))
    
    fig.update_layout(
        mapbox=dict(style="open-street-map", zoom=15, center=dict(lat=37.2284, lon=-80.4235)),
        height=520, margin=dict(l=0, r=0, t=0, b=0), showlegend=True,
        legend=dict(font=dict(color="#00ffff", family="Courier New"), bgcolor="rgba(10,14,20,0.8)"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        uirevision="fixed"
    )
    st.plotly_chart(fig, use_container_width=True, on_select="rerun")
    
    # Selected zone from map click
    selected_points = st.session_state.get("plotly_selected_points", None)
    if selected_points and len(selected_points) > 0:
        clicked_zone = zone_agg.iloc[selected_points[0]["point_index"]]
        st.session_state.current_zone = clicked_zone["zone_id"]
    
    # Quick stats below map
    stcol1, stcol2, stcol3, stcol4 = st.columns(4)
    dead_count = len(zone_agg[zone_agg["teams_quality"] == "poor"])
    good_count = len(zone_agg[zone_agg["teams_quality"] == "good"])
    mod_count = len(zone_agg[zone_agg["teams_quality"] == "moderate"])
    total_zones = len(zone_agg)
    
    with stcol1:
        st.metric("Total Zones", total_zones)
    with stcol2:
        st.metric("✅ Good", good_count)
    with stcol3:
        st.metric("⚠️ Moderate", mod_count)
    with stcol4:
        st.metric("❌ Dead Zones", dead_count)
    
    # Live alert bar
    if current_quality == "poor":
        st.markdown(f'<div class="alert-poor">🚨 ALERT: You are in a dead zone! {current_signal:.0f}% signal, {current_mbps:.1f} Mbps. Ask the agent 👉 to find a better spot, or tap "Find Better Zone" below.</div>', unsafe_allow_html=True)
        if st.button("🏃 Find Better Zone", type="primary", use_container_width=True):
            good_zones = zone_agg[(zone_agg["teams_quality"] == "good") & (zone_agg["zone_id"] != st.session_state.current_zone)]
            if not good_zones.empty:
                best = good_zones.nlargest(1, "avg_signal").iloc[0]
                st.session_state.recommended_zone = best["zone_id"]
                st.session_state.chat_history.append({"role": "user", "content": "I'm in a dead zone, find me a better spot!"})
                st.session_state.chat_history.append({"role": "assistant", "content": f"📍 Move to **{best['zone_label']}** — Signal {best['avg_signal']:.0f}%, {best['avg_mbps']:.1f} Mbps, {best['avg_latency']:.0f}ms latency. It's highlighted on your map!"})
                st.rerun()
    elif current_quality == "moderate":
        st.markdown(f'<div class="alert-moderate">⚠️ Connection is moderate. Video calls may lag. Consider moving to a better zone.</div>', unsafe_allow_html=True)
    elif current_quality == "good":
        st.markdown(f'<div class="alert-good">✅ You are in a good zone! Great for Teams/Zoom calls.</div>', unsafe_allow_html=True)

# --- AI AGENT (RIGHT) ---
with chat_col:
    st.markdown("### 🤖 WiFi Agent")
    
    # Chat history display
    chat_container = st.container(height=400)
    with chat_container:
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
    
    # Chat input
    user_input = st.chat_input("Ask about WiFi, dead zones, best spots...")
    
    if user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        
        with st.spinner("Analyzing..."):
            try:
                # Build context from current zone data
                zones_str = " | ".join([
                    f"{row['zone_label']}: signal={row['avg_signal']:.0f}%, bw={row['avg_mbps']:.1f}Mbps, latency={row['avg_latency']:.0f}ms, quality={row['teams_quality']}"
                    for _, row in zone_agg.iterrows()
                ])
                current_label = zone_labels.get(st.session_state.current_zone, st.session_state.current_zone)
                prompt = f"You are a VT campus WiFi assistant. Student is at '{current_label}' (quality: {current_quality}, signal: {current_signal:.0f}%, bandwidth: {current_mbps:.1f}Mbps). Question: '{user_input}' Answer in 2-3 sentences. Zone data at hour {hour_val}: {zones_str}"
                
                ai_result = run_query(f"SELECT ai_gen('{prompt.replace(chr(39), chr(39)+chr(39))}') as response")
                response = ai_result.iloc[0]["response"]
                
                # Check if agent recommended a zone (simple keyword match)
                for _, row in zone_agg.iterrows():
                    if row["zone_label"].lower() in response.lower() and row["teams_quality"] == "good":
                        st.session_state.recommended_zone = row["zone_id"]
                        break
                
                st.session_state.chat_history.append({"role": "assistant", "content": response})
                st.rerun()
            except Exception as e:
                st.session_state.chat_history.append({"role": "assistant", "content": f"Sorry, I couldn't process that: {e}"})
                st.rerun()
    
    # Quick action buttons
    st.markdown("**Quick actions:**")
    qcol1, qcol2 = st.columns(2)
    with qcol1:
        if st.button("📊 Dead zones", use_container_width=True):
            dead = zone_agg[zone_agg["teams_quality"] == "poor"]
            dead_list = ", ".join([f"{r['zone_label']} ({r['avg_signal']:.0f}%)" for _, r in dead.iterrows()])
            st.session_state.chat_history.append({"role": "user", "content": "What are the dead zones right now?"})
            st.session_state.chat_history.append({"role": "assistant", "content": f"🚨 Dead zones at {hour_val}:00: {dead_list}"})
            st.rerun()
    with qcol2:
        if st.button("✅ Best zones", use_container_width=True):
            good = zone_agg[zone_agg["teams_quality"] == "good"].nlargest(3, "avg_signal")
            good_list = "\n".join([f"• **{r['zone_label']}** — {r['avg_signal']:.0f}% signal, {r['avg_mbps']:.1f} Mbps" for _, r in good.iterrows()])
            st.session_state.chat_history.append({"role": "user", "content": "Where are the best zones right now?"})
            st.session_state.chat_history.append({"role": "assistant", "content": f"✅ Best zones at {hour_val}:00:\n{good_list}"})
            st.rerun()
    
    # Current zone detail card
    if not current_zone_data.empty:
        cz = current_zone_data.iloc[0]
        st.markdown(f"""
        <div class="zone-card" style="border-left-color: {'#ff4444' if current_quality=='poor' else '#ffaa00' if current_quality=='moderate' else '#00aa44'}">
            <strong>📍 {cz['zone_label']}</strong><br>
            <span style="color:#888">Signal:</span> <span style="color:#fff">{cz['avg_signal']:.0f}%</span> &nbsp;
            <span style="color:#888">BW:</span> <span style="color:#fff">{cz['avg_mbps']:.1f} Mbps</span> &nbsp;
            <span style="color:#888">Latency:</span> <span style="color:#fff">{cz['avg_latency']:.0f}ms</span><br>
            <span style="color:#888">Jitter:</span> <span style="color:#fff">{cz['avg_jitter']:.1f}ms</span> &nbsp;
            <span style="color:#888">Loss:</span> <span style="color:#fff">{cz['avg_loss']:.1f}%</span> &nbsp;
            <span style="color:#888">Quality:</span> <span style="color:{'#ff4444' if current_quality=='poor' else '#ffaa00' if current_quality=='moderate' else '#00aa44'}">{current_quality.upper()}</span>
        </div>
        """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("<p style='text-align:center;color:#555'>Built for Virginia Tech students 🐾 | Data from eduroam network monitoring | <a href='https://vt-wifi-heatmap-7474651229870249.aws.databricksapps.com' style='color:#666'>VT WiFi Live</a></p>", unsafe_allow_html=True)

st.stop()

("📡 VT Campus WiFi Heatmap")


# Sidebar
st.sidebar.markdown("### 🎛️ Controls")
selected_building = st.sidebar.selectbox("Building", df["building"].unique() if "building" in df.columns else ["All"])



tab1, tab2, tab3, tab4, tab5 = st.tabs(["🗺️ Campus Map", "📈 Indoor Heatmap", "✅ Teams Call Checker", "📊 Zone Details", "🤖 AI Agent"])

with tab1:
    st.subheader("VT Campus WiFi Dead Zone Map")
    st.markdown("Red = poor, Yellow = moderate, Green = good. Hover any point for details.")
    
    map_metric = st.selectbox("Color metric", ["avg_signal", "avg_mbps", "avg_latency"],
        format_func=lambda x: {"avg_signal": "Signal %", "avg_mbps": "Bandwidth Mbps", "avg_latency": "Latency ms"}.get(x, x))
    map_hour = st.slider("Hour of day", 7, 23, 14, key="outdoor_hour")
    
    outdoor_hour_df = outdoor_df[outdoor_df["hour_of_day"] == map_hour].copy()
    
    if not outdoor_hour_df.empty:
        zone_agg = outdoor_hour_df.groupby(["zone_id", "zone_label", "lat", "lon"]).agg({
            "avg_signal": "mean", "avg_mbps": "mean", "avg_latency": "mean",
            "avg_jitter": "mean", "avg_loss": "mean", "teams_quality": "first"
        }).reset_index()
        
        color_scale = "RdYlGn" if map_metric in ["avg_signal", "avg_mbps"] else "RdYlGn_r"
        
        fig_map = px.scatter_mapbox(
            zone_agg, lat="lat", lon="lon", color=map_metric,
            size=map_metric, size_max=25, zoom=15, center={"lat": 37.2284, "lon": -80.4235},
            mapbox_style="open-street-map", hover_name="zone_label",
            hover_data={"avg_signal": ":.0f", "avg_mbps": ":.1f", "avg_latency": ":.0f",
                "avg_jitter": ":.1f", "avg_loss": ":.1f", "teams_quality": True, "lat": False, "lon": False},
            color_continuous_scale=color_scale,
            range_color=(zone_agg[map_metric].min(), zone_agg[map_metric].max()),
            title=f"Campus WiFi at {map_hour}:00"
        )
        fig_map.update_layout(height=600)
        st.plotly_chart(fig_map, use_container_width=True)
        
        dead_zones = zone_agg[zone_agg["teams_quality"] == "poor"]
        if not dead_zones.empty:
            st.error(f"{len(dead_zones)} dead zones at {map_hour}:00 - avoid for video calls!")
            for _, dz in dead_zones.iterrows():
                st.markdown(f"  ❌ **{dz['zone_label']}** - Signal {dz['avg_signal']:.0f}%, {dz['avg_mbps']:.1f} Mbps, {dz['avg_latency']:.0f}ms")
        
        good_zones = zone_agg[zone_agg["teams_quality"] == "good"]
        if not good_zones.empty:
            st.success(f"{len(good_zones)} zones with good connectivity at {map_hour}:00")
    else:
        st.warning("No outdoor data for this hour.")
    
    st.markdown("### All Outdoor Zones")
    all_summary = outdoor_df.groupby(["zone_id", "zone_label", "lat", "lon"]).agg({
        "avg_signal": "mean", "avg_mbps": "mean", "avg_latency": "mean",
        "teams_quality": lambda x: x.mode().iloc[0] if not x.empty else "unknown"
    }).reset_index().sort_values("avg_signal")
    st.dataframe(all_summary.rename(columns={
        "zone_label": "Location", "avg_signal": "Signal %", "avg_mbps": "Mbps",
        "avg_latency": "Latency ms", "teams_quality": "Quality"
    })[["Location", "Signal %", "Mbps", "Latency ms", "Quality"]], use_container_width=True, hide_index=True)

# ============================================================
# TAB 2: Indoor Heatmap
# ============================================================
with tab2:
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
# TAB 3: Teams Call Checker
# ============================================================
with tab3:
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
# TAB 4: Zone Details
# ============================================================
with tab4:
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

# ============================================================
# TAB 5: AI Agent
# ============================================================
with tab5:
    st.subheader("🤖 WiFi Connectivity Assistant")
    st.markdown("Ask me anything about campus WiFi! I can recommend the best zones for video calls, identify dead zones, and more.")
    
    # Suggested questions
    st.markdown("**Try asking:**")
    st.markdown("- Where's the best WiFi near Goodwin Hall?")
    st.markdown("- Can I take a Teams call at the Drillfield?")
    st.markdown("- Which zones are dead zones right now?")
    st.markdown("- What floor of Goodwin has the best connectivity?")
    
    user_question = st.chat_input("Ask about campus WiFi...")
    
    if user_question:
        with st.spinner("Analyzing connectivity data..."):
            try:
                # Get current hour zone data as context
                import datetime
                current_hour = datetime.datetime.now().hour
                if current_hour < 7 or current_hour > 23:
                    current_hour = 14
                
                # Fetch zone summary data
                zone_data = run_query(f"""
                    SELECT zone_label, ROUND(AVG(signal_pct),0) as signal, 
                           ROUND(AVG(bandwidth_mbps),1) as mbps,
                           ROUND(AVG(latency_ms),0) as latency,
                           ROUND(AVG(packet_loss_pct),1) as loss,
                           teams_quality
                    FROM vt_connectivity.campus.outdoor_measurements
                    WHERE hour_of_day = {current_hour}
                    GROUP BY zone_label, teams_quality
                    ORDER BY signal DESC
                """)
                
                # Format zone data as context string
                zones_str = " | ".join([
                    f"{row['zone_label']}: signal={row['signal']:.0f}%, bandwidth={row['mbps']:.1f}Mbps, latency={row['latency']:.0f}ms, loss={row['loss']:.1f}%, quality={row['teams_quality']}"
                    for _, row in zone_data.iterrows()
                ])
                
                # Also get indoor zone data
                indoor_data = run_query(f"""
                    SELECT zone_id, floor, hour_of_day,
                           ROUND(avg_signal_pct,0) as signal,
                           ROUND(avg_tcp_down_mbps,1) as mbps,
                           ROUND(avg_inet_latency_ms,0) as latency,
                           teams_quality
                    FROM vt_connectivity.campus.gold_zone_summary
                    WHERE hour_of_day = {current_hour}
                    ORDER BY avg_signal_pct DESC
                """)
                
                indoor_str = " | ".join([
                    f"{row['zone_id']} (floor {row['floor']}): signal={row['signal']:.0f}%, bandwidth={row['mbps']:.1f}Mbps, latency={row['latency']:.0f}ms, quality={row['teams_quality']}"
                    for _, row in indoor_data.iterrows()
                ])
                
                # Call ai_gen with context
                prompt = f"You are a WiFi connectivity assistant for Virginia Tech campus. A student asks: '{user_question}' Answer concisely in 2-3 sentences. Current hour is {current_hour}:00. Outdoor zones: {zones_str}. Indoor zones (Torgersen Hall): {indoor_str}."
                
                # Use ai_gen SQL function
                ai_result = run_query(f"""
                    SELECT ai_gen('{prompt.replace("'", "''")}') as response
                """)
                
                response = ai_result.iloc[0]['response']
                
                # Display chat
                st.chat_message("user").markdown(user_question)
                st.chat_message("assistant").markdown(response)
                
                # Show supporting data
                with st.expander("📊 View supporting data"):
                    st.markdown(f"**Outdoor zones at {current_hour}:00**")
                    st.dataframe(zone_data.rename(columns={
                        'zone_label': 'Zone', 'signal': 'Signal %', 'mbps': 'Mbps',
                        'latency': 'Latency ms', 'loss': 'Loss %', 'teams_quality': 'Quality'
                    }), use_container_width=True, hide_index=True)
                    st.markdown(f"**Indoor zones at {current_hour}:00**")
                    st.dataframe(indoor_data.rename(columns={
                        'zone_id': 'Zone', 'floor': 'Floor', 'signal': 'Signal %',
                        'mbps': 'Mbps', 'latency': 'Latency ms', 'teams_quality': 'Quality'
                    }), use_container_width=True, hide_index=True)
                    
            except Exception as e:
                st.error(f"Agent error: {e}")
                st.info("The AI agent uses the ai_gen SQL function. Make sure it's available in your workspace.")

st.sidebar.markdown("---")
st.sidebar.markdown("Built for Virginia Tech students 🐾")
st.sidebar.markdown("Data from eduroam network monitoring")