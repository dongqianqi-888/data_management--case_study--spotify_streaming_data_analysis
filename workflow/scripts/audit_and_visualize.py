# =====================================================================
# audit_and_visualize.py
# Spark reconciliation audit + Python/Matplotlib/Seaborn plotting
# Stray Kids Spotify Streaming Analytics
# Run as: spark-submit audit_and_visualize.py  (PySpark, inside Oozie spark-action)
# Reads:  skz_spotify_db.fact_streams_detail, dim_time_counts, dim_albums
# Writes: 8 chart PNGs to /tmp/spotify_automatic_charts/
# =====================================================================

# --- Part 1: Cross-layer reconciliation audit ---
# Synchronize Spark session state to the target data warehouse database
spark.sql("USE skz_spotify_db")

# Perform an audit query by cross-joining fact aggregates against dimensional snapshots
audit_df = spark.sql("""
    SELECT f.year, f.month,
        ROUND(SUM(f.played_ms) / 60000.0, 2) as detail_calculated_minutes,
        ROUND(t.snapshot_minutes, 2) as snapshot_recorded_minutes
    FROM skz_spotify_db.fact_streams_detail f
    JOIN (
        SELECT end_year, end_month, SUM(total_time_played_ms) / 60000.0 as snapshot_minutes
        FROM skz_spotify_db.dim_time_counts GROUP BY end_year, end_month
    ) t ON f.year = t.end_year AND f.month = t.end_month
    GROUP BY f.year, f.month, t.snapshot_minutes ORDER BY f.year, f.month
""")

# Output the alignment report to ensure there is zero data drop during ingestion
print("=== SPARK CROSS-LAYER COMPLIANCE & RECONCILIATION REPORT ===")
audit_df.show()

# --- Part 2: 8-chart EDA visualization ---
import sys
import os
import shutil
import base64
import time

# 修正 Python2.7 沙箱环境下的 site-packages 路径
user_site_package = os.path.expanduser('~/.local/lib/python2.7/site-packages')
if user_site_package not in sys.path:
    sys.path.insert(0, user_site_package)

import matplotlib
matplotlib.use('Agg')  # 强制使用无 GUI 渲染模式，防止沙箱抛出 X11 错误
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 初始化并重置图表本地缓存路径
CHART_DIR = "/tmp/spotify_automatic_charts"
if os.path.exists(CHART_DIR):
    shutil.rmtree(CHART_DIR)
os.makedirs(CHART_DIR)

# 配置全局样式
sns.set(style="whitegrid")
plt.rcParams.update({'font.sans-serif': ['DejaVu Sans', 'Arial'], 'axes.unicode_minus': False})
SPOTIFY_GREEN = "#1DB954"
DARK_FIELD    = "#191414"
GREEN_PALETTE = ["#FF6B6B", "#FF9F43", "#FECA57", "#1DD1A1", "#10AC84", "#00D2D3", "#54A0FF", "#5F27CD", "#341F97", "#FF9FF3"]

# 封装标准的 Base64 HTML 渲染函数，确保在 Zeppelin 中完美输出
def show_saved_chart(img_path, max_width="90%"):
    with open(img_path, "rb") as f:
        encoded_string = base64.b64encode(f.read())
    print("%html")
    print("<div style='text-align:center; padding:15px;'>")
    print("  <img src='data:image/png;base64,{}' style='max-width:{}; border-radius:6px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);' />".format(encoded_string, max_width))
    print("</div>")
    time.sleep(0.2) # 给数据缓冲区留出写出时间

# ==========================================
# Q1: Annual Song Release Volume & Style
# ==========================================
q1_df = spark.sql("SELECT release_year, album_type, COUNT(DISTINCT track_name) as song_count FROM skz_spotify_db.dim_albums WHERE release_year IS NOT NULL GROUP BY release_year, album_type ORDER BY release_year").toPandas()
plt.figure(figsize=(10, 5))
sns.barplot(data=q1_df, x="release_year", y="song_count", hue="album_type", palette=GREEN_PALETTE)
plt.title("Annual Song Release Volume & Track Style Evolution Trends", fontsize=12, fontweight='bold', color=DARK_FIELD)
plt.xlabel("Release Year"), plt.ylabel("Number of Unique Songs"), plt.legend(title="Album Type Structure")
plt.tight_layout()
p1 = "{}/q1_annual_style_evolution.png".format(CHART_DIR)
plt.savefig(p1, dpi=300)
plt.close()
show_saved_chart(p1)

# ==========================================
# Q2: Weekly Consumer Audio BPM Fluctuations
# ==========================================
q2_df = spark.sql("SELECT end_day, AVG(bpm) as avg_bpm FROM skz_spotify_db.fact_streams_detail GROUP BY end_day").toPandas()
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
q2_df['end_day'] = pd.Categorical(q2_df['end_day'], categories=day_order, ordered=True)
q2_df = q2_df.sort_values("end_day")
plt.figure(figsize=(9, 4.5))
plt.plot(q2_df['end_day'], q2_df['avg_bpm'], marker='o', color=SPOTIFY_GREEN, linewidth=3, markersize=8)
plt.title("Weekly Consumer Audio Beats Per Minute (BPM) Kinetic Fluctuations", fontsize=12, fontweight='bold')
plt.xlabel("Day of the Week"), plt.ylabel("Average Dynamic BPM Metric")
plt.tight_layout()
p2 = "{}/q2_weekly_bpm_fluctuations.png".format(CHART_DIR)
plt.savefig(p2, dpi=300)
plt.close()
show_saved_chart(p2)

# ==========================================
# Q3: Global Footprint of Album Countries (🔥修复了原文档读错变量的Bug)
# ==========================================
q3_df = spark.sql("SELECT release_country, COUNT(DISTINCT album_name) as cnt FROM skz_spotify_db.fact_streams_detail WHERE release_country IS NOT NULL AND release_country != '' GROUP BY release_country").toPandas()
plt.figure(figsize=(6, 6))
plt.pie(q3_df['cnt'], labels=q3_df['release_country'], colors=GREEN_PALETTE, autopct='%1.1f%%', startangle=140)
plt.title("Global Structural Footprint of Album Country Releases", fontsize=12, fontweight='bold')
plt.tight_layout()
p3 = "{}/q3_release_country_distribution.png".format(CHART_DIR)
plt.savefig(p3, dpi=300)
plt.close()
show_saved_chart(p3, max_width="65%") # 饼图稍微收紧宽度，更显精致

# ==========================================
# Q4: Textual Density Analytics (🔥修复了原文档缺少 output_path 的Bug)
# ==========================================
q4_df = spark.sql("SELECT track_name, COUNT(1) as freq FROM skz_spotify_db.fact_streams_detail GROUP BY track_name ORDER BY freq DESC LIMIT 15").toPandas()
plt.figure(figsize=(10, 5))
sns.barplot(data=q4_df, x="freq", y="track_name", color="#1ed760")
plt.title("Textual Density Analytics: Track Title Vector Frequency Metrics", fontsize=12, fontweight='bold')
plt.xlabel("Streaming Occurrence Log Counts"), plt.ylabel("Track Title")
plt.tight_layout()
p4 = "{}/q4_track_name_density_matrix.png".format(CHART_DIR)
plt.savefig(p4, dpi=300)
plt.close()
show_saved_chart(p4)

# ==========================================
# Q5: Top Ingestion Hits Analysis (🔥修复了未展示输出的Bug)
# ==========================================
q5_df = spark.sql("SELECT track_name, number_of_streams, avg_percent_song_played FROM skz_spotify_db.dim_albums ORDER BY number_of_streams DESC LIMIT 8").toPandas()
fig, ax1 = plt.subplots(figsize=(11, 5))
sns.barplot(data=q5_df, x="number_of_streams", y="track_name", ax=ax1, color=SPOTIFY_GREEN, alpha=0.8)
ax2 = ax1.twinx()
ax2.plot(q5_df['avg_percent_song_played'], q5_df['track_name'], color=DARK_FIELD, marker='s', linewidth=2, label="Completion Rate")
ax1.set_title("Top Ingestion Hits Analysis: Stream Volumes vs User Retention Percentage", fontsize=12, fontweight='bold')
ax1.set_xlabel("Total Stream Counts"), ax2.set_ylabel("Average Song Completion Rate (%)")
plt.tight_layout()
p5 = "{}/q5_top_hits_characteristics.png".format(CHART_DIR)
plt.savefig(p5, dpi=300)
plt.close()
show_saved_chart(p5)

# ==========================================
# Q6: Macroscopic Consumption Capacity (🔥修复了未展示输出的Bug)
# ==========================================
q6_df = spark.sql("SELECT album_type, SUM(number_of_streams) as total_streams FROM skz_spotify_db.dim_albums GROUP BY album_type ORDER BY total_streams DESC").toPandas()
plt.figure(figsize=(8, 4.5))
sns.barplot(data=q6_df, x="album_type", y="total_streams", palette="Greens_r")
plt.title("Macroscopic Consumption Capacity Sorted by Album Category Entity", fontsize=12, fontweight='bold')
plt.xlabel("Album Type Category"), plt.ylabel("Aggregated Stream Volume Scale")
plt.tight_layout()
p6 = "{}/q6_album_type_dominance.png".format(CHART_DIR)
plt.savefig(p6, dpi=300)
plt.close()
show_saved_chart(p6)

# ==========================================
# Q7: User Listening Micro-Retention (🔥修复了未展示输出的Bug)
# ==========================================
q7_df = spark.sql("""
    SELECT 
        CASE WHEN played_ms < 10000 THEN '0-10s (Bounce)' 
             WHEN played_ms < 30000 THEN '10-30s (Intro Filter)'
             WHEN played_ms < 120000 THEN '30s-2m (Dropout)' 
             ELSE '2m+ (Conversion)' END as duration_zone, 
        COUNT(1) as log_cnt
    FROM skz_spotify_db.fact_streams_detail GROUP BY 1
""").toPandas()
plt.figure(figsize=(8, 4.5))
sns.barplot(data=q7_df, x="log_cnt", y="duration_zone", color=SPOTIFY_GREEN)
plt.title("User Listening Micro-Retention Duration Breakdown Curve Map", fontsize=12, fontweight='bold')
plt.xlabel("Log Interaction Frequencies"), plt.ylabel("Retention Temporal Range")
plt.tight_layout()
p7 = "{}/q7_user_retention_duration.png".format(CHART_DIR)
plt.savefig(p7, dpi=300)
plt.close()
show_saved_chart(p7)

# ==========================================
# Q8: Post-Release Velocity Decay Curve (🔥修复了未展示输出的Bug)
# ==========================================
q8_df = spark.sql("SELECT month, COUNT(1) as stream_velocity FROM skz_spotify_db.fact_streams_detail GROUP BY month ORDER BY month").toPandas()
plt.figure(figsize=(9, 4.5))
plt.fill_between(q8_df['month'], q8_df['stream_velocity'], color="#1ed760", alpha=0.3)
plt.plot(q8_df['month'], q8_df['stream_velocity'], color=SPOTIFY_GREEN, linewidth=3)
plt.title("Post-Release Velocity Decay Curve Model & Active Lifespan Horizon", fontsize=12, fontweight='bold')
plt.xlabel("Observation Timeline Months"), plt.ylabel("Active Ingestion Velocity Log Volumetrics")
plt.tight_layout()
p8 = "{}/q8_post_release_decay_model.png".format(CHART_DIR)
plt.savefig(p8, dpi=300)
plt.close()
show_saved_chart(p8)
