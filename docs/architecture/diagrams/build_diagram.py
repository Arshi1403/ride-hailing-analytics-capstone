"""Draw the architecture diagram to PNG and PDF (no cloud access). Run: python docs/architecture/diagrams/build_diagram.py
Needs matplotlib (pip install matplotlib). The Mermaid source architecture.mmd is the editable text version."""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

OUT = Path(__file__).resolve().parent
COLORS = {"src": "#e8f0fe", "ing": "#fff4e5", "raw": "#fdecea", "proc": "#e6f4ea", "wh": "#f3e8fd", "bi": "#e0f7fa", "orch": "#fffde7"}

fig, ax = plt.subplots(figsize=(17, 9.5))
ax.set_xlim(0, 170)
ax.set_ylim(0, 95)
ax.axis("off")
fig.suptitle("Ride-Hailing Analytics Platform: architecture (project labs-507309, region asia-south1)", fontsize=15, fontweight="bold")


def box(x, y, w, h, text, kind, size=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.5", fc=COLORS[kind], ec="#444", lw=1.2))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=size)
    return (x, y, w, h)


def arrow(a, b, label="", style="-|>", color="#333", ls="-", rad=0.0):
    ax_, ay, aw, ah = a
    bx, by, bw, bh = b
    if bx >= ax_ + aw - 1:
        start, end = (ax_ + aw, ay + ah / 2), (bx, by + bh / 2)
    elif by >= ay + ah - 1:
        start, end = (ax_ + aw / 2, ay + ah), (bx + bw / 2, by)
    else:
        start, end = (ax_ + aw / 2, ay), (bx + bw / 2, by + bh)
    ax.add_patch(FancyArrowPatch(start, end, arrowstyle=style, mutation_scale=14, color=color, lw=1.4, ls=ls,
                                 connectionstyle=f"arc3,rad={rad}"))
    if label:
        ax.text((start[0] + end[0]) / 2, (start[1] + end[1]) / 2 + 1.5, label, fontsize=8, ha="center", color=color)


producer = box(3, 62, 22, 10, "Python producer\n(1000 mock rides / 10 min)\nproducer/", "src")
history = box(3, 38, 22, 10, "Historical CSV\nNYC Taxi 2018 subset\nGCS landing bucket", "src")
pubsub = box(33, 62, 20, 10, "Pub/Sub\ntopic rides-events\nsub rides-events-sub", "ing")
dataflow = box(60, 62, 22, 10, "Dataflow\nGoogle PubSub-to-BigQuery\ntemplate + JS UDF (ingest_ts)", "ing", 8)
raw_stream = box(90, 62, 24, 10, "BigQuery raw_ds.rides_stream\n(+ rides_stream_error_records)", "raw", 8)
raw_hist = box(90, 38, 24, 10, "BigQuery raw_ds.rides_history\n(Composer GCS load + MERGE)", "raw", 8)
spark = box(90, 14, 24, 12, "Dataproc Serverless\nPySpark batch\nclean, dedupe, enrich,\nderive, SCD2", "proc", 8)
parquet = box(122, 14, 22, 12, "GCS curated bucket\nrides/pickup_date=.../\nrejected/, dim_driver_scd2/", "proc", 8)
stage = box(122, 38, 22, 10, "BigQuery stg_ds\n(Parquet load, per run)", "wh", 8)
curated = box(122, 62, 22, 14, "BigQuery curated_ds\nfact_rides (partition\npickup_date, cluster\nzone, driver)\ndim_driver_scd2\nrejected_rides", "wh", 8)
dq = box(148, 50, 18, 10, "DQ gate\nops_ds.dq_results", "wh", 8)
marts = box(148, 66, 18, 12, "mart_ds views\nv_daily_revenue\nv_zone_demand_hourly\nv_driver_leaderboard", "wh", 8)
bqml = box(148, 82, 18, 8, "BQML ARIMA_PLUS\nml_ds forecast 168 h", "wh", 8)
looker = box(148, 22, 18, 10, "Looker Studio\ndashboard", "bi")
cost = box(148, 6, 18, 10, "Cost report\nINFORMATION_SCHEMA\n.JOBS_BY_PROJECT", "bi", 8)
composer = box(33, 80, 100, 9, "Cloud Composer: DAG ride_hailing_daily (sense, load, spark, merge, DQ, marts, weekly BQML, notify; retries, SLA 4 h, Secret Manager)", "orch", 9)

arrow(producer, pubsub)
arrow(pubsub, dataflow)
arrow(dataflow, raw_stream)
arrow(history, raw_hist, "Composer load")
arrow(raw_stream, spark, rad=-0.2)
arrow(raw_hist, spark)
arrow(spark, parquet)
arrow(parquet, stage, rad=0.0)
arrow(stage, curated, "MERGE")
arrow(curated, dq)
arrow(dq, marts, "pass")
arrow(marts, bqml, "weekly")
arrow(marts, looker, rad=-0.35)
arrow(bqml, looker, rad=-0.45)
arrow(cost, looker, ls="--", color="#888")
for target in (history, spark, curated, dq):
    ax.add_patch(FancyArrowPatch((83, 80), (target[0] + target[2] / 2, target[1] + target[3]), arrowstyle="-|>", mutation_scale=9,
                                 color="#b59b00", lw=0.8, ls=":", connectionstyle="arc3,rad=0.0"))
ax.text(3, 4, "Dotted lines: orchestrated by Cloud Composer. Solid lines: data flow. Looker Studio reads mart_ds only.", fontsize=9, color="#555")

fig.savefig(OUT / "architecture.png", dpi=170, bbox_inches="tight")
fig.savefig(OUT / "architecture.pdf", bbox_inches="tight")
print("wrote", OUT / "architecture.png", "and", OUT / "architecture.pdf")
