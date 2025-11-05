# streamlit_app.py
# HR Co-Pilot – 6 High-Impact Agents (Individual + Team Attrition)
# Airbus Light Theme (white background + Airbus blue accents)
# Built for Airbus HR Pod on GCP (Vertex AI + BigQuery + Looker)
# © 2025 Doanh Pham

import streamlit as st
import pandas as pd
import random

# ─────────────────── PAGE CONFIG ───────────────────
st.set_page_config(page_title="HR Co-Pilot – 6 Agents", layout="wide")

# ─────────────────── CUSTOM CSS (Airbus Light Theme) ───────────────────
st.markdown("""
    <style>
    /* GLOBAL LAYOUT */
    body, .stApp {
        background-color: #ffffff;
        color: #0a2342;
        font-family: "Helvetica Neue", sans-serif;
    }

    /* TITLES */
    h1, h2, h3 {
        color: #0a2342 !important;
        font-weight: 600;
    }

    /* TABS */
    .stTabs [data-baseweb="tab-list"] {
        background-color: #f5f7fa;
        border-radius: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        color: #0a2342 !important;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        border-bottom: 3px solid #005bbb !important;
        font-weight: 600;
    }

    /* METRIC CARDS */
    [data-testid="stMetricValue"] {
        color: #005bbb !important;
        font-weight: 700 !important;
    }
    [data-testid="stMetricDelta"] {
        color: #0078d7 !important;
    }

    /* BUTTONS */
    div.stButton > button {
        background-color: #005bbb;
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: 600;
        padding: 0.5em 1em;
        transition: 0.2s;
    }
    div.stButton > button:hover {
        background-color: #003f87;
        transform: scale(1.03);
    }

    /* INPUTS */
    textarea, input[type="text"], select {
        background-color: #ffffff;
        color: #0a2342;
        border-radius: 6px;
        border: 1px solid #ccd6e0;
        padding: 0.4em;
    }

    /* ALERT COLORS */
    .stAlert {
        border-radius: 8px;
        padding: 1em;
        font-weight: 500;
    }
    .stAlert[data-baseweb="alert"][class*="success"] {
        background-color: #e6f2ff !important;
        color: #003f87 !important;
        border-left: 5px solid #005bbb;
    }
    .stAlert[data-baseweb="alert"][class*="warning"] {
        background-color: #fff5e6 !important;
        color: #6e4b00 !important;
        border-left: 5px solid #ffb100;
    }
    .stAlert[data-baseweb="alert"][class*="error"] {
        background-color: #fde8e8 !important;
        color: #8b0000 !important;
        border-left: 5px solid #d32f2f;
    }

    /* FOOTER */
    footer, .stCaption, .css-1lsmgbg {
        color: #4b6382;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

# ─────────────────── HEADER ───────────────────
st.title("✈️ HR Co-Pilot – Airbus HR Agents (GCP HR Pod)")
st.caption("Powered by Vertex AI • BigQuery • Looker Studio | Demo by Doanh Pham")

# ─────────────────── MOCK DATA ───────────────────
employees = pd.DataFrame([
    {"id": "E101", "name": "Maya Chen", "dept": "Sales", "tenure": 14, "rating": 4.2, "ot": 28, "risk": 78, "sentiment": 0.42},
    {"id": "E102", "name": "Liam Park", "dept": "Engineering", "tenure": 36, "rating": 4.8, "ot": 12, "risk": 22, "sentiment": 0.81},
    {"id": "E103", "name": "Sofia Patel", "dept": "Finance", "tenure": 8, "rating": 3.1, "ot": 65, "risk": 91, "sentiment": 0.33},
    {"id": "E104", "name": "Alex Smith", "dept": "Engineering", "tenure": 22, "rating": 4.0, "ot": 34, "risk": 40, "sentiment": 0.67},
    {"id": "E105", "name": "Emma Lopez", "dept": "Sales", "tenure": 5, "rating": 3.4, "ot": 50, "risk": 85, "sentiment": 0.45},
])

courses = ["People Analytics 101", "Vertex AI for HR", "Data Storytelling for Leaders"]
policies = {
    "vacation": "Employees are entitled to 20 days of paid vacation annually.",
    "parental": "14 weeks fully paid parental leave + 4 weeks flexible option.",
    "remote": "Hybrid policy: minimum 2 days in office; flexible Fridays.",
    "benefits": "Health, dental, vision, 401k matching, and annual wellness stipend."
}

# ─────────────────── TABS ───────────────────
tabs = st.tabs([
    "1️⃣ Attrition Prediction (Individual)",
    "2️⃣ Attrition Prediction (Team-Level)",
    "3️⃣ Sentiment & Engagement",
    "4️⃣ Time & Workforce Analytics",
    "5️⃣ Training & Development Recommender",
    "6️⃣ HR Policy & Benefits Copilot"
])

# ── 1️⃣ INDIVIDUAL ATTRITION PREDICTION ─────────────────────────
with tabs[0]:
    st.header("Agent 1: Attrition Prediction (Individual Level)")
    st.write("Predict which individual employees may be at higher flight risk based on current indicators.")
    emp = st.selectbox("Select Employee", employees["name"], key="attrition_individual")
    if st.button("Run Individual Prediction", key="attr_btn"):
        row = employees[employees["name"] == emp].iloc[0]
        risk = row["risk"] + random.randint(-5, 5)
        st.metric("Attrition Risk", f"{risk}%", delta=f"{random.choice(['+3%', '-2%', '+6%'])}")
        if risk > 70:
            st.warning(f"⚠️ High risk detected for {emp}. Recommended: career chat, mentor match, or development plan.")
        else:
            st.success(f"{emp} shows stable retention outlook.")
        st.bar_chart({"Performance": row["rating"]*20, "Engagement": row["sentiment"]*100, "Tenure": row["tenure"]})

# ── 2️⃣ TEAM-LEVEL ATTRITION PREDICTION ─────────────────────────
with tabs[1]:
    st.header("Agent 2: Attrition Prediction (Team / Department Level)")
    st.write("Analyze team-level retention risk to support workforce planning — not for individual monitoring.")
    team = st.selectbox("Select Department", employees["dept"].unique(), key="attrition_team")
    if st.button("Run Team-Level Analysis", key="team_btn"):
        team_data = employees[employees["dept"] == team]
        avg_risk = round(team_data["risk"].mean(), 1)
        avg_sentiment = round(team_data["sentiment"].mean()*100, 1)
        avg_rating = round(team_data["rating"].mean(), 1)
        st.metric("Average Attrition Risk", f"{avg_risk}%", delta=f"{random.choice(['+2%', '-3%', '+5%'])}")
        st.metric("Average Sentiment", f"{avg_sentiment}%", delta=f"{random.choice(['+4%', '-2%'])}")
        st.metric("Average Performance Score", f"{avg_rating}/5")
        st.bar_chart(team_data.set_index("name")[["risk", "ot"]])
        if avg_risk > 70:
            st.error(f"⚠️ {team} team showing elevated attrition risk. Recommend deeper engagement review.")
        elif avg_risk > 50:
            st.warning(f"Moderate attrition risk in {team}. Suggest manager discussions and targeted learning.")
        else:
            st.success(f"{team} team retention risk is within normal range.")

# ── 3️⃣ SENTIMENT & ENGAGEMENT ─────────────────────────
with tabs[2]:
    st.header("Agent 3: Sentiment & Engagement Monitor")
    st.write("Use NLP to analyze employee sentiment from survey comments and text feedback.")
    comment = st.text_area("Paste a recent myPulse comment:", "Feeling burnt out lately with extra hours.")
    if st.button("Analyze Sentiment", key="sentiment"):
        score = random.randint(45, 90)
        st.metric("Sentiment Score", f"{score}%", delta=f"{random.choice(['+4%', '-6%'])}")
        if score < 60:
            st.error("Negative sentiment detected → Notify HR Business Partner.")
        elif score < 75:
            st.warning("Neutral tone → Recommend manager follow-up discussion.")
        else:
            st.success("Positive sentiment detected.")
        st.line_chart([random.randint(40, 85) for _ in range(6)])

# ── 4️⃣ TIME & WORKFORCE ANALYTICS ─────────────────────────
with tabs[3]:
    st.header("Agent 4: Time & Workforce Analytics")
    st.write("Monitor overtime, absenteeism, and workload balance to improve wellbeing and safety.")
    emp = st.selectbox("Choose Employee", employees["name"], key="time")
    if st.button("Generate Report", key="time_btn"):
        row = employees[employees["name"] == emp].iloc[0]
        st.metric("Overtime Hours (This Month)", f"{row['ot']} hrs", "↑5h vs last month")
        if row["ot"] > 40:
            st.warning("⚠️ Fatigue risk detected → Recommend auto-rest scheduling.")
        else:
            st.success("Workload within healthy range.")
        st.bar_chart({"Week1": 8, "Week2": 12, "Week3": 10, "Week4": 6})

# ── 5️⃣ TRAINING & DEVELOPMENT RECOMMENDER ─────────────────────────
with tabs[4]:
    st.header("Agent 5: Training & Development Recommender")
    st.write("Recommend personalized learning and mentorship opportunities to support career growth.")
    goal = st.text_input("Career goal / next role:", "People Analytics Lead")
    if st.button("Generate Development Plan", key="train_btn"):
        recs = random.sample(courses, 2)
        st.success(f"Recommended learning for '{goal}'")
        for r in recs:
            st.write(f"📘 {r} – [Enroll]")
        st.write(f"👥 Mentor matched: {random.choice(['Sarah', 'Raj', 'Ana'])}")

# ── 6️⃣ HR POLICY & BENEFITS COPILOT ─────────────────────────
with tabs[5]:
    st.header("Agent 6: HR Policy & Benefits Copilot")
    st.write("Ask HR policy questions and get instant, consistent answers.")
    q = st.text_input("Ask HR Copilot", "How much parental leave do we have?")
    if st.button("Get Answer", key="policy_btn"):
        found = None
        for k, v in policies.items():
            if k in q.lower():
                found = v
                break
        if found:
            st.success(found)
        else:
            st.info("Policy not found – forwarding to HR shared inbox.")
    st.metric("Response Accuracy (Pilot)", "93%")
    st.metric("Tickets Deflected", "88%")

# ─────────────────── FOOTER ───────────────────
st.markdown("---")
st.caption("Demo: 6 HR AI Agents | Airbus HR Pod | Vertex AI + BigQuery | © 2025 Doanh Pham")
