# streamlit_app.py
# HR Navigator – 5 High-Impact Agents
# Airbus Light Theme (white background + Airbus blue accents)
# Built for Airbus HR Pod on GCP (Vertex AI + BigQuery + Looker)
# © 2025 Doanh Pham

import streamlit as st
import pandas as pd
import random
import time

# ─────────────────── PAGE CONFIG ───────────────────
st.set_page_config(page_title="HR Navigator – 5 Agents", layout="wide")

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
st.title("✈️ HR Navigator – 5 AI Agents to Guide Workforce Strategy")
st.caption("Proposal for Airbus HR Pod | Powered by Vertex AI • BigQuery • Looker | © 2025 Doanh Pham")

# ─────────────────── MOCK DATA ───────────────────
employees = pd.DataFrame([
    {"id": "E101", "name": "Maya Chen", "dept": "Sales", "tenure": 14, "rating": 4.2, "ot": 28, "risk": 78, "sentiment": 0.42, "role": "Sales Manager"},
    {"id": "E102", "name": "Liam Park", "dept": "Engineering", "tenure": 36, "rating": 4.8, "ot": 12, "risk": 22, "sentiment": 0.81, "role": "Senior Engineer"},
    {"id": "E103", "name": "Sofia Patel", "dept": "Finance", "tenure": 8, "rating": 3.1, "ot": 65, "risk": 91, "sentiment": 0.33, "role": "Financial Analyst"},
    {"id": "E104", "name": "Alex Smith", "dept": "Engineering", "tenure": 22, "rating": 4.0, "ot": 34, "risk": 40, "sentiment": 0.67, "role": "Data Scientist"},
    {"id": "E105", "name": "Emma Lopez", "dept": "Sales", "tenure": 5, "rating": 3.4, "ot": 50, "risk": 85, "sentiment": 0.45, "role": "Account Executive"},
])

courses = ["People Analytics 101", "Vertex AI for HR", "Data Storytelling for Leaders", "Cloud Architecture Certification", "Advanced Project Management (PMP)"]
policies = {
    "vacation": "Employees are entitled to 20 days of paid vacation annually, accruing monthly.",
    "parental": "We offer 14 weeks of fully paid parental leave for the primary caregiver and 4 weeks for the secondary caregiver.",
    "remote": "Our hybrid policy requires a minimum of 2 days in the office (typically Tues/Wed) for most roles. Flexible Fridays are available.",
    "benefits": "Airbus provides comprehensive Health, Dental, and Vision insurance, 401k matching up to 5%, and an annual wellness stipend of €500."
}

# ─────────────────── TABS ───────────────────
tabs = st.tabs([
    "1️⃣ Attrition Prediction (Team-Level)",
    "2️⃣ Sentiment & Engagement",
    "3️⃣ Time & Workforce Analytics",
    "4️⃣ Training & Development Recommender",
    "5️⃣ HR Policy & Benefits Copilot"
])

# ── 1️⃣ TEAM-LEVEL ATtrition PREDICTION ─────────────────────────
with tabs[0]:
    st.header("Agent 1: Attrition Prediction (Team-Level)")
    st.write(
        """**Purpose:** To identify teams and critical roles at high risk of attrition, allowing HRBPs and managers to deploy targeted, data-driven retention strategies."""
    )
    
    team = st.selectbox("Select Department / Function", employees["dept"].unique(), key="attrition_team")
    
    if st.button("Run Team-Level Analysis", key="team_btn"):
        with st.spinner(f"Analyzing {team} data..."):
            time.sleep(1)
            team_data = employees[employees["dept"] == team]
            avg_risk = round(team_data["risk"].mean(), 1)
            avg_sentiment = round(team_data["sentiment"].mean()*100, 1)
            avg_rating = round(team_data["rating"].mean(), 1)
            
            st.subheader(f"Retention Risk Heatmap for {team}")
            col1, col2, col3 = st.columns(3)
            col1.metric("Average Attrition Risk", f"{avg_risk}%", delta=f"{random.choice(['+2%', '-3%', '+5%'])} vs. last quarter")
            col2.metric("Average Sentiment", f"{avg_sentiment}%", delta=f"{random.choice(['+4%', '-2%'])} vs. last quarter")
            col3.metric("Average Performance Score", f"{avg_rating}/5")

            st.bar_chart(team_data.set_index("name")[["risk", "ot", "tenure"]])

            if avg_risk > 60:
                st.error(f"⚠️ **Elevated Risk in {team}:** Recommend immediate HRBP review.", icon="🚨")
            elif avg_risk > 40:
                st.warning(f"**Moderate Risk in {team}:** Recommend manager-level review of top drivers.", icon="⚠️")
            else:
                st.success(f"**Stable Risk in {team}:** Retention risk is within normal range.", icon="✅")

            st.markdown("---")
            st.subheader("HRBP & Leadership Dashboard (Demo Outputs)")
            
            # Mock "Top 5 Attrition Drivers" Output
            drivers = random.sample(["Pay vs. Market", "Lack of Career Pathing", "Poor Management Sentiment", "Workload", "Low Engagement Scores"], 3)
            st.info(f"""**Predicted Attrition Drivers (Top 3):**
1.  **{drivers[0]}**
2.  **{drivers[1]}**
3.  **{drivers[2]}**
            """)

            # Mock "At-Risk High-Performer" Alert Output
            if team == "Finance" or team == "Sales": # Mocking high risk for demo
                 st.error("""**Confidential: At-Risk High-Performer Alert**
* **1** high-performer (Top 10%) on this team is showing >85% flight risk.
* **Action:** Trigger proactive intervention workflow for HRBP.""", icon="🔒")

# ── 2️⃣ SENTIMENT & ENGAGEMENT ─────────────────────────
with tabs[1]:
    st.header("Agent 2: Sentiment & Engagement Monitor")
    st.write(
        """**Purpose:** Use NLP to analyze all textual feedback (myPulse, exit interviews) to pinpoint the specific, real-time drivers of employee sentiment by function, location, or project."""
    )
    
    comment = st.text_area("Paste a recent myPulse comment (or use default):", "Feeling burnt out lately with extra hours and unclear project goals. The new tooling is also slow, and it feels like career growth is stalled here.")
    
    if st.button("Analyze Sentiment", key="sentiment"):
        with st.spinner("Analyzing comment with Vertex AI NLP..."):
            time.sleep(1)
            score = random.randint(20, 45) # Mocking a negative comment
            st.metric("Sentiment Score (from text)", f"{score}%", delta="-8% vs. team average")
            
            if score < 40:
                st.error("Negative sentiment detected.")
            elif score < 60:
                st.warning("Mixed/Neutral sentiment detected.")
            else:
                st.success("Positive sentiment detected.")
            
            # Mock "Topic Cluster" Map Output
            st.subheader("Detected Topics & Topic Clusters")
            topics = random.sample(["Workload", "Tooling", "Project Goals", "Career Growth", "Work-Life Balance"], 4)
            st.info(f"""Key topics detected: **{topics[0]}**, **{topics[1]}**, **{topics[2]}**, **{topics[3]}**
* **Cluster:** "Workload" and "Project Goals" were mentioned together (Negative).
* **Cluster:** "Career Growth" was mentioned alone (Negative).
            """)

            st.markdown("---")
            st.subheader("Leadership Dashboard (Demo Outputs)")
            
            # Mock "Live Sentiment Index" Output
            st.write("**Live Sentiment Index (Overall Engineering)**")
            st.line_chart(pd.DataFrame({
                'Workload': [random.randint(20, 40) for _ in range(7)],
                'Tooling': [random.randint(30, 50) for _ in range(7)],
                'Management': [random.randint(60, 80) for _ in range(7)]
            }))
            
            # Mock "Hotspot Alert" Dashboard Output
            st.warning("""**Hotspot Alert Dashboard**
* Sentiment on **'Workload'** in the **A350 program** dropped **15%** this week.
* **Action:** Recommend review by local management.""", icon="⚠️")

# ── 3️⃣ TIME & WORKFORCE ANALYTICS ─────────────────────────
with tabs[2]:
    st.header("Agent 3: Time & Workforce Analytics")
    st.write(
        """**Purpose:** To provide managers and leadership with actionable insights into workforce productivity, cost allocation, and operational efficiency."""
    )

    st.subheader("Leadership Dashboard (Demo)")
    dept = st.selectbox("Select Department / Cost Center (BC/WC)", ["Engineering", "Finance", "Sales", "A350 Program", "Defence & Space"], key="time_dept")
    
    if st.button("Generate Productivity Report", key="time_btn"):
        with st.spinner(f"Analyzing workforce data for {dept}..."):
            time.sleep(1)
            # Mock data based on proposal
            ot_cost = random.randint(120000, 150000)
            ot_budget = 110000
            prod_hours = random.randint(3500, 4000)
            non_prod_hours = random.randint(1500, 2500)
            new_hire_prod = random.randint(55, 70)
            
            # Mock "Overtime Cost", "Productive Ratio", "New Hire Productivity" Outputs
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Overtime Cost vs. Budgeted", f"€{ot_cost:,}", f"€{ot_cost - ot_budget:,} Over Budget")
                if ot_cost > ot_budget:
                    st.error("Overtime costs are significantly over budget.", icon="💸")
            
            with col2:
                ratio = prod_hours / (prod_hours + non_prod_hours)
                st.metric("Productive Hour Ratio", f"{ratio:.1%}", "-3% vs. target")
                if ratio < 0.75:
                     st.warning("Productivity ratio below target.", icon="⚠️")

            with col3:
                st.metric("New Hire Time-to-Productivity", f"{new_hire_prod} Days", f"-5 days vs. avg.")
                if new_hire_prod > 60:
                    st.warning("New hire ramp time is lagging.", icon="📈")

            st.markdown("---")
            
            # Mock "Resource Utilization Report" Output
            st.subheader(f"Resource Utilization Report for {dept}")
            chart_data = pd.DataFrame({
                "Task Type": ["Project (Billable)", "Admin", "Training", "Meetings (Non-Project)"],
                "Hours Logged": [prod_hours, non_prod_hours / 2, non_prod_hours / 4, non_prod_hours / 4]
            })
            st.bar_chart(chart_data.set_index("Task Type"))
            st.info("**Insight:** Demo shows 15% of 'Senior Engineer' hours are being logged to 'Admin' tasks, indicating potential inefficiency or under-delegation.")

# ── 4️⃣ TRAINING & DEVELOPMENT RECOMMENDER ─────────────────────────
with tabs[3]:
    st.header("Agent 4: Training & Development Recommender")
    st.write(
        """**Purpose:** To build a more agile and mobile workforce by automatically mapping critical future skill gaps to personalized learning paths and internal mobility opportunities."""
    )
    
    st.subheader("Employee View: Development Plan")
    emp_role = st.selectbox("Your current role:", employees["role"].unique(), key="train_role")
    goal = st.text_input("Your career goal / next role:", "Senior Data Scientist")
    
    if st.button("Generate Development Plan", key="train_btn"):
        with st.spinner("Generating personalized plan..."):
            time.sleep(1)
            # Mock "Recommended For You" Output
            recs = random.sample(courses, 2)
            st.success(f"**Recommended learning for '{goal}'**")
            for r in recs:
                st.write(f"📘 **{r}** – [Enroll Now]")
            st.write(f"👥 **Mentor Matched:** {random.choice(['Liam Park', 'Sarah Chen', 'Raj Singh'])} (Senior Data Scientist)")

    st.markdown("---")
    st.subheader("Manager & Leadership View (Demo Outputs)")
    
    if st.button("Show Manager Dashboards", key="manager_view_btn"):
        # Mock "Skill Gap Analysis" Output
        st.info("""**Manager View: Skill Gap Analysis (My Team)**
* Your team is **80%** proficient in 'Python 3'.
* Your team is only **20%** proficient in **'Cloud Architecture'** (a critical future skill).
* **Action:** Nominate 2 high-performers for the 'Cloud Architecture' certification path.""")

        # Mock "Bench Strength" Report Output
        st.success("""**Manager View: Bench Strength Report**
* You have **3** employees who are 'Ready Now' for a 'Senior Engineering' role.
* **Employees:** Alex Smith, Maya Chen, [Name]
* **Action:** Begin career conversations for next-level roles.""")

# ── 5️⃣ HR POLICY & BENEFITS COPILOT ─────────────────────────
with tabs[4]:
    st.header("Agent 5: HR Policy & Benefits Copilot")
    st.write(
        """**Purpose:** Provide instant, accurate, and consistent 24/7 answers to Tier-1 HR policy and benefits questions using generative AI, reducing the load on HR Shared Services."""
    )

    st.subheader("Employee View: Ask HR")
    q = st.text_input("Ask a question (e.g., 'parental', 'vacation', 'benefits'):", "How much parental leave do we have?")
    
    if st.button("Get Answer", key="policy_btn"):
        with st.spinner("Searching policy documents..."):
            time.sleep(1)
            # Mock (Employee-facing) "Ask HR" Chat Output
            found = None
            for k, v in policies.items():
                if k in q.lower():
                    found = v
                    break
            if found:
                st.success(f"""**Answer:** {found}
*Source: Employee Handbook (2025), pg. 12*
                """)
            else:
                st.info("I couldn't find a specific answer for that. Forwarding your query to the HR shared inbox.")
    
    st.markdown("---")
    st.subheader("HR Leadership View: Analytics Dashboard")
    
    col1, col2 = st.columns(2)
    col1.metric("Response Accuracy (Pilot)", "93%", "+2% vs. last week")
    col2.metric("Tickets Deflected (Est.)", "88%", "450 tickets this month")

    # Mock (HR-facing) "Top 10 Most Asked" Output
    st.info("""**Top 5 Most Asked Questions (This Week):**
1.  "What is the 2026 holiday schedule?"
2.  "How do I submit travel expenses?"
3.  "Details on 'flexible work' policy."
4.  "What is the wellness stipend?"
5.  "Parental leave policy for secondary caregiver."
    """)
    
    # Mock (HR-facing) "Policy Gap" Alert Output
    st.warning("""**Policy Gap Alert**
* **300+** queries this week for **'Travel Policy'**, but no single, clear document is found.
* **Action:** Recommend Communications team to publish a new, clear policy doc.""", icon="⚠️")

# ─────────────────── FOOTER ───────────────────
st.markdown("---")
st.caption("Demo: 5 AI Agents to Guide Workforce Strategy | Airbus HR Pod | © 2025 Doanh Pham")
