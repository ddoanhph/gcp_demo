import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from foundry.transforms import Dataset


# Set page configuration
st.set_page_config(
    page_title="Cost of Labor Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

EURO_TO_USD_RATE = 1.07 # 1 EUR = 1.07 USD
USD_TO_EURO_RATE = 1 / EURO_TO_USD_RATE # 1 USD = approx 0.93 EUR

# Apply custom CSS for better styling, with improved header layout
st.markdown("""
<style>
    /* Header container to properly position logo and title */
    .header-container {
        display: flex;
        flex-direction: column;
        margin-bottom: 1rem;
    }
    
    /* Top bar with logo and optional divider */
    .top-bar {
        display: flex;
        align-items: center;
        margin-bottom: 0.5rem;
    }
    
    /* Airbus branding */
    .airbus-title {
        font-family: 'Frutiger', 'Univers', 'DIN Condensed', sans-serif;
        font-size: 2.8rem;
        font-weight: 700;
        color: #00205B; /* Airbus dark blue */
        text-align: left;
        margin: 0;
        letter-spacing: 0.5px;
        padding-left: 1rem;
        text-transform: uppercase;
    }

    /* Main dashboard title */
    .main-header {
        font-size: 3.3rem;
        font-weight: 700;
        color: #1E3A8A; /* Dark Blue */
        text-align: center;
        margin: 0.5rem 0 1rem 0;
        position: relative;
    }
    
    /* Decorative underline for main header */
    .main-header:after {
        content: "";
        position: absolute;
        width: 100px;
        height: 3px;
        background-color: #2563EB;
        bottom: -10px;
        left: 50%;
        transform: translateX(-50%);
    }
    
    /* Dashboard description text */
    .dashboard-description {
        text-align: center;
        color: #4B5563;
        margin: 1.5rem 0 2rem 0;
        max-width: 800px;
        margin-left: auto;
        margin-right: auto;
    }
    
    /* Horizontal divider */
    .header-divider {
        height: 1px;
        background: linear-gradient(to right, transparent, #CBD5E1, transparent);
        margin: 0.5rem 0;
        width: 100%;
    }

    /* --- Keep other existing styles below --- */
    .sub-header {
        font-size: 1.8rem;
        font-weight: 600;
        color: #2563EB; /* Medium Blue */
        margin-top: 1.5rem;
        margin-bottom: 0.75rem;
        border-bottom: 2px solid #DBEAFE; /* Light blue underline */
        padding-bottom: 0.25rem;
    }
    .metric-card {
        background-color: #FFFFFF;
        border-radius: 8px;
        padding: 1.25rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
        border: 1px solid #E5E7EB;
        border-left: 6px solid #2563EB;
        margin-bottom: 1rem;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        transition: all 0.2s ease-in-out;
    }
    .metric-card:hover {
        box-shadow: 0 7px 14px rgba(0, 0, 0, 0.1);
        transform: translateY(-4px);
        border-left-color: #1E3A8A;
    }
    .metric-value {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1E3A8A;
        line-height: 1.2;
        margin-bottom: 0.5rem;
    }
    .metric-label-container {
        display: flex;
        align-items: center;
        gap: 0.6rem;
    }
    .metric-label {
        font-size: 0.95rem;
        color: #4B5563;
        font-weight: 500;
        margin: 0;
    }
    .metric-icon {
        color: #6B7280;
        font-size: 1.2rem;
        width: 20px;
        text-align: center;
    }
    .highlight {
        background-color: #EFF6FF;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #2563EB;
        margin-top: 1rem;
    }
    .main .block-container {
        padding-top: 1rem;
    }
    /* Optional: Style default st.metric if used elsewhere */
    div[data-testid="stMetric"] {
       background-color: #FFFFFF; 
       border: 1px solid #E5E7EB;
       box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); 
       padding: 1rem; 
       border-radius: 8px;
    }
    div[data-testid="stMetric"] > label { 
        font-weight: 500; 
        color: #4B5563 !important; 
    }
    div[data-testid="stMetric"] > div { 
        color: #1E3A8A !important; 
    }
</style>
""", unsafe_allow_html=True)

# header structure
st.markdown("""
<div class="header-container">
    <div class="top-bar">
        <div class="airbus-title">AIRBUS</div>
    </div>
    <div class="header-divider"></div>
    <div class="main-header">Cost of Labor Dashboard</div>
    <div class="dashboard-description">
        This dashboard provides a comprehensive view of labor costs for 2023, 2024, and projected costs for 2025.
        It allows for simulation of new hires and their impact on the overall cost structure.
    </div>
</div>
""", unsafe_allow_html=True)


def calculate_totals(df):
    """Calculate total cost and hour metrics from employee data"""
    # input columns exist and fill NA with 0 for summation
    required_numeric_cols = [
        'base_salary', 'work_conditions_premium', 'overtime_premium', 'other_premiums',
        'annual_bonus', 'profit_sharing', 'social_contribution', 'ltips', 'planned_hours', 'actual_hours',
        'sick_hours', 'holiday_hours', 'other_absences'
    ]
    for col in required_numeric_cols:
        if col not in df.columns:
            df[col] = 0 # Add column if missing, initializing to 0
        else:
            # Ensure column is numeric, coercing errors and filling resulting NaNs
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    totals = {
        'total_base_salary': df['base_salary'].sum(),
        # Individual Premium Costs
        'total_overtime_premium_cost': df['overtime_premium'].sum(),
        'total_work_conditions_premium_cost': df['work_conditions_premium'].sum(),
        'total_other_specific_premiums_cost': df['other_premiums'].sum(),
        
        'total_bonuses': df['annual_bonus'].sum(),
        'total_profit_sharing': df['profit_sharing'].sum(),
        'total_social_contributions': df['social_contribution'].sum(),
        'total_ltips': df['ltips'].sum(),
        'total_fte': df['fte'].sum(),
        
        'total_planned_hours': df['planned_hours'].sum(),
        'total_actual_hours': df['actual_hours'].sum(),
        
        'total_sick_hours': df['sick_hours'].sum(),
        'total_holiday_hours': df['holiday_hours'].sum(),
        'total_other_absences_hours': df['other_absences'].sum(),
        
        'total_absence_costs': calculate_absence_costs(df),
        'total_overtime_hours': df['overtime_hours'].sum(),
        'total_cost': 0
    }

    # Calculate total_premiums based on the individual components for consistency
    totals['total_premiums'] = (
        totals['total_overtime_premium_cost'] +
        totals['total_work_conditions_premium_cost'] +
        totals['total_other_specific_premiums_cost']
    )
    
    totals['total_all_absence_hours'] = (
            totals['total_sick_hours'] +
            totals['total_holiday_hours'] +
            totals['total_other_absences_hours']
    )

    totals['total_cost'] = (
            totals['total_base_salary'] +
            totals['total_premiums'] + # Uses the sum of individual premiums
            totals['total_bonuses'] +
            totals['total_social_contributions'] +
            totals['total_profit_sharing'] +
            totals['total_ltips'] 
    )

    totals['fte_costs'] = totals['total_cost'] / totals['total_fte'] if totals['total_fte'] > 0 else 0

    return totals

def calculate_absence_costs(df):
    """Calculate absence costs based on base salary and days off"""
    # Assuming 260 working days per year (52 weeks * 5 days)
    daily_rate = df['base_salary'] / 260
    return (daily_rate * (df['sick_hours']/8 + df['holiday_hours']/8 + df['other_absences']/8)).sum()

def calculate_overtime(df):
    """Calculate overtime hours as actual_hours - planned_hours where positive"""
    return (df['actual_hours'] - df['planned_hours']).apply(lambda x: max(0, x)).sum()

def get_monthly_actual_costs(df, year_to_filter, date_column_name='pay_date'): # Adjust 'pay_date' if your column is named differently
    """
    Calculates total individual costs aggregated by month for a specific year.
    Assumes 'df' contains individual records with a date column and cost components.
    """
    df_copy = df.copy()

    # Ensure 'total_individual_cost' exists or calculate it
    if 'total_individual_cost' not in df_copy.columns:
        # Sum of all cost components per record. Ensure all these columns exist in your df.
        # Adding .fillna(0) for robustness.
        df_copy['total_individual_cost'] = (
            df_copy.get('base_salary', pd.Series(0, index=df_copy.index)).fillna(0) +
            df_copy.get('work_conditions_premium', pd.Series(0, index=df_copy.index)).fillna(0) +
            df_copy.get('overtime_premium', pd.Series(0, index=df_copy.index)).fillna(0) +
            df_copy.get('other_premiums', pd.Series(0, index=df_copy.index)).fillna(0) +
            df_copy.get('annual_bonus', pd.Series(0, index=df_copy.index)).fillna(0) +
            df_copy.get('profit_sharing', pd.Series(0, index=df_copy.index)).fillna(0) +
            df_copy.get('social_contribution', pd.Series(0, index=df_copy.index)).fillna(0) +
            df_copy.get('ltips', pd.Series(0, index=df_copy.index)).fillna(0)
        )
    
    if date_column_name not in df_copy.columns:
        # If no date column, we can't do monthly analysis.
        # Return a series of zeros for all 12 months.
        st.warning(f"Monthly analysis requires a date column (e.g., '{date_column_name}') in the raw data. Displaying zero for monthly actuals.")
        return pd.Series(index=range(1, 13), data=0.0, name='Monthly_Cost').rename_axis("Month")

    try:
        df_copy[date_column_name] = pd.to_datetime(df_copy[date_column_name])
        df_year_filtered = df_copy[df_copy[date_column_name].dt.year == year_to_filter]
        
        if df_year_filtered.empty: # No data for the specified year
             return pd.Series(index=range(1, 13), data=0.0, name='Monthly_Cost').rename_axis("Month")

        monthly_costs = df_year_filtered.groupby(df_year_filtered[date_column_name].dt.month)['total_individual_cost'].sum()
        # Ensure all 12 months are present, filling with 0.0 if no data for a specific month
        monthly_costs = monthly_costs.reindex(range(1, 13), fill_value=0.0)
        monthly_costs.index.name = "Month"
        monthly_costs.name = "Monthly_Cost"
        return monthly_costs
    except Exception as e:
        st.error(f"Error processing date column '{date_column_name}' for monthly analysis: {e}. Please check the column format.")
        return pd.Series(index=range(1, 13), data=0.0, name='Monthly_Cost').rename_axis("Month")
    
def project_costs_for_new_hires(current_totals, band_avg_df, dept, band, location, num_new_hires):
    """Project costs for 2025 based on current totals and new hires"""
    # Filter band averages for the selected Entity/Function and band
    avg_data = band_avg_df[(band_avg_df['Entity/Function'] == dept) 
                            & (band_avg_df['band'] == band)
                            & (band_avg_df['location'] == location)]

    if avg_data.empty:
        st.error(f"No average data available for {dept}, {band}, {location}")
        return current_totals

    # Get the first (and should be only) row
    avg = avg_data.iloc[0]

    # Calculate projections
    projections = current_totals.copy()

    # Add costs for new hires
    projections['total_base_salary'] += avg['avg_base_salary'] * num_new_hires
    projections['total_premiums'] += (avg['avg_work_conditions'] + avg['avg_overtime'] + avg[
        'avg_other_premiums']) * num_new_hires
    projections['total_bonuses'] += (avg['avg_annual_bonus'] + avg['avg_profit_sharing']) * num_new_hires
    projections['total_social_contributions'] += (avg['avg_social_contribution']) * num_new_hires
    projections['total_ltips'] += avg['avg_ltips'] * num_new_hires
    projections['total_fte'] += num_new_hires
    projections['total_planned_hours'] += avg['avg_planned_hours'] * num_new_hires

    # Recalculate total cost
    projections['total_cost'] = (
            projections['total_base_salary'] +
            projections['total_premiums'] +
            projections['total_bonuses'] +
            projections['total_social_contributions'] +
            projections['total_ltips']
    )

    # Calculate new hiring costs
    projections['hiring_costs'] = avg['avg_hiring_cost'] * num_new_hires

    # Update FTE costs
    projections['fte_costs'] = projections['total_cost'] / projections['total_fte'] if projections[
                                                                                           'total_fte'] > 0 else 0

    return projections

def apply_sidebar_filters(df, selected_dept_list, selected_band_group, selected_collar):
    """Applies the three main filters (Department, Band Group, Collar) to the DataFrame."""
    
    df_filtered = df.copy()

    # Entity/Function Filter
    # Filter only if the list is not empty AND does not contain the 'All' option.
    if selected_dept_list and "All Entity/Function" not in selected_dept_list: 
        df_filtered = df_filtered[df_filtered["Entity/Function"].isin(selected_dept_list)]

    # Blue/White Collar Filter
    if selected_collar != "All Job Types":
        df_filtered = df_filtered[df_filtered["Blue_White_Collar"] == selected_collar]

    # Band Group Filter
    if selected_band_group != "All Bands":
        below_biii_bands = ["BZ", "BV", "BIV"]
        biii_plus_bands = ["BIII", "BII", "undefined"]
        
        if selected_band_group == "Below BIII (BZ, BV, BIV)":
            df_filtered = df_filtered[df_filtered["band"].isin(below_biii_bands)]
        elif selected_band_group == "BIII+ (BIII, BII, Undefined)":
            df_filtered = df_filtered[df_filtered["band"].isin(biii_plus_bands)]
            
    return df_filtered
    
def convert_currency(value, selected_currency):
    """Converts USD value to EUR if selected_currency is 'Euro'."""
    if selected_currency == "Euro":
        return value * USD_TO_EURO_RATE
    return value

def format_currency(value, selected_currency):
    """Formats the numerical value into a currency string based on the selection."""
    converted_value = convert_currency(value, selected_currency)
    if selected_currency == "Euro":
        return f"€{converted_value:,.2f}"
    return f"${converted_value:,.2f}"

def apply_currency_format(value):
    # Pass the global selected_currency to the function
    return format_currency(value, selected_currency)

def load_data():
    df_2023 = Dataset.get("payroll_processed_2023").read_table(format="pandas")
    df_2024 = Dataset.get("payroll_processed_2024").read_table(format="pandas")
    band_avg_df = Dataset.get("aggregation_df").read_table(format="pandas")

    return df_2023, df_2024, band_avg_df

df_2023, df_2024, band_avg_df = load_data()

# Calculate totals for 2023 and 2024
totals_2023 = calculate_totals(df_2023)
totals_2024 = calculate_totals(df_2024)

# --- SIDEBAR CONTROLS ---
st.sidebar.markdown("## Dashboard Controls")

# Currency Selector
selected_currency = st.sidebar.radio(
    "💰 Select Currency",
    ["USD", "Euro"],
    horizontal=True,
    key="currency_selector"
)

# Year selection
selected_year = st.sidebar.selectbox("⚙️ Select Year", ["2023", "2024", "2025 (Projected)"])

all_departments_unique = sorted(df_2023["Entity/Function"].unique().tolist())
all_departments_options = ["All Entity/Function"] + all_departments_unique

# Entity Filter
selected_dept = st.sidebar.multiselect(
    "⚙️ Filter by Entity/Function",
    options=all_departments_options,
    default=["All Entity/Function"] 
)

all_departments = all_departments_unique

# Band Group Filter ---
band_group_options = ["All Bands", "Below BIII (BZ, BV, BIV)", "BIII+ (BIII, BII, Undefined)"]
selected_band_group = st.sidebar.selectbox(
    "⚙️ Filter by Band Group",
    band_group_options
)

# --- NEW: Blue/White Collar Filter ---
collar_options = ["All Job Types"] + sorted(df_2023["Blue_White_Collar"].unique().tolist())
selected_collar = st.sidebar.selectbox(
    "⚙️ Filter by Job Type (Collar)",
    collar_options
)

# Initialize session state for the start month if it doesn't exist
if 'selected_hire_start_month' not in st.session_state:
    st.session_state.selected_hire_start_month = 1 # Default to January (1)

# Initialize projection parameters
num_new_hires = 0
default_selected_band = "BV"
default_selected_hire_dept = "Finance" # Changed from "ERP"
default_selected_hire_location = "Herndon, VA"
projected_totals = totals_2024.copy()

month_names_dict = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
}

# --- PROJECTION PARAMETERS (NOW FULLY CONDITIONAL) ---
# --- (In the Sidebar section) ---

# Only show projection inputs when 2025 is selected
if selected_year == "2025 (Projected)":
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 2025 Projection Parameters")

    # The month selector logic remains the same
    selected_hire_start_month = st.sidebar.selectbox(
        "New Hire Start Month (2025)",
        options=list(month_names_dict.keys()),
        format_func=lambda m: month_names_dict[m],
        key="sb_hire_start_month"
    )
    st.session_state.selected_hire_start_month = selected_hire_start_month

    num_new_hires = st.sidebar.number_input("Number of New Hires", min_value=0, max_value=100, value=0)
    
    # --- DYNAMIC DROPDOWN LOGIC STARTS HERE ---
    
    # 1. First, select the Function/Entity
    # We use the UNFILTERED data here for the selection list
    sorted_departments = sorted(df_2024["Entity/Function"].unique().tolist())
    try:
        default_dept_index = sorted_departments.index(default_selected_hire_dept)
    except ValueError:
        default_dept_index = 0

    selected_hire_dept = st.sidebar.selectbox(
        "Function/Entity for New Hires",
        options=sorted_departments,
        index=default_dept_index
    )

    all_locations = sorted(band_avg_df["location"].unique().tolist())
    try:
        default_location_index = all_locations.index(default_selected_hire_location)
    except ValueError:
        default_location_index = 0
        
    selected_hire_location = st.sidebar.selectbox(
        "Location for New Hires",
        options=all_locations,
        index=default_location_index,
        key="sb_hire_location"
    )

    valid_bands_for_dept = sorted(
        band_avg_df[
            (band_avg_df['Entity/Function'] == selected_hire_dept) &
            (band_avg_df['location'] == selected_hire_location)]['band'].unique().tolist())

    # 3. FINALLY, create the Band dropdown with ONLY the valid options
    selected_band = st.sidebar.selectbox(
        "Band for New Hires",
        options=valid_bands_for_dept,
    )

    # Calculate projections based on the now-guaranteed-to-be-valid inputs
    if selected_band: # Ensure a band was actually available and selected
        projected_totals = project_costs_for_new_hires(
            totals_2024,
            band_avg_df,
            selected_hire_dept,
            selected_band,
            selected_hire_location,
            num_new_hires
        )
    else:
        st.sidebar.warning(f"No bands with average cost data found for {selected_hire_dept}.")


# --- START OF NEW FILTERING LOGIC ---

df_2023_filtered = apply_sidebar_filters(df_2023, selected_dept, selected_band_group, selected_collar)
df_2024_filtered = apply_sidebar_filters(df_2024, selected_dept, selected_band_group, selected_collar)

# Recalculate totals based on the newly filtered DataFrames
dept_totals_2023 = calculate_totals(df_2023_filtered)
dept_totals_2024 = calculate_totals(df_2024_filtered)


# Filter data based on selected department
# if selected_dept != "All Entity/Function":
#     df_2023_filtered = df_2023[df_2023["Entity/Function"] == selected_dept]
#     df_2024_filtered = df_2024[df_2024["Entity/Function"] == selected_dept]
#     dept_totals_2023 = calculate_totals(df_2023_filtered)
#     dept_totals_2024 = calculate_totals(df_2024_filtered)
# else:
#     df_2023_filtered = df_2023
#     df_2024_filtered = df_2024
#     dept_totals_2023 = totals_2023
#     dept_totals_2024 = totals_2024

# Main dashboard area
col1, col2, col3 = st.columns(3)

# Determine which totals to use based on selected year
if selected_year == "2023":
    current_totals = dept_totals_2023
elif selected_year == "2024":
    current_totals = dept_totals_2024
else:  # 2025 Projected
    # Check if *all* departments are selected (i.e., no effective filter is active)
    if set(selected_dept) == set(all_departments):
        # Case 1: No functional filter active. Use the overall, site-level projection.
        current_totals = projected_totals
    else:
        # Case 2: A functional filter is active. Start with the filtered 2024 total.
        current_totals = dept_totals_2024.copy()

        # Only add new hire costs if the specific hire department is IN the selected filter list
        if selected_hire_dept in selected_dept:
            # Re-calculate the projection starting from the filtered base (dept_totals_204)
            current_totals = project_costs_for_new_hires(
                dept_totals_2024,
                band_avg_df,
                selected_hire_dept,
                selected_band,
                selected_hire_location,
                num_new_hires
            )

# Main dashboard area - Use the custom markdown approach with icons
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f'''
        <div class="metric-card">
            <div class="metric-value">{format_currency(current_totals["total_cost"], selected_currency)}</div>
            <div class="metric-label-container">
                 <i class="fas fa-dollar-sign metric-icon"></i>
                 <span class="metric-label">Total Cost of Labor ({selected_year})</span>
            </div>
        </div>
    ''', unsafe_allow_html=True)

with col2:
    st.markdown(f'''
        <div class="metric-card">
            <div class="metric-value">{current_totals["total_fte"]:,.0f}</div>
             <div class="metric-label-container">
                 <i class="fas fa-users metric-icon"></i>
                 <span class="metric-label">Total FTE ({selected_year})</span>
            </div>
        </div>
    ''', unsafe_allow_html=True)

with col3:
    st.markdown(f'''
        <div class="metric-card">
            <div class="metric-value">{format_currency(current_totals["fte_costs"], selected_currency)}</div>
             <div class="metric-label-container">
                 <i class="fas fa-money-check-alt metric-icon"></i>
                 <span class="metric-label">Cost per FTE ({selected_year})</span>
            </div>
        </div>
    ''', unsafe_allow_html=True)

# Create tabs for different views
tab1, tab2, tab3, tab4 = st.tabs(["📚 Dashboard Guide", "📊 Cost Breakdown", "📈 Year-over-Year", "🔍 Detailed Analysis"])

# NEW TAB: Dashboard Guide
with tab1:
    st.markdown('<h2 class="section-header">Navigating the Dashboard: Data & Costs Explained</h2>', unsafe_allow_html=True)
    st.markdown("""
        <p class="dashboard-description" style="text-align:left; max-width:none; margin-bottom:2rem;">
        Welcome! This guide helps you understand the critical information presented in the Cost of Labor dashboard.
        We'll walk you through the data sources, how different cost components are calculated, and how you can use
        the projection features to plan for the future.
        </p>
    """, unsafe_allow_html=True)

    # Consolidated and prominent Data Scope context
    st.markdown(f"""
    <div class="note-section" style="border-left: 4px solid #007bff;">
        <p><strong>Dashboard Scope:</strong> Data for this dashboard is derived solely from <strong>AAI (Airbus Americas, Inc.) US employee information</strong>.</p>
    </div>
    """, unsafe_allow_html=True)


    st.markdown('<h3 class="section-header" style="font-size:1.6rem; margin-top:1rem; margin-bottom:1rem; border-bottom: none;">Understanding the Data & Scope 📊</h3>', unsafe_allow_html=True)

    cols_data_scope_cost = st.columns(2)
    with cols_data_scope_cost[0]:
        with st.expander("Historical Data Scope 📅", expanded=True):
            # Content directly inside the expander, using note-section for consistent styling
            st.markdown("""
            <div class="note-section">
                <p>The dashboard provides a deep dive into the <strong>actual labor costs</strong> for specific historical years.</p>
                <h4>What's Included:</h4>
                <ul>
                    <li><strong>Comprehensive Payroll Data:</strong> Detailed payroll records for <strong>2023 and 2024</strong>, covering all active employees within the selected 'Function/Entity'.</li>
                    <li><strong>Full Cost Components:</strong> Captures all major elements contributing to labor costs.</li>
                    <li><strong>Actual Hours Worked:</strong> Data on planned and actual hours for productivity and overtime analysis.</li>
                </ul>
                <p>This historical data forms the foundation for understanding past trends and informs future projections.</p>
            </div>
            """, unsafe_allow_html=True)

    with cols_data_scope_cost[1]:
        with st.expander("Future Gaze: Projections & Simulations 🔮", expanded=True):
            # Content directly inside the expander, using note-section for consistent styling
            st.markdown("""
            <div class="note-section">
                <p>The <strong>2025 (Projected)</strong> view allows you to simulate the financial impact of future hiring decisions.</p>
                <h4>How it Works:</h4>
                <ul>
                    <li><strong>Base from 2024:</strong> 2025 projection starts with 2024 actual labor costs as its baseline.</li>
                    <li><strong>New Hire Simulation:</strong> Input the <strong>number of new hires</strong>, their expected <strong>Function/Entity</strong>, and <strong>Band</strong>.</li>
                    <li><strong>Average Cost Logic:</strong> Uses pre-calculated <strong>average costs per employee</strong> (derived from historical data) to estimate new hire impact.</li>
                    <li><strong>"What-If" Scenarios:</strong> Helps understand how headcount changes for specific roles might influence overall labor cost.</li>
                </ul>
                <p>These projections are estimations based on historical averages and your inputs, valuable for strategic planning.</p>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('<h3 class="section-header" style="font-size:1.6rem; margin-top:2.5rem; margin-bottom:1rem; border-bottom: none;">Understanding the Cost Components 💰</h3>', unsafe_allow_html=True)
    st.markdown("""
        <p class="note-section" style="margin-bottom:1.5rem;">
        Labor costs are more than just salaries. This dashboard breaks down the total cost into its key elements for a holistic view.
        </p>
    """, unsafe_allow_html=True)

    with st.expander("Key Cost Categories Explained 🔍"):
        # Corrected section using st.columns for each item
        st.markdown('<div class="note-section">', unsafe_allow_html=True)

        # Base Salary
        col1_bs, col2_bs = st.columns([0.4, 0.6]) # Adjusted ratio for better alignment
        with col1_bs:
            st.markdown('<p><span class="highlight"><strong>Base Salary:</strong></span></p>', unsafe_allow_html=True)
        with col2_bs:
            st.markdown('<p>Fundamental fixed compensation paid to employees.</p>', unsafe_allow_html=True)

        # Premiums
        col1_p, col2_p = st.columns([0.4, 0.6])
        with col1_p:
            st.markdown('<p><span class="highlight"><strong>Premiums:</strong></span></p>', unsafe_allow_html=True)
        with col2_p:
            st.markdown('<p>Additional payments for work conditions, overtime, or other special circumstances (e.g., Work Conditions, Overtime, Other Premiums).</p>', unsafe_allow_html=True)

        # Bonuses
        col1_b, col2_b = st.columns([0.4, 0.6])
        with col1_b:
            st.markdown('<p><span class="highlight"><strong>Bonuses:</strong></span></p>', unsafe_allow_html=True)
        with col2_b:
            st.markdown('<p>Performance-based incentives, including Annual Bonus and Profit Sharing.</p>', unsafe_allow_html=True)

        # Social Contributions
        col1_sc, col2_sc = st.columns([0.4, 0.6])
        with col1_sc:
            st.markdown('<p><span class="highlight"><strong>Social Contributions:</strong></span></p>', unsafe_allow_html=True)
        with col2_sc:
            st.markdown('<p>Employer-paid taxes and benefits (e.g., Social Security, Medicare, Employer 401(k), Employer Pension contributions).</p>', unsafe_allow_html=True)

        # LTIPs
        col1_l, col2_l = st.columns([0.4, 0.6])
        with col1_l:
            st.markdown('<p><span class="highlight"><strong>LTIPs:</strong></span></p>', unsafe_allow_html=True)
        with col2_l:
            st.markdown('<p>Long-Term Incentive Plans (equity-based or cash) for long-term performance and retention.</p>', unsafe_allow_html=True)

        # Absence Costs
        col1_ac, col2_ac = st.columns([0.4, 0.6])
        with col1_ac:
            st.markdown('<p><span class="highlight"><strong>Absence Costs:</strong></span></p>', unsafe_allow_html=True)
        with col2_ac:
            st.markdown('<p>Calculated cost of paid time off (sick days, holidays, other absences) based on base salary.</p>', unsafe_allow_html=True)

        # Hiring Costs (Projected)
        col1_hc, col2_hc = st.columns([0.4, 0.6])
        with col1_hc:
            st.markdown('<p><span class="highlight"><strong>Hiring Costs (Projected):</strong></span></p>', unsafe_allow_html=True)
        with col2_hc:
            st.markdown('<p>Estimated costs for recruitment, onboarding, and initial training for new hires in 2025 projections.</p>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True) # Close the note-section div

    with st.expander("Planned vs. Actual Hours & Overtime ⏰"):
        st.markdown("""
        <div class="note-section">
            <p>Beyond financial figures, understanding working hours provides insights into workforce utilization:</p>
        """, unsafe_allow_html=True) # Start note-section here

        # Planned Hours
        col1_ph, col2_ph = st.columns([0.4, 0.6]) # Adjusted ratio
        with col1_ph:
            st.markdown('<p><span class="highlight"><strong>Planned Hours:</strong></span></p>', unsafe_allow_html=True)
        with col2_ph:
            st.markdown('<p>Standard, expected working hours for all employees.</p>', unsafe_allow_html=True)

        # Actual Hours
        col1_ah, col2_ah = st.columns([0.4, 0.6]) # Adjusted ratio
        with col1_ah:
            st.markdown('<p><span class="highlight"><strong>Actual Hours:</strong></span></p>', unsafe_allow_html=True)
        with col2_ah:
            st.markdown('<p>Total hours employees actually worked, differing due to overtime or under-utilization.</p>', unsafe_allow_html=True)

        # Overtime Hours
        col1_oh, col2_oh = st.columns([0.4, 0.6]) # Adjusted ratio
        with col1_oh:
            st.markdown('<p><span class="highlight"><strong>Overtime Hours:</strong></span></p>', unsafe_allow_html=True)
        with col2_oh:
            st.markdown('<p>Sum of hours where \'Actual Hours\' exceed \'Planned Hours\', highlighting extra work and its cost implications.</p>', unsafe_allow_html=True)

        st.markdown("""
            <p>Monitoring these metrics helps optimize resource allocation and manage operational efficiency.</p>
        </div>
        """, unsafe_allow_html=True) # Close note-section here

    st.markdown('<h3 class="section-header" style="font-size:1.6rem; margin-top:2.5rem; margin-bottom:1rem; border-bottom: none;">Leveraging the Dashboard for Impact 📈</h3>', unsafe_allow_html=True)
    st.markdown("""
        <p class="note-section" style="margin-bottom:1.5rem;">
        This dashboard is more than just a reporting tool; it's designed to support informed decision-making and strategic planning.
        </p>
    """, unsafe_allow_html=True)

    with st.expander("Strategic Value & Use Cases ✅"):
        st.markdown("""
        <div class="note-section">
            <p>You can use this dashboard to:</p>
            <ul>
                <li><strong>Monitor & Control Costs:</strong> Gain a clear, real-time understanding of where your labor budget is being allocated.</li>
                <li><strong>Identify Cost Drivers:</strong> Pinpoint which cost categories are growing or shrinking year-over-year.</li>
                <li><strong>Support Budgeting & Forecasting:</strong> Utilize the 2025 projection tool to accurately estimate future labor costs based on anticipated hiring plans.</li>
                <li><strong>Evaluate Staffing Efficiency:</strong> Analyze planned vs. actual hours and overtime trends to assess workload management and potential inefficiencies.</li>
                <li><strong>Inform Compensation Strategy:</strong> Understand average salaries and cost per FTE across different funtions or entities and bands to refine compensation and benefits policies.</li>
                <li><strong>Drive Data-Driven Discussions:</strong> Facilitate conversations with leadership and Entity/Function heads about resource allocation, operational efficiency, and strategic workforce planning.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("""
        <hr style="margin-top:2rem; margin-bottom:1rem;">
        <p class="note-section" style="text-align:center; font-size:1.1rem;">
        Our goal is to provide transparent and actionable insights into the Cost of Labor, enabling smarter financial and people decisions for Airbus!
        </p>
    """, unsafe_allow_html=True)

# Tab 1: Cost Breakdown
with tab2:
    # Add the sub-header for this tab
    st.markdown('<div class="sub-header">Cost Breakdown Analysis</div>', unsafe_allow_html=True)

    # Create DataFrame for easier processing
    cost_data = {
        'Category': ['Base Salary', 'Premiums Work Conditions', 'Overtime', 'Annual Bonuses', 'Profit Sharing', 'Other Premiums', 'Social Contribution', 'LTIPs'],
        'Amount': [
            current_totals.get('total_base_salary', 0), 
            current_totals.get('total_work_conditions_premium_cost', 0),
            current_totals.get('total_overtime_premium_cost', 0),
            current_totals.get('total_bonuses', 0),
            current_totals.get('total_profit_sharing', 0),
            current_totals.get('total_other_specific_premiums_cost', 0),
            current_totals.get('total_social_contributions', 0),
            current_totals.get('total_ltips', 0)
        ]
    }
    cost_df = pd.DataFrame(cost_data)

    # Calculate total cost for percentages and center annotation
    # Use the sum from the DataFrame to ensure consistency
    total_cost_for_breakdown = cost_df['Amount'].sum()

    # Calculate percentages (handle division by zero if total is zero)
    if total_cost_for_breakdown > 0:
        # Ensure Percentage is float
        cost_df['Percentage'] = (cost_df['Amount'] / total_cost_for_breakdown * 100).astype(float)
    else:
        cost_df['Percentage'] = 0.0

    # Ensure Amount is numeric
    cost_df['Amount'] = pd.to_numeric(cost_df['Amount'])

    # Sort by Amount descending for better visual hierarchy in metrics display
    cost_df = cost_df.sort_values(by='Amount', ascending=False).reset_index(drop=True)

    # --- Create Donut Chart (Displayed first) ---
    st.markdown(f"#### Cost Distribution for {selected_year}") # Add a title above the chart

    fig = go.Figure(data=[go.Pie(
        labels=cost_df['Category'],
        values=cost_df['Amount'],
        hole=.4,  # Creates the donut hole
        # Slightly pull out the largest slice (first row after sorting)
        pull=[0.05 if i == 0 else 0 for i in cost_df.index],
        marker_colors=px.colors.sequential.Blues_r, # Use a sequential color scheme
        textinfo='label+percent', # Show label and percentage on slices
        insidetextorientation='auto', # Let Plotly decide best text orientation
        # Define hover text format
        hovertemplate="<b>%{label}</b><br>Amount: %{value:$,.2f}<br>Percentage: %{percent:.1%}<extra></extra>"
    )])

    # Add center annotation for Total Cost
    # Make sure your format_currency function is defined elsewhere or replace with f-string formatting
    try:
        formatted_total = format_currency(total_cost_for_breakdown, selected_currency)
    except NameError:
        # Fallback if format_currency isn't defined in this scope
        formatted_total = f"${total_cost_for_breakdown:,.2f}"

    fig.add_annotation(
        text=f"Total:<br>{formatted_total}",
        x=0.5, y=0.5, # Center position
        font_size=18,
        showarrow=False,
        font_color="#1E3A8A" # Match header color from CSS if possible
    )

    # Update chart layout
    fig.update_layout(
        legend_title_text='Categories', # Add title to legend
        showlegend=True, # Ensure legend is shown
        margin=dict(t=20, b=20, l=20, r=20), # Adjust margins
        height=500 # Adjust height as needed for chart + legend
    )

    # Display the chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)
    
    # --- Create METRIC DISPLAYS for Breakdown Details (Displayed below chart) ---
    st.markdown("---") 
    st.markdown("#### Breakdown Details")
    st.dataframe(cost_df.style.format({"Amount": lambda x: apply_currency_format(x), "Percentage": "{:,.2f}%"}), use_container_width=True, hide_index= True)

    # --- NEW: Waterfall Chart for Cost per FTE ---
    st.markdown("---")
    st.markdown("#### Incremental Cost per FTE Breakdown")
    st.info("This waterfall chart illustrates how the Cost per FTE builds up from the base salary through all additional cost components.")

    # 1. Prepare data for the Waterfall Chart (Cost per FTE)
    # Using the same cost categories as your attached image/code structure:
    # NOTE: The waterfall chart is complex to calculate from scratch (needs specific cost per FTE for each),
    # so we'll simplify by converting the total annual cost components to a 'per FTE' basis.
    
    fte_count = current_totals.get('total_fte', 0)
    
    # Calculate Cost per FTE for each major component
    if fte_count > 0:
        base_salary_fte = current_totals.get('total_base_salary', 0) / fte_count
        premiums_wc_fte = current_totals.get('total_work_conditions_premium_cost', 0) / fte_count
        overtime_fte = current_totals.get('total_overtime_premium_cost', 0) / fte_count
        annual_bonus_fte = current_totals.get('total_bonuses', 0) / fte_count
        profit_sharing_fte = current_totals.get('total_profit_sharing', 0) / fte_count
        other_premiums_fte = current_totals.get('total_other_specific_premiums_cost', 0) / fte_count
        social_contribution_fte = current_totals.get('total_social_contributions', 0) / fte_count
        ltips_fte = current_totals.get('total_ltips', 0) / fte_count
        total_cost_fte = current_totals.get('total_cost', 0) / fte_count
    else:
        # If no FTEs, set all to zero to avoid division by zero
        base_salary_fte, premiums_wc_fte, overtime_fte, annual_bonus_fte = 0, 0, 0, 0
        profit_sharing_fte, other_premiums_fte, social_contribution_fte, ltips_fte, total_cost_fte = 0, 0, 0, 0, 0

    # 2. Define labels and measures for Plotly Waterfall
    # For simplicity and visual impact, we combine Annual Bonus and Profit Sharing, 
    # and combine all premiums not already split out (Other Premiums).
    waterfall_labels = [
        "Base Salary", 
        "Premiums Work Conditions", 
        "Overtime", 
        "Annual Bonus & Profit Sharing", 
        "Other Premiums", 
        "Social Contribution", 
        "LTIP Payments",
        "Total Cost"
    ]

    # The 'measures' array defines how to treat each value: absolute (start), relative (increment), or total (end)
    waterfall_measures = [
        "absolute", # Base Salary is the starting value
        "relative", # Premiums WC is an increment
        "relative", # Overtime is an increment
        "relative", # Bonuses & Profit Sharing is an increment
        "relative", # Other Premiums is an increment
        "relative", # Social Contribution is an increment
        "relative", # LTIPs is an increment
        "total"     # Total Cost is the final sum
    ]

    # The 'y' values (the increments/totals)
    # The first value is the starting absolute value (Base Salary FTE)
    # Subsequent values are the incremental cost per FTE, except for the final 'total'
    waterfall_y_values = [
        base_salary_fte, 
        premiums_wc_fte,
        overtime_fte,
        annual_bonus_fte + profit_sharing_fte, # Combined
        other_premiums_fte,
        social_contribution_fte,
        ltips_fte,
        total_cost_fte # The cumulative total
    ]

    # Create the Waterfall Figure
    fig_waterfall = go.Figure(go.Waterfall(
        orientation = "v", # Vertical orientation
        measure = waterfall_measures,
        x = waterfall_labels,
        y = waterfall_y_values,
        connector = {"line":{"color":"rgb(63, 63, 63)"}}, # Dark connector lines
        
        # colors for different bar types
        increasing = {"marker":{"color": "#1976D2"}},
        decreasing = {"marker":{"color": "#FF5252"}}, 
        totals = {"marker":{"color": "#00205B"}}, # Dark Blue for the final total
        
        # Text display on the bars
        text = [f"{convert_currency(v, selected_currency):,.0f}" for v in waterfall_y_values],
        textposition = "outside",
    ))

    # Update layout
    currency_prefix = "€" if selected_currency == "Euro" else "$"
    
    fig_waterfall.update_layout(
        title = f"Cost per FTE Build-up ({selected_year})",
        showlegend = False,
        height = 600,
        xaxis_title = "Cost Component",
        yaxis_title = f"Amount per FTE ({selected_currency})",
        margin = dict(t=50, b=100, l=50, r=50) # Increased bottom margin for labels
    )
    
    # Format Y-axis ticks
    fig_waterfall.update_yaxes(tickprefix=currency_prefix, tickformat=",")
    
    # Display the chart
    st.plotly_chart(fig_waterfall, use_container_width=True)
    # --- END OF NEW WATERFALL CHART ---

# --- BEGINNING OF COMPARATIVE COST ANALYSIS SECTION ---
    st.markdown("---") # Add a separator
    st.markdown("#### Comparative Cost Analysis by Dimension")
    st.markdown(
        "Analyze cost metrics by selecting a dimension to group the data. "
        "This can help identify variations in cost structures across different parts of the organization."
    )

    if selected_year == "2023":
        analysis_df = df_2023_filtered
        analysis_totals = dept_totals_2023
    elif selected_year == "2024":
        analysis_df = df_2024_filtered
        analysis_totals = dept_totals_2024
    else:  # 2025 projection - base on 2024 data
        analysis_df = df_2024_filtered
        analysis_totals = current_totals
        
    df_for_comparison = analysis_df.copy()
    if 'total_individual_cost' not in df_for_comparison.columns:
        df_for_comparison['total_individual_cost'] = (
            df_for_comparison['base_salary'].fillna(0) +
            df_for_comparison['work_conditions_premium'].fillna(0) +
            df_for_comparison['overtime_premium'].fillna(0) +
            df_for_comparison['other_premiums'].fillna(0) +
            df_for_comparison['annual_bonus'].fillna(0) +
            df_for_comparison['profit_sharing'].fillna(0) +
            df_for_comparison['social_contribution'].fillna(0) +
            df_for_comparison['ltips'].fillna(0)
        )

    comparison_options = {
        "Entity/Function": "Entity/Function",
        "Job Type (Blue/White Collar)": "Blue_White_Collar",
        "Location": "location"
    }
    selected_display_dimension = st.selectbox(
        "Select Dimension for Comparison:",
        options=list(comparison_options.keys()),
        key="cost_comparison_dimension_selector"
    )
    dimension_to_group_by = comparison_options[selected_display_dimension]

    if dimension_to_group_by:
        if dimension_to_group_by not in df_for_comparison.columns:
            st.error(f"Dimension column '{dimension_to_group_by}' not found in the data.")
        elif df_for_comparison.empty:
            st.warning(f"No data available for comparison for the current selection.")
        else:
            # --- Data aggregation logic remains the same ---
            summary_df = df_for_comparison.groupby(dimension_to_group_by, as_index=False).agg(
                Number_of_FTEs=('fte', 'sum'),
                Total_Labor_Cost=('total_individual_cost', 'sum'),
                Average_Base_Salary=('base_salary', 'mean')
            )
            summary_df['Average_Cost_per_FTE'] = summary_df.apply(
                lambda row: row['Total_Labor_Cost'] / row['Number_of_FTEs'] if row['Number_of_FTEs'] > 0 else 0,
                axis=1
            ).sort_values(ascending=False)

            # --- Display Summary Table (Always shown) ---
            st.markdown(f"##### Cost Summary by {selected_display_dimension} ({selected_year})")
            summary_df_display = summary_df.sort_values(by='Average_Cost_per_FTE', ascending=False).copy()
            
            # Rename columns first for easier use in the Styler
            summary_df_display = summary_df_display.rename(columns={
                dimension_to_group_by: selected_display_dimension,
                'Number_of_FTEs': 'Number of FTEs',
                'Total_Labor_Cost': 'Total Labor Cost',
                'Average_Base_Salary': 'Avg. Base Salary',
                'Average_Cost_per_FTE': 'Avg. Cost per FTE'
            })

            st.dataframe(
                summary_df_display.style.format({
                    'Number of FTEs': '{:,.0f}', 
                    # Apply the currency format function to these three columns
                    'Total Labor Cost': lambda x: apply_currency_format(x),
                    'Avg. Base Salary': lambda x: apply_currency_format(x), 
                    'Avg. Cost per FTE': lambda x: apply_currency_format(x)
                }), 
                use_container_width=True, 
                hide_index=True
            )

            # --- NEW: Conditional Visualization (Treemap or Bar Chart) ---
            if selected_display_dimension == "Location":
                # --- 1. Data Preparation and Dropdown Selector ---
                # We define the data and dropdown first, so the chart can use the selection
                st.markdown(f"##### Overview: Total Labor Cost by Location ({selected_year})")

                location_summary = df_for_comparison.groupby('location').agg(
                    Total_Labor_Cost=('total_individual_cost', 'sum'),
                    Number_of_FTEs=('fte', 'sum')
                ).sort_values(by='Total_Labor_Cost', ascending=False)

                location_list = location_summary.index.tolist()

                # NOTE: The selectbox is now defined before the chart
                selected_location = st.selectbox(
                    "Select a Location to see a detailed breakdown and highlight it in the chart above:",
                    options=[""] + location_list,
                    format_func=lambda x: "Select a Location..." if x == "" else x
                )

                # --- 2. The Overview Chart with Highlighting ---

                # Create a list of colors - one color for the selected location, another for the rest
                highlight_color = '#FFC300' # A bright gold for the highlight
                primary_color = '#0d47a1' # The primary Airbus blue

                colors = [highlight_color if loc == selected_location else primary_color for loc in location_summary.index]

                # We use go.Figure for more control over coloring individual bars
                fig_overview = go.Figure(data=[go.Bar(
                    x=location_summary['Total_Labor_Cost'],
                    y=location_summary.index,
                    orientation='h',
                    text=location_summary['Total_Labor_Cost'],
                    marker_color=colors # Apply the list of colors here
                )])

                fig_overview.update_layout(
                    height=400,
                    yaxis={'categoryorder':'total descending'}, # Keep the ranked order
                    plot_bgcolor='white',
                    title_text='State-by-State Breakdown', 
                    title_x=0.5
                )
                fig_overview.update_traces(texttemplate='%{text:$,.2s}', textposition='outside') # Format text like $1.2M
                fig_overview.update_xaxes(title="Total Labor Cost")
                fig_overview.update_yaxes(title="")
                st.plotly_chart(fig_overview, use_container_width=True)


                # --- 3. The Conditional Detail View (No change to this part) ---
                st.markdown("---")
                if selected_location:
                    st.markdown(f"##### Deep Dive for: **{selected_location}**")
                    # Filter the main dataframe for only the selected location
                    location_detail_df = df_for_comparison[df_for_comparison['location'] == selected_location]

                    # KPI Cards for the selected location
                    loc_total_cost = location_detail_df['total_individual_cost'].sum()
                    loc_total_fte = location_detail_df['fte'].sum()
                    loc_avg_cost_fte = loc_total_cost / loc_total_fte if loc_total_fte > 0 else 0

                    cols = st.columns(3)
                    with cols[0]:
                        st.metric("Total Labor Cost", format_currency(loc_total_cost, selected_currency))
                    with cols[1]:
                        st.metric("Total FTEs", f"{loc_total_fte:,.0f}")
                    with cols[2]:
                        st.metric("Avg. Cost per FTE", format_currency(loc_avg_cost_fte, selected_currency))

                    st.markdown("---")

                    # Detail Charts for the selected location
                    col1, col2 = st.columns([0.6, 0.4])
                    with col1:
                        st.markdown(f"###### Breakdown by Function")
                        function_breakdown = location_detail_df.groupby('Entity/Function').agg(
                            Number_of_FTEs=('fte', 'sum')
                        ).sort_values(by='Number_of_FTEs', ascending=False)

                        fig_func = px.bar(
                            function_breakdown, x=function_breakdown.index, y='Number_of_FTEs',
                            text='Number_of_FTEs', labels={'x': 'Function / Entity'}
                        )
                        fig_func.update_traces(textposition='outside')
                        st.plotly_chart(fig_func, use_container_width=True)
                    with col2:
                        st.markdown(f"###### Blue vs. White Collar")
                        collar_breakdown = location_detail_df['Blue_White_Collar'].value_counts()
                        fig_collar = px.pie(
                            names=collar_breakdown.index, values=collar_breakdown.values, hole=.4,
                            color_discrete_map={'Blue Collar': '#0d47a1', 'White Collar': '#64b5f6'}
                        )
                        st.plotly_chart(fig_collar, use_container_width=True)
                
            else: 
            # --- VERTICAL BAR CHART WITH DYNAMIC AVERAGE LINE ---
                st.markdown(f"##### Average Cost per FTE by {selected_display_dimension} ({selected_year})")

                # --- Step 1: Aggregation (Group Data) ---
                summary_df_enhanced = df_for_comparison.groupby(dimension_to_group_by, as_index=False).agg(
                    Number_of_FTEs=('fte', 'sum'),
                    Total_Labor_Cost=('total_individual_cost', 'sum'),
                    Median_Cost_per_FTE=('total_individual_cost', 'median')
                )
                summary_df_enhanced['Average_Cost_per_FTE'] = (
                    summary_df_enhanced['Total_Labor_Cost'] / summary_df_enhanced['Number_of_FTEs']
                ).where(summary_df_enhanced['Number_of_FTEs'] > 0, 0)

                # --- Step 2: Calculate the TRUE Overall Benchmark Average (FIXED LOGIC) ---
                overall_total_cost_filtered = df_for_comparison['total_individual_cost'].sum()
                overall_total_fte_filtered = df_for_comparison['fte'].sum()
                dynamic_avg_cost_benchmark = overall_total_cost_filtered / overall_total_fte_filtered if overall_total_fte_filtered > 0 else 0
                
                # --- Step 3: Create the Vertical Bar Chart ---
                sorted_summary_df = summary_df_enhanced.sort_values(by='Average_Cost_per_FTE', ascending=False)

                fig_vbar_final = px.bar(
                    sorted_summary_df,
                    x=dimension_to_group_by,
                    y='Average_Cost_per_FTE',
                    color='Average_Cost_per_FTE',
                    color_continuous_scale=px.colors.sequential.Blues_r,
                    text='Average_Cost_per_FTE',
                    custom_data=['Median_Cost_per_FTE', 'Number_of_FTEs', 'Total_Labor_Cost'],
                    labels={
                        dimension_to_group_by: selected_display_dimension,
                        'Average_Cost_per_FTE': f'Average Cost per FTE ({selected_currency})' # Dynamically named axis
                    }
                )
                
                # Dynamic currency prefix for tooltips and axis ticks
                currency_prefix = "€" if selected_currency == "Euro" else "$"

                fig_vbar_final.update_traces(
                    # Use dynamic prefix for bar text display
                    texttemplate=f'{currency_prefix}' + '%{text:,.0f}', textposition='outside', 
                    hovertemplate=(
                        '<b>%{x}</b><br><br>' +
                        'Avg. Cost per Group: <b>' + currency_prefix + '%{y:,.0f}</b><br>' + # Dynamic Prefix
                        'Median Cost within Group: ' + currency_prefix + '%{customdata[0]:,.0f}<br>' + # Dynamic Prefix
                        'Number of FTEs: %{customdata[1]:,}<br>' +
                        'Total Labor Cost: ' + currency_prefix + '%{customdata[2]:,.0f}<extra></extra>' # Dynamic Prefix
                    )
                )

                # --- Step 4: Add the Restyled Average Line (FIXED to use benchmark and currency) ---
                if dynamic_avg_cost_benchmark > 0:
                    fig_vbar_final.add_hline(
                        y=dynamic_avg_cost_benchmark,
                        line_width=1,
                        line_dash="dot",       
                        line_color="green",    
                        # Use the correct benchmark cost and currency formatter in the annotation
                        annotation_text=f"Overall Average: {format_currency(dynamic_avg_cost_benchmark, selected_currency)}", 
                        annotation_position="top right",
                        annotation_font=dict(size=11, color="green")
                    )

                # Clean up layout (FIXED tickprefix for currency)
                fig_vbar_final.update_layout(
                    xaxis_title=None,
                    yaxis_title=f"Average Cost per FTE ({selected_currency})", # Dynamically named axis
                    coloraxis_showscale=False,
                    xaxis_tickangle=-45
                )
                # Apply currency prefix to Y-axis ticks
                fig_vbar_final.update_yaxes(tickprefix=currency_prefix, tickformat=",.0f")

                st.plotly_chart(fig_vbar_final, use_container_width=True)

        # --- Conditional Section for 2025 New Hire Impact ---
        # Display this section only if 2025 is selected and new hires are specified
        # Ensure num_new_hires, selected_hire_dept, selected_band, projected_totals are defined earlier
        if selected_year == "2025 (Projected)" and 'num_new_hires' in globals() and num_new_hires > 0:
            st.markdown("---") # Separator before this section
            # Use the custom highlight style defined in your CSS
            st.markdown('<div class="highlight">', unsafe_allow_html=True)
            st.markdown(f"#### New Hire Impact Analysis ({num_new_hires} Hires)")
            st.markdown(f"**Entity/Function:** {selected_hire_dept}, **Band:** {selected_band}")

            # Check if hiring cost data is available in the projected totals
            if 'projected_totals' in globals() and 'hiring_costs' in projected_totals and projected_totals['hiring_costs'] > 0:
                # Format hiring cost values
                try:
                    hiring_cost_formatted = format_currency(projected_totals['hiring_costs'], selected_currency)
                except NameError:
                    hiring_cost_formatted = f"${projected_totals['hiring_costs']:,.2f}"

                st.metric(label="Estimated Total Hiring Costs", value=hiring_cost_formatted)

                # Calculate and display cost per new hire
                cost_per_hire = projected_totals['hiring_costs'] / num_new_hires
                try:
                    cost_per_hire_formatted = format_currency(cost_per_hire, selected_currency)
                except NameError:
                     cost_per_hire_formatted = f"${cost_per_hire:,.2f}"

                st.metric(label="Estimated Cost per Hire", value=cost_per_hire_formatted)
            else:
                # Display message if hiring cost data is missing or zero
                st.info("Hiring cost data not available or calculated as zero.")
            # Close the highlight div
            st.markdown('</div>', unsafe_allow_html=True)

    # Advanced 2025 Projection Section
    if selected_year == "2025 (Projected)":
        st.markdown("---")
        st.markdown('<div class="sub-header">2025 Projection Details</div>', unsafe_allow_html=True)
    
        # Create two columns
        col1, col2 = st.columns(2)
    
        with col1:
            st.markdown("### New Hire Cost Breakdown")
    
            if num_new_hires > 0:
                # Get averages for the selected department and band
                hire_avg = band_avg_df[(band_avg_df['Entity/Function'] == selected_hire_dept) &
                                       (band_avg_df['band'] == selected_band)].iloc[0]
    
                # Create breakdown of costs per new hire
                new_hire_data = {
                    'Category': [
                        'Base Salary',
                        'Premiums',
                        'Bonuses',
                        'Social Contributions',
                        'LTIPs',
                        'Hiring Cost',
                        'Total'
                    ],
                    'Amount': [
                        hire_avg['avg_base_salary'],
                        hire_avg['avg_work_conditions'] + hire_avg['avg_overtime'] + hire_avg['avg_other_premiums'],
                        hire_avg['avg_annual_bonus'] + hire_avg['avg_profit_sharing'],
                        hire_avg['avg_social_contribution'],
                        hire_avg['avg_ltips'],
                        hire_avg['avg_hiring_cost'],
                        0  # Will calculate total below
                    ]
                }
                new_hire_df = pd.DataFrame(new_hire_data)
    
                # Calculate total
                new_hire_df.loc[6, 'Amount'] = new_hire_df['Amount'].sum()
    
                new_hire_df['Amount'] = new_hire_df['Amount'].apply(lambda x: format_currency(x, selected_currency))
                st.table(new_hire_df)
    
                # Total additional cost
                total_add_cost = hire_avg['avg_base_salary'] + \
                                 hire_avg['avg_work_conditions'] + hire_avg['avg_overtime'] + hire_avg[
                                     'avg_other_premiums'] + \
                                 hire_avg['avg_annual_bonus'] + hire_avg['avg_profit_sharing'] + \
                                 hire_avg['avg_social_contribution'] + \
                                 hire_avg['avg_ltips'] + hire_avg['avg_hiring_cost']
    
                st.markdown(f"**Total Additional Cost for {num_new_hires} New Hires:** {format_currency(total_add_cost * num_new_hires, selected_currency)}")
            else:
                st.markdown("No new hires selected for 2025 projection.")
    
        with col2:
            st.markdown("### Cost Growth Analysis")
    
            # Calculate growth from 2024 to projected 2025
            growth_categories = [
                'Base Salary',
                'Premiums',
                'Bonuses',
                'Social Contributions',
                'LTIPs',
                'Total Cost'
            ]
    
            growth_2024_values = [
                totals_2024['total_base_salary'],
                totals_2024['total_premiums'],
                totals_2024['total_bonuses'],
                totals_2024['total_social_contributions'],
                totals_2024['total_ltips'],
                totals_2024['total_cost']
            ]
    
            growth_2025_values = [
                projected_totals['total_base_salary'],
                projected_totals['total_premiums'],
                projected_totals['total_bonuses'],
                projected_totals['total_social_contributions'],
                projected_totals['total_ltips'],
                projected_totals['total_cost']
            ]
    
            growth_rates = [
                ((g2025 - g2024) / g2024 * 100).round(2)
                for g2025, g2024 in zip(growth_2025_values, growth_2024_values)
            ]
    
            # Create growth chart
            growth_df = pd.DataFrame({
                'Category': growth_categories,
                'Growth Rate (%)': growth_rates
            })
    
            fig = px.bar(
                growth_df,
                x='Category',
                y='Growth Rate (%)',
                title='Projected 2024-2025 Growth Rates',
                color='Growth Rate (%)',
                color_continuous_scale='Blues',
                text='Growth Rate (%)'
            )
    
            fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    
            st.plotly_chart(fig, use_container_width=True)
    
        # Add what-if scenario analysis section
        st.markdown("### What-If Scenario Analysis")
    
        # Create a range of potential new hire counts
        scenario_hires = list(range(0, 21, 5))
        if num_new_hires > 0 and num_new_hires not in scenario_hires:
            scenario_hires.append(num_new_hires)
            scenario_hires.sort()
    
        # Calculate scenarios
        base_totals_for_scenario = dept_totals_2024.copy()
        scenario_data = []
    
        for hire_count in scenario_hires:
            scenario = project_costs_for_new_hires(
                base_totals_for_scenario, 
                band_avg_df,
                selected_hire_dept,
                selected_band,
                selected_hire_location, 
                hire_count
            )
        
            row = {
                'New Hires': hire_count,
                'Total Cost': scenario['total_cost'],
                'Cost Increase': 0, 
                'Percent Increase': 0
            }
        
            scenario_data.append(row)
    
        scenario_df = pd.DataFrame(scenario_data)
        cost_at_zero_hires = scenario_df.loc[scenario_df['New Hires'] == 0, 'Total Cost'].iloc[0]
        scenario_df['Cost Increase'] = scenario_df['Total Cost'] - cost_at_zero_hires
        scenario_df['Percent Increase'] = (scenario_df['Cost Increase'] / cost_at_zero_hires * 100).round(2)
    
        # Create scenario chart
        fig = px.line(
            scenario_df,
            x='New Hires',
            y='Total Cost',
            title=f'Cost Projection Scenarios for {selected_hire_dept} {selected_band} Hires',
            markers=True
        )
    
        # Add second y-axis for percentage increase
        fig.add_trace(
            go.Scatter(
                x=scenario_df['New Hires'],
                y=scenario_df['Percent Increase'],
                name='Percent Increase',
                yaxis='y2',
                line=dict(color='red', dash='dot'),
                mode='lines+markers'
            )
        )
    
        # Update layout for dual y-axes
        fig.update_layout(
            yaxis=dict(title='Total Cost ($)'),
            yaxis2=dict(title='Percent Increase (%)', overlaying='y', side='right'),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
        )
    
        # Add dollar sign formatting to primary y-axis
        fig.update_yaxes(tickprefix="$", tickformat=",", secondary_y=False)
        fig.update_yaxes(ticksuffix="%", secondary_y=True)
    
        st.plotly_chart(fig, use_container_width=True)
    
        # Display scenario table
        scenario_df['Total Cost'] = scenario_df['Total Cost'].apply(lambda x: format_currency(x, selected_currency))
        scenario_df['Cost Increase'] = scenario_df['Cost Increase'].apply(lambda x: format_currency(x, selected_currency))
        scenario_df['Percent Increase'] = scenario_df['Percent Increase'].apply(lambda x: f"{x}%")
    
        st.table(scenario_df)

        st.markdown("---")
        st.markdown('<div class="sub-header" style="font-size: 1.5rem; margin-top:1.5rem;">Monthly Cost Projection for 2025</div>', unsafe_allow_html=True)

        # Assumption: df_2024_filtered is the correct DataFrame for 2024 actuals for the current sidebar selection.
        # IMPORTANT: Replace 'your_date_column_in_df_2024' with the actual name of your date column.
        # If your df_2024_filtered doesn't have individual records with dates, this will not work.
        monthly_costs_2024_actual = get_monthly_actual_costs(df_2024_filtered.copy(), 2024, date_column_name='pay_date') # ADJUST 'pay_date'

        if monthly_costs_2024_actual.sum() == 0 and 'pay_date' in df_2024_filtered.columns : # Check if sum is zero even if column exists
            st.warning("2024 actual monthly costs are zero. Ensure 'pay_date' column is correct and data exists for 2024. 2025 monthly projection will be based on new hires only or distributed annual total.")
            # Fallback: create a zero series for 2024 actuals if needed for calculations below, or handle this case.
            # For now, if this happens, the 2025 projected cost will be mainly the new hire impact or distributed annual.

        # Calculate total *annualized* cost of all new hires for 2025
        cost_one_new_hire_annual = 0
        if num_new_hires > 0 and not band_avg_df.empty: # Ensure band_avg_df is loaded
            avg_data_for_hire = band_avg_df[
                (band_avg_df['Entity/Function'] == selected_hire_dept) &
                (band_avg_df['band'] == selected_band)
            ]
            if not avg_data_for_hire.empty:
                avg = avg_data_for_hire.iloc[0]
                cost_one_new_hire_annual = ( # Sum of all average cost components for one new hire
                    avg.get('avg_base_salary', 0) + avg.get('avg_work_conditions', 0) +
                    avg.get('avg_overtime', 0) + avg.get('avg_other_premiums', 0) +
                    avg.get('avg_annual_bonus', 0) + avg.get('avg_profit_sharing', 0) +
                    avg.get('avg_social_contribution', 0) + 
                    avg.get('avg_ltips', 0)
                )
        total_annual_cost_new_hires = cost_one_new_hire_annual * num_new_hires
        monthly_incremental_cost_all_new_hires = total_annual_cost_new_hires / 12.0 if num_new_hires > 0 else 0.0

        # Create 2025 projected monthly costs DataFrame
        # Start with 2024 actuals as the base for each month
        projected_monthly_df = pd.DataFrame({
            'Month_Num': range(1, 13),
            '2024_Actual_Cost': monthly_costs_2024_actual.values # Uses values from the reindexed Series
        })
        projected_monthly_df['Month'] = projected_monthly_df['Month_Num'].apply(lambda m: month_names_dict[m])

        projected_monthly_df['New_Hire_Monthly_Impact'] = 0.0
        if num_new_hires > 0:
            # Apply new hire cost from their start month onwards
            # The 'selected_hire_start_month' comes from st.session_state or direct from sidebar widget
            start_month_val = st.session_state.get('selected_hire_start_month', 1) # Get from session_state
            projected_monthly_df.loc[projected_monthly_df['Month_Num'] >= start_month_val, 'New_Hire_Monthly_Impact'] = monthly_incremental_cost_all_new_hires

        # Projected 2025 cost is 2024 actual + new hire impact for that month
        projected_monthly_df['2025_Projected_Monthly_Cost'] = projected_monthly_df['2024_Actual_Cost'] + projected_monthly_df['New_Hire_Monthly_Impact']

        st.markdown("##### Table: Projected Monthly Labor Costs for 2025")
        display_monthly_table_df = projected_monthly_df[['Month', '2024_Actual_Cost', 'New_Hire_Monthly_Impact', '2025_Projected_Monthly_Cost']].copy()
        for col_to_format in ['2024_Actual_Cost', 'New_Hire_Monthly_Impact', '2025_Projected_Monthly_Cost']:
            display_monthly_table_df[col_to_format] = display_monthly_table_df[col_to_format].apply(lambda x: format_currency(x, selected_currency) if pd.notna(x) else "$0.00")
        st.dataframe(display_monthly_table_df, use_container_width=True, hide_index=True)

        # Display Line Chart for Monthly Trend
        st.markdown("##### Chart:Monthly Labor Cost Trend (2024 Actual vs. 2025 Projected)")
        plot_melted_monthly_df = projected_monthly_df.melt(
            id_vars=['Month_Num', 'Month'],
            value_vars=['2024_Actual_Cost', '2025_Projected_Monthly_Cost'],
            var_name='Scenario',
            value_name='Cost'
        ).sort_values(by='Month_Num') # Ensure correct month order for line chart

        # Rename scenarios for legend
        plot_melted_monthly_df['Scenario'] = plot_melted_monthly_df['Scenario'].replace({
            '2024_Actual_Cost': '2024 Actual',
            '2025_Projected_Monthly_Cost': '2025 Projected'
        })

        fig_monthly_trend_2025 = px.line(
            plot_melted_monthly_df,
            x='Month',
            y='Cost',
            color='Scenario',
            title='Monthly Labor Cost: 2024 Actual vs. 2025 Projected',
            labels={'Cost': 'Total Labor Cost ($)', 'Month': 'Month'},
            markers=True,
            # Ensure month order is correct if 'Month' (name) is used for x-axis
            category_orders={"Month": [month_names_dict[m] for m in range(1,13)]}
        )
        fig_monthly_trend_2025.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_monthly_trend_2025, use_container_width=True)

        # Note on annual vs. monthly sum for 2025
        sum_of_2025_monthly_projections = projected_monthly_df['2025_Projected_Monthly_Cost'].sum()
        # 'projected_totals' is the dictionary holding the overall annual projection
        annual_projection_total_cost = projected_totals.get('total_cost', 0)

        start_month_val = st.session_state.get('selected_hire_start_month', 1) # Get from session_state
        st.info(f"""
        **Understanding the 2025 Monthly Projection:**
        The chart and table above show a 2025 monthly labor cost projection where the impact of {num_new_hires} new hire(s) is phased in starting from **{month_names_dict[start_month_val]} {selected_year}**.
        - Sum of these 2025 monthly projected costs: **{format_currency(sum_of_2025_monthly_projections, selected_currency)}**.
        - The main dashboard's annual 2025 projection: **{format_currency(annual_projection_total_cost, selected_currency)}**.
        The annual projection typically represents the full-year cost impact assuming new hires are on board for an *annualized equivalent period* for budgeting and overall impact assessment. The monthly view provides a more granular look at how costs might phase in during the year 2025.
        If all new hires were to start in January, the sum of monthly costs would more closely align with the annualized figure (plus any underlying 2024 costs).
        """)

# Tab 3: Year-over-Year Overall Cost Comparison
with tab3:
    # Add the sub-header for this tab
    st.markdown('<div class="sub-header">Year-over-Year Overall Cost Comparison</div>', unsafe_allow_html=True)

    # --- Helper Functions Defined within Tab 2 Scope ---

    # Helper function for safe division (to handle potential zero denominators)
    def safe_division(numerator, denominator):
        """Performs division, returning 'inf' or 0.0 if denominator is zero."""
        # Check if denominator is zero
        if denominator == 0:
            # If numerator is also 0, result is 0. Otherwise, infinite growth.
            return 0.0 if numerator == 0 else float('inf')
        # Calculate percentage growth: (new - old) / old * 100
        return (numerator - denominator) / denominator * 100

    # Helper function to determine the text color for styling the dataframe
    def color_growth(val):
        """Returns CSS style string for text color based on value."""
        # Check for NaN or infinite values
        if pd.isna(val) or val == float('inf'):
            color = 'grey' # Neutral color for N/A or infinite growth
        elif val > 0:
            color = 'green' # Green for positive growth
        elif val < 0:
            color = 'red'   # Red for negative growth
        else: # val == 0
            color = 'grey'  # Neutral color for zero growth (or 'black')
        # Return the CSS style string
        return f'color: {color}'

    # Helper function to format the growth number into the desired display string for the dataframe
    def format_growth_display(val):
        """Formats number into string with arrow and percentage."""
        # Handle NaN values
        if pd.isna(val):
            return "N/A"
        # Handle infinite growth (from zero base)
        if val == float('inf'):
            return "N/A (from zero)"
        # Format positive growth
        elif val > 0:
            return f"▲ {val:.1f}%" # Up arrow, value, % sign
        # Format negative growth
        elif val < 0:
            # Down arrow, absolute value (to avoid double negative), % sign
            return f"▼ {abs(val):.1f}%"
        # Format zero growth
        else: # val == 0
            return f"▬ {val:.1f}%" # Neutral symbol, value, % sign

    # --- Data Preparation ---

    # Select the correct data source based on the Entity/Function filter in the sidebar
    if selected_dept != "All Entity/Function":
        # Use totals calculated for the specific selected Entity/Function
        compare_2023 = dept_totals_2023
        compare_2024 = dept_totals_2024
    else:
        # Use the overall totals calculated from the full dataset
        compare_2023 = totals_2023
        compare_2024 = totals_2024

    # Calculate 2023-2024 growth rates for each category using the safe_division helper
    growth_base = safe_division(compare_2024['total_base_salary'], compare_2023['total_base_salary'])
    growth_work_condition_premiums = safe_division(compare_2024['total_work_conditions_premium_cost'], compare_2023['total_work_conditions_premium_cost'])
    growth_overtime = safe_division(compare_2024['total_overtime_premium_cost'], compare_2023['total_overtime_premium_cost'])
    growth_bonuses = safe_division(compare_2024['total_bonuses'], compare_2023['total_bonuses'])
    growth_profit_sharing = safe_division(compare_2024['total_profit_sharing'], compare_2023['total_profit_sharing'])
    growth_other_premiums = safe_division(compare_2024['total_other_specific_premiums_cost'], compare_2023['total_other_specific_premiums_cost'])
    growth_social = safe_division(compare_2024['total_social_contributions'], compare_2023['total_social_contributions'])
    growth_ltips = safe_division(compare_2024['total_ltips'], compare_2023['total_ltips'])
    growth_total = safe_division(compare_2024['total_cost'], compare_2023['total_cost'])

    # Round valid growth rates to one decimal place (infinite values remain infinite)
    growth_base = round(growth_base, 1) if growth_base != float('inf') else float('inf')
    growth_work_condition_premiums = round(growth_work_condition_premiums, 1) if growth_work_condition_premiums != float('inf') else float('inf')
    growth_overtime = round(growth_overtime, 1) if growth_overtime != float('inf') else float('inf')
    growth_bonuses = round(growth_bonuses, 1) if growth_bonuses != float('inf') else float('inf')
    growth_profit_sharing = round(growth_profit_sharing, 1) if growth_profit_sharing != float('inf') else float('inf')
    growth_other_premiums = round(growth_other_premiums, 1) if growth_other_premiums != float('inf') else float('inf')
    growth_social = round(growth_social, 1) if growth_social != float('inf') else float('inf')
    growth_ltips = round(growth_ltips, 1) if growth_ltips != float('inf') else float('inf')
    growth_total = round(growth_total, 1) if growth_total != float('inf') else float('inf')

    # Prepare lists of categories and values for the chart and table
    categories = ['Base Salary', 'Premiums Work Conditions', 'Overtime', 'Annual Bonuses', 'Profit Sharing', 'Other Premiums', 'Social Contribution', 'LTIPs', 'Total Cost']
    year_2023_values = [
        compare_2023['total_base_salary'], 
        compare_2023['total_work_conditions_premium_cost'], 
        compare_2023['total_overtime_premium_cost'],
        compare_2023['total_bonuses'],
        compare_2023['total_profit_sharing'],
        compare_2023['total_other_specific_premiums_cost'],
        compare_2023['total_social_contributions'], 
        compare_2023['total_ltips'], 
        compare_2023['total_cost']
    ]
    year_2024_values = [
        compare_2024['total_base_salary'], 
        compare_2024['total_work_conditions_premium_cost'], 
        compare_2024['total_overtime_premium_cost'],
        compare_2024['total_bonuses'],
        compare_2024['total_profit_sharing'],
        compare_2024['total_other_specific_premiums_cost'],
        compare_2024['total_social_contributions'], 
        compare_2024['total_ltips'], 
        compare_2024['total_cost']
    ]
    # List of calculated (and rounded) growth rates
    growth_rates = [
        growth_base, growth_work_condition_premiums, growth_overtime,
        growth_bonuses, growth_profit_sharing, growth_other_premiums, 
        growth_social, growth_ltips, growth_total
    ]

    # --- Chart Creation with Outlines and Text Annotations ---
    fig = go.Figure()

    # Add 2023 bars
    fig.add_trace(go.Bar(
        x=categories,
        y=year_2023_values,
        name='2023',
        marker_color='#93C5FD' # Light Blue
    ))

    # Prepare lists for conditional styling of 2024 bars and annotations
    bar_outline_colors = [] # List to hold outline color for each 2024 bar
    bar_outline_widths = [] # List to hold outline width for each 2024 bar
    annotation_texts = []   # List to hold the text for annotations (e.g., "+2.3%")
    annotation_colors = []  # List to hold the color for annotation text

    # Loop through the 2023-2024 growth rates to determine styling for each category
    for growth in growth_rates:
        if growth > 0 and growth != float('inf'):
            # Positive growth styling
            bar_outline_colors.append('green')
            bar_outline_widths.append(2) # Set outline width
            annotation_texts.append(f"+{growth:.1f}%") # Add '+' sign for clarity
            annotation_colors.append('green')
        elif growth < 0:
            # Negative growth styling
            bar_outline_colors.append('red')
            bar_outline_widths.append(2) # Set outline width
            annotation_texts.append(f"{growth:.1f}%") # Negative sign is inherent
            annotation_colors.append('red')
        else:
            # Zero or infinite growth styling (neutral)
            bar_outline_colors.append('#2563EB') # Use the bar's fill color (or grey)
            bar_outline_widths.append(0) # No distinct outline
            annotation_texts.append(" ") # Empty text for no annotation
            annotation_colors.append('grey') # Neutral color

    # Add 2024 bars with the prepared conditional outlines
    fig.add_trace(go.Bar(
        x=categories,
        y=year_2024_values,
        name='2024',
        marker_color='#2563EB', # Medium Blue fill color
        marker_line_color=bar_outline_colors, # Apply the list of outline colors
        marker_line_width=bar_outline_widths  # Apply the list of outline widths
    ))

    # Add 2025 projected bars if that year is selected in the sidebar
    if selected_year == "2025 (Projected)":
        # 'current_totals' holds the correctly calculated/filtered projected data
        proj_data_source = current_totals
        # Get the projected values for the chart
        year_2025_values_chart = [
            proj_data_source['total_base_salary'], 
            proj_data_source['total_work_conditions_premium_cost'], 
            proj_data_source['total_overtime_premium_cost'],
            proj_data_source['total_bonuses'],
            proj_data_source['total_profit_sharing'],
            proj_data_source['total_other_specific_premiums_cost'],
            proj_data_source['total_social_contributions'], 
            proj_data_source['total_ltips'], 
            proj_data_source['total_cost']
        ]
        # Add the 2025 bar trace
        fig.add_trace(go.Bar(
            x=categories,
            y=year_2025_values_chart,
            name='2025 (Projected)',
            marker_color='#1E3A8A' # Dark Blue
            # No special outline needed unless comparing 2024 vs 2025
        ))

    # Add text annotations (percentage change) above the 2024 bars
    for i, category in enumerate(categories):
        max_height = max(year_2023_values[i], year_2024_values[i])
        text = annotation_texts[i]    
        color = annotation_colors[i]  

        if text.strip():
            fig.add_annotation(
                x=category, 
                y=max_height, 
                text=text, 
                showarrow=False, 
                font=dict(
                    color=color,
                    size=11
                ),
                yshift=15 
            )

    # Configure the overall chart layout
    fig.update_layout(
        title="Comparative Cost Analysis: 2023 vs. 2024 Trends", # Informative title
        xaxis_title="Cost Category",
        yaxis_title="Amount ($)",
        legend_title="Year",
        barmode='group', # Group bars by category
        height=550,      # Set chart height
        margin=dict(t=90, b=40, l=40, r=40) # Adjust top margin for annotations
    )

    # Format the Y-axis ticks as currency ($)
    currency_prefix = "€" if selected_currency == "Euro" else "$"
    fig.update_yaxes(tickprefix=currency_prefix, tickformat=",")

    # Display the Plotly chart in the Streamlit app
    st.plotly_chart(fig, use_container_width=True)

    # --- Display Growth Rates Table using st.dataframe with Styling ---

    # Create the dictionary for the DataFrame - STORE RAW NUMBERS for styling
    growth_data_raw = {
        'Category': categories,
        '2023 Amount': year_2023_values, # Store raw numbers
        '2024 Amount': year_2024_values, # Store raw numbers
        '2023-2024 Growth': growth_rates # Store raw numerical growth rates
    }

    # Add 2025 columns to the raw data dictionary if applicable
    if selected_year == "2025 (Projected)":
        # Use the same projection data source as the chart
        proj_data_source = current_totals
        # Get raw values for the table
        year_2025_values_table = [
            proj_data_source['total_base_salary'], 
            proj_data_source['total_work_conditions_premium_cost'], 
            proj_data_source['total_overtime_premium_cost'],
            proj_data_source['total_bonuses'],
            proj_data_source['total_profit_sharing'],
            proj_data_source['total_other_specific_premiums_cost'],
            proj_data_source['total_social_contributions'], 
            proj_data_source['total_ltips'], 
            proj_data_source['total_cost']
        ]
        # Calculate 2024-2025 growth rates using the correct projected data
        keys_for_growth = ['total_base_salary', 'total_work_conditions_premium_cost', 'total_overtime_premium_cost',
                           'total_bonuses', 'total_profit_sharing', 'total_other_specific_premiums_cost',
                          'total_social_contributions', 'total_ltips', 'total_cost']
        growth_2025 = []
        for key in keys_for_growth:
            # Use compare_2024 as the base for 2024->2025 growth calculation
            rate_2025 = safe_division(proj_data_source[key], compare_2024[key])
            growth_2025.append(round(rate_2025, 1) if rate_2025 != float('inf') else float('inf'))

        # Add raw 2025 data and growth to the dictionary
        growth_data_raw['2025 Amount (Projected)'] = year_2025_values_table
        growth_data_raw['2024-2025 Growth (Projected)'] = growth_2025

    # Create the Pandas DataFrame from the raw data dictionary
    growth_df_raw = pd.DataFrame(growth_data_raw)

    # Define columns to apply specific styling/formatting
    amount_cols = [col for col in growth_df_raw.columns if 'Amount' in col]
    growth_cols = [col for col in growth_df_raw.columns if 'Growth' in col]

    
    # Apply styling and formatting using Pandas Styler
    styled_df = growth_df_raw.style \
        .format(apply_currency_format, subset=amount_cols) \
        .format(format_growth_display, subset=growth_cols) \
        .apply(lambda x: x.map(color_growth), subset=growth_cols)

    st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    ## --- BEGINNING OF REVAMPED Year-over-Year Hours & Productivity Analysis SECTION ---
    st.markdown("---")
    st.markdown('<div class="sub-header" style="font-size: 1.6rem;">Year-over-Year Hours & Productivity Analysis</div>', unsafe_allow_html=True)
    st.markdown(
        "Analyze trends in hours and productivity. Use the toggles below to switch between Total Hours and Hours per FTE, or to drill down into a specific location."
    )

    # --- NEW: Radio button to toggle between views ---
    analysis_mode = st.radio(
        "Select Analysis View:",
        ["Hours per FTE", "Total Hours"],
        horizontal=True,
        key="analysis_mode_toggle"
    )

    # --- DYNAMIC DATA PREPARATION WITH LOCATION FILTER ---
    df_2023_analysis = df_2023_filtered
    df_2024_analysis = df_2024_filtered

    # This 'if' block was moved from the original code to handle 2025 projection case cleanly
    if selected_year != "2025 (Projected)":
        locations_list = sorted(list(set(df_2023_analysis['location'].unique()) | set(df_2024_analysis['location'].unique())))
        selected_location = st.selectbox(
            "Filter Analysis by Location",
            ["All Locations"] + locations_list,
            key="location_filter_yoy_hours"
        )
        if selected_location != "All Locations":
            df_2023_analysis = df_2023_analysis[df_2023_analysis['location'] == selected_location]
            df_2024_analysis = df_2024_analysis[df_2024_analysis['location'] == selected_location]
    else:
        st.info("The location filter is disabled when viewing 2025 projections, as the projection model is not location-specific.")

    section_totals_2023 = calculate_totals(df_2023_analysis)
    section_totals_2024 = calculate_totals(df_2024_analysis)

    # --- REFACTORED: Data calculation function now handles both modes ---
    def get_hour_analysis_data(totals_dict, mode='Hours per FTE'):
        fte = totals_dict.get('total_fte', 0)
        divisor = 1.0
        if mode == 'Hours per FTE' and fte > 0:
            divisor = fte

        data_dict = {"Total FTEs": fte}
        if divisor == 0:
            for metric in ["Planned Hours", "Overtime Hours", "Total Absence Hours", "Sick Hours", "Holiday Hours", "Other Absences", "Attendance Time", "Productivity Rate (%)", "Absence Rate (vs Planned) (%)"]: data_dict[metric] = 0.0
            return data_dict

        planned_total = totals_dict.get('total_planned_hours', 0)
        overtime_total = totals_dict.get('total_overtime_hours', 0)
        absence_total = totals_dict.get('total_all_absence_hours', 0)
        attendance_total = planned_total + overtime_total - absence_total

        data_dict.update({
            "Planned Hours": planned_total / divisor, "Overtime Hours": overtime_total / divisor,
            "Sick Hours": totals_dict.get('total_sick_hours', 0) / divisor, "Holiday Hours": totals_dict.get('total_holiday_hours', 0) / divisor,
            "Other Absences": totals_dict.get('total_other_absences_hours', 0) / divisor, "Total Absence Hours": absence_total / divisor,
            "Attendance Time": attendance_total / divisor,
            "Productivity Rate (%)": (attendance_total / (planned_total + overtime_total) * 100) if (planned_total + overtime_total) > 0 else 0,
            "Absence Rate (vs Planned) (%)": (absence_total / planned_total * 100) if planned_total > 0 else 0
        })
        return data_dict

    hours_data_2023 = get_hour_analysis_data(section_totals_2023, mode=analysis_mode)
    hours_data_2024 = get_hour_analysis_data(section_totals_2024, mode=analysis_mode)
    
    # --- FIX: Define main/compare variables here, before any visuals use them ---
    if selected_year == "2023":
        main_label, compare_label, main_data, compare_data = "2023", "2024", hours_data_2023, hours_data_2024
    else: # Default to 2024 vs 2023 for "2024" and "2025" selections
        main_label, compare_label, main_data, compare_data = "2024", "2023", hours_data_2024, hours_data_2023
    
    # --- UPDATED: The breakdown table ---
    st.markdown(f"##### Hours & Productivity Metrics ({analysis_mode})")
    metric_definitions_ordered = [
        ("Total FTEs", "Total FTEs"), 
        ("Planned Hours", "Planned Hours"), 
        ("Overtime Hours", "(+) Overtime Hours"), # Indented
        ("Total Absence Hours", "(-) Total Absence Hours"), # Indented
        ("Sick Hours", "      Sick Hours"),
        ("Holiday Hours", "      Holiday Hours"),
        ("Other Absences", "      Other Absences"),
        ("Attendance Time", "(=) Attendance Time"), 
        ("Productivity Rate (%)", "Productivity Rate (%)"),
        ("Absence Rate (vs Planned) (%)", "Absence Rate (vs Planned) (%)")
    ]
    
    numeric_rows = [{"Metric": name, main_label: main_data.get(key, 0), compare_label: compare_data.get(key, 0)} for key, name in metric_definitions_ordered]
    numeric_df = pd.DataFrame(numeric_rows)
    numeric_df['Growth'] = numeric_df.apply(lambda row: safe_division(row[main_label], row[compare_label]), axis=1)

    display_df = numeric_df.copy()
    for idx, row in display_df.iterrows():
        metric_name = row['Metric']
        for col in [main_label, compare_label]:
            value = row[col]
            
            if pd.isna(value):
                display_df.loc[idx, col] = "N/A"
            elif metric_name == "Total FTEs":
                # Rule 1: FTEs are always a whole number
                display_df.loc[idx, col] = f"{value:,.0f}"
            elif "(%)" in metric_name:
                # Rule 2: Percentage-based rates get one decimal and a '%' sign
                display_df.loc[idx, col] = f"{value:,.1f}%"
            else:
                # Rule 3: All other metrics (the hour values) get one decimal place
                display_df.loc[idx, col] = f"{value:,.1f}"
    
    display_df['Growth'] = numeric_df['Growth'].apply(format_growth_display)
    
    def color_metric_column(val):
        style = ''
        val_stripped = val.replace("&nbsp;", "").strip() # Remove spaces for matching

        # Group the calculation rows with a single background color
        calculation_rows = ["Planned Hours", "(+) Overtime Hours", "(-) Total Absence Hours", "(=) Attendance Time"]
        if any(row in val_stripped for row in calculation_rows):
             style += 'background-color: #EFF6FF;' # Airbus Light Blue background

        # Apply specific left borders based on the row's role
        if val_stripped == "Planned Hours":
            style += 'border-left: 3px solid #64B5F6;' # Medium Blue
        elif val_stripped == "(+) Overtime Hours":
            style += 'border-left: 3px solid #4CAF50;' # Green for addition
        elif val_stripped == "(-) Total Absence Hours":
            style += 'border-left: 3px solid #FFC107;' # Amber for subtraction
        elif val_stripped == "(=) Attendance Time":
            style += 'border-left: 3px solid #00205B; font-weight: bold;' # Airbus Dark Blue for total
        
        # Style for the KPI rows
        elif "(%)" in val_stripped or "Total FTEs" in val_stripped:
            style = 'background-color: #F5F5F5; font-weight: bold;' # Grey for KPIs
            
        return style

    styled_table = display_df.style.applymap(color_metric_column, subset=['Metric']).apply(lambda s: numeric_df['Growth'].map(color_growth), subset=['Growth'])
    st.dataframe(styled_table, use_container_width=True, hide_index=True)

    # --- NEW: Waterfall chart ---
    st.markdown("---")
    st.markdown(f"###### {main_label} Hour Flow ({analysis_mode})")
    fig_waterfall = go.Figure(go.Waterfall(
        name=f"{main_label} Hour Flow", orientation="v", measure=["absolute", "relative", "relative", "total"],
        x=["Planned", "+ Overtime", "- Absences", "Attendance Time"], textposition="outside",
        text=[f"{v:,.0f}" for v in [main_data.get("Planned Hours", 0), main_data.get("Overtime Hours", 0), main_data.get("Total Absence Hours", 0), main_data.get("Attendance Time", 0)]],
        y=[main_data.get("Planned Hours", 0), main_data.get("Overtime Hours", 0), -main_data.get("Total Absence Hours", 0), main_data.get("Attendance Time", 0)],
        connector={"line": {"color": "rgb(63, 63, 63)"}}, 
        increasing = {"marker":{"color":"#2e7d32"}}, # Green for OT
        decreasing = {"marker":{"color":"#c62828"}}, # Red for negative variance
        totals = {"marker":{"color":"#0d47a1"}} 
    ))
    fig_waterfall.update_layout(showlegend=False, height=450, yaxis_title="Hours")
    st.plotly_chart(fig_waterfall, use_container_width=True)
    
    # --- UPDATED: Bullet chart function ---
    # --- FINAL: Bullet chart function with nuanced color scheme ---
    def create_bullet_indicator(title, main_value, compare_value, main_label, compare_label, is_percent=False, higher_is_better=True, is_total_hours=False):
        """
        Creates a bullet chart with a nuanced color scheme.
        - higher_is_better: True, False, or None for a neutral color.
        """
        change = main_value - compare_value
        pct_change = (change / compare_value * 100) if compare_value != 0 else float('inf')

        # 1. Determine the SYMBOL based on mathematical direction
        if main_value > compare_value: symbol = '▲'
        elif main_value < compare_value: symbol = '▼'
        else: symbol = '▬'

        # 2. Define the nuanced color palette
        good_increase_color = '#2e7d32'   # Dark Green
        good_decrease_color = '#4CAF50'   # Light Green
        bad_increase_color = '#F44336'    # Light Red
        bad_decrease_color = '#c62828'    # Dark Red
        neutral_color = '#1E88E5'         # Blue
        no_change_color = '#37474F'       # Grey

        # 3. Determine the COLOR using nuanced logic
        if main_value == compare_value:
            bar_color = no_change_color
        elif higher_is_better is None:
            bar_color = neutral_color
        else:
            # Check the direction of change and combine with the business rule
            if main_value > compare_value:  # The value increased
                bar_color = good_increase_color if higher_is_better else bad_increase_color
            else:  # The value decreased
                bar_color = good_decrease_color if not higher_is_better else bad_decrease_color

        # (The rest of the function remains the same)
        num_format_str = ",.0f" if is_total_hours else ",.1f"
        fig = go.Figure(go.Bar(
            x=[title], y=[main_value], marker_color=bar_color, width=0.4, name=main_label,
            text=f"<b>{main_value:{num_format_str}}{'%' if is_percent else ''}</b>",
            textposition='outside', textfont=dict(size=16)
        ))
        fig.add_shape(type='line', x0=-0.4, y0=compare_value, x1=0.4, y1=compare_value, line=dict(color='black', width=2, dash='dot'))
        fig.add_annotation(x=0.45, y=compare_value, text=f"{compare_label}: {compare_value:{num_format_str}}", 
                           showarrow=False, xanchor='left', font=dict(size=11, color='black'))
        if compare_value != 0 and main_value != compare_value:
            fig.add_annotation(x=title, y=main_value, yshift=35, 
                               text=f"<b>{symbol} {abs(pct_change):.1f}% vs {compare_label}</b>", 
                               showarrow=False, font=dict(color=bar_color, size=14))
        max_y = max(main_value, compare_value)
        fig.update_layout(
            title={'text': f"<b>{title}</b>", 'y':0.95, 'x':0.5, 'xanchor': 'center', 'yanchor': 'top'},
            showlegend=False, plot_bgcolor='white', xaxis_visible=False,
            yaxis=dict(range=[0, max_y * 1.5]), height=350, margin=dict(t=80, b=20, l=20, r=20)
        )
        return fig

    # --- UPDATED: Bullet chart display ---
    st.markdown("---")
    st.markdown(f"###### Key Hour Components ({analysis_mode})")
    is_total_mode = (analysis_mode == "Total Hours")
    h_col1, h_col2, h_col3, h_col4 = st.columns(4)
    with h_col1:
        fig = create_bullet_indicator("Planned Hours", main_data["Planned Hours"], compare_data["Planned Hours"], main_label, compare_label, is_total_hours=is_total_mode)
        st.plotly_chart(fig, use_container_width=True)
    with h_col2:
        fig = create_bullet_indicator("Overtime Hours", main_data["Overtime Hours"], compare_data["Overtime Hours"], main_label, compare_label, higher_is_better=False, is_total_hours=is_total_mode)
        st.plotly_chart(fig, use_container_width=True)
    with h_col3:
        fig = create_bullet_indicator("Absence Hours", main_data["Total Absence Hours"], compare_data["Total Absence Hours"], main_label, compare_label, higher_is_better=False, is_total_hours=is_total_mode)
        st.plotly_chart(fig, use_container_width=True)
    with h_col4:
        fig = create_bullet_indicator("Attendance Time", main_data["Attendance Time"], compare_data["Attendance Time"], main_label, compare_label, is_total_hours=is_total_mode)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("###### Key Performance Indicators")
    k_col1, k_col2 = st.columns(2)
    with k_col1:
        fig = create_bullet_indicator("Productivity Rate", main_data["Productivity Rate (%)"], compare_data["Productivity Rate (%)"], main_label, compare_label, is_percent=True, higher_is_better=True)
        st.plotly_chart(fig, use_container_width=True)
    with k_col2:
        fig = create_bullet_indicator("Absence Rate", main_data["Absence Rate (vs Planned) (%)"], compare_data["Absence Rate (vs Planned) (%)"], main_label, compare_label, is_percent=True, higher_is_better=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # --- BEGINNING OF NEW Year-over-Year Overtime and Premiums Analysis SECTION ---
    st.markdown("---") # Separator
    st.markdown('<div class="sub-header" style="font-size: 1.6rem;">Overtime & Premiums Analysis</div>', unsafe_allow_html=True)
    st.markdown(
        "This section analyzes the key drivers of variable labor costs, focusing on the composition of premium payments and the relationship between overtime hours and their associated costs."
    )

    # --- 1. Data Preparation ---
    # We'll redefine the data-gathering function to be more focused.
    def get_premium_analysis_data(totals_dict, year_label):

        total_premiums = totals_dict.get('total_premiums', 0)
        total_cost = totals_dict.get('total_cost', 0)
        ot_cost = totals_dict.get('total_overtime_premium_cost', 0)
        wc_cost = totals_dict.get('total_work_conditions_premium_cost', 0)
        other_cost = totals_dict.get('total_other_specific_premiums_cost', 0)
        ot_hours = totals_dict.get('total_overtime_hours', 0)

        data = {
            "year": year_label,
            "total_premiums": total_premiums,
            "premiums_as_pct_of_total_cost": (total_premiums / total_cost * 100) if total_cost > 0 else 0,
            "cost_per_ot_hour": (ot_cost / ot_hours) if ot_hours > 0 else 0,
            "ot_cost": ot_cost,
            "wc_cost": wc_cost,
            "other_cost": other_cost,
            "ot_hours": ot_hours
        }
        return data

    # Gather data for each year
    data_2023 = get_premium_analysis_data(compare_2023, "2023")
    data_2024 = get_premium_analysis_data(compare_2024, "2024")
    if selected_year == "2025 (Projected)":
        data_2025 = get_premium_analysis_data(current_totals, "2025 (Projected)")

    # --- 2. High-Level KPI Scorecard ---
    # --- 2. High-Level KPI Scorecard ---
    st.markdown("##### Key Premium Metrics (2024)")

    # Change to 4 columns to make space for the new metric
    cols = st.columns(4) 

    with cols[0]:
        st.metric(
            label="Total Premium Costs",
            value=format_currency(data_2024['total_premiums'], selected_currency),
            delta=f"{((data_2024['total_premiums'] / data_2023['total_premiums']) - 1):.1%} vs '23",
        )
    with cols[1]:
        st.metric(
            label="As % of Total Labor Cost",
            value=f"{data_2024['premiums_as_pct_of_total_cost']:.1f}%",
            delta=f"{data_2024['premiums_as_pct_of_total_cost'] - data_2023['premiums_as_pct_of_total_cost']:.2f} pts vs '23"
        )

    # ADDED THIS NEW METRIC CARD FOR TOTAL OVERTIME HOURS
    with cols[2]:
        st.metric(
            label="Total Overtime Hours",
            value=f"{data_2024['ot_hours']:,.0f}",
            delta=f"{((data_2024['ot_hours'] / data_2023['ot_hours']) - 1):.1%} vs '23",
            delta_color="inverse" # More overtime hours is typically 'bad'
        )

    with cols[3]:
        st.metric(
            label="Cost per Hour of Overtime",
            value=format_currency(data_2024['cost_per_ot_hour'], selected_currency),
            delta=f"{((data_2024['cost_per_ot_hour'] / data_2023['cost_per_ot_hour']) - 1):.1%} vs '23",
            delta_color="inverse" # Higher cost is 'bad'
        )

    # --- 3. Premium Cost Composition (Side-by-Side Donut Charts) ---
    st.markdown("---")
    st.markdown("##### Premium Cost Breakdown by Year")
    cols = st.columns(2)
    donut_labels = ['Overtime', 'Work Conditions', 'Other Premiums']
    colors = ['#0d47a1', '#1976d2', '#64b5f6'] # Shades of blue

    with cols[0]:
        values_2023 = [data_2023['ot_cost'], data_2023['wc_cost'], data_2023['other_cost']]
        fig = go.Figure(data=[go.Pie(labels=donut_labels, values=values_2023, hole=.4, marker_colors=colors)])
        fig.update_layout(title_text='2023 Breakdown', showlegend=False, annotations=[dict(text='2023', x=0.5, y=0.5, font_size=20, showarrow=False)])
        st.plotly_chart(fig, use_container_width=True)

    with cols[1]:
        values_2024 = [data_2024['ot_cost'], data_2024['wc_cost'], data_2024['other_cost']]
        fig = go.Figure(data=[go.Pie(labels=donut_labels, values=values_2024, hole=.4, marker_colors=colors)])
        fig.update_layout(title_text='2024 Breakdown', showlegend=True, annotations=[dict(text='2024', x=0.5, y=0.5, font_size=20, showarrow=False)])
        st.plotly_chart(fig, use_container_width=True)


    # --- 4. Overtime Efficiency Analysis (Scatter Plot) ---
    st.markdown("---")
    st.markdown("##### Overtime Efficiency: Hours vs. Cost")
    st.info("""
        **Interpretation:** This chart plots the total cost of overtime against the total hours worked.
        - **Further from the origin** means more overtime was used.
        - A **steeper slope** from the origin to a point indicates a higher average cost per hour of overtime for that year.
    """)

    scatter_data = [data_2023, data_2024]
    if selected_year == "2025 (Projected)":
        scatter_data.append(data_2025)

    df_scatter = pd.DataFrame(scatter_data)

    fig_scatter = px.scatter(
        df_scatter,
        x='ot_hours',
        y='ot_cost',
        color='year',
        size='total_premiums',
        text='year',
        size_max=30,
        labels={
            "ot_hours": "Total Overtime Hours",
            "ot_cost": "Total Overtime Premium Cost ($)"
        }
    )
    fig_scatter.update_traces(textposition='top center')
    fig_scatter.update_layout(
        plot_bgcolor='white',
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True),
        legend_title_text='Year'
    )
    # Add lines from origin to each point to visualize the "rate"
    for i, row in df_scatter.iterrows():
        fig_scatter.add_shape(
            type="line",
            x0=0, y0=0, x1=row['ot_hours'], y1=row['ot_cost'],
            line=dict(color=px.colors.qualitative.Plotly[i], width=1, dash="dot"),
        )

    st.plotly_chart(fig_scatter, use_container_width=True)

    # --- END OF REDESIGNED SECTION ---


# --- End of Tab 3 ---

# Tab 4: Detailed Analysis
with tab4:
    st.markdown('<div class="sub-header">Detailed Analysis</div>', unsafe_allow_html=True)

    # Choose which data to display based on selected year
    if selected_year == "2023":
        analysis_df = df_2023_filtered
        analysis_totals = dept_totals_2023
    elif selected_year == "2024":
        analysis_df = df_2024_filtered
        analysis_totals = dept_totals_2024
    else:  # 2025 projection - base on 2024 data
        analysis_df = df_2024_filtered
        analysis_totals = current_totals

    # Create two sub-tabs for different analysis views
    subtab1, subtab2, subtab3, subtab4, subtab5 = st.tabs([
    "Band Distribution",
    "Hour Analysis",
    "Cost/FTE by Group", # New Tab Name
    "Overtime Cost",      # New Tab Name
    "Compensation Structure" # New tab name
])

    # Band Distribution
    with subtab1:
        st.markdown('<h3 class="sub-header" style="font-size: 1.6rem;">Analysis by Band</h3>', unsafe_allow_html=True)

        # analysis_df is already defined and filtered by Year from the main tab logic.

        # Ensure total_individual_cost is calculated for accurate metrics
        if 'total_individual_cost' not in analysis_df.columns:
            analysis_df['total_individual_cost'] = (
                analysis_df['base_salary'].fillna(0) +
                analysis_df['work_conditions_premium'].fillna(0) +
                analysis_df['overtime_premium'].fillna(0) +
                analysis_df['other_premiums'].fillna(0) +
                analysis_df['annual_bonus'].fillna(0) +
                analysis_df['profit_sharing'].fillna(0) +
                analysis_df['social_contribution'].fillna(0) +
                analysis_df['ltips'].fillna(0)
            )

        # Aggregate data by band
        band_analysis = analysis_df.groupby('band').agg(
            Number_of_FTEs=('fte', 'sum'),
            Total_Labor_Cost=('total_individual_cost', 'sum'),
            Average_Cost_per_FTE=('total_individual_cost', 'mean')
        ).sort_values(by='Average_Cost_per_FTE', ascending=True) # Sort for a clean ranked chart

        # --- Create a two-column layout ---
        col1, col2 = st.columns([0.6, 0.4])

        with col1:
            st.markdown("###### Band Cost per FTE, Ranked")

            if not band_analysis.empty:
                # --- Create the horizontal bar chart ---
                fig = px.bar(
                    band_analysis,
                    x='Average_Cost_per_FTE',
                    y=band_analysis.index, # Use index for y-axis since it's sorted
                    orientation='h',
                    color='Average_Cost_per_FTE',
                    color_continuous_scale=px.colors.sequential.Blues,
                    labels={
                        "y": "Band",
                        "Average_Cost_per_FTE": "Average Cost per FTE ($)",
                        "Number_of_FTEs": "FTEs"
                    }
                )

                # --- Add rich annotations inside the bars ---
                for i, (index, row) in enumerate(band_analysis.iterrows()):
                    annotation_text = f"<b>{row['Number_of_FTEs']:,}</b> FTEs  |  <b>{format_currency(row['Total_Labor_Cost'], selected_currency)}</b> Total"
                    fig.add_annotation(
                        x=5000, y=index, text=annotation_text,
                        showarrow=False, font=dict(color="white", size=11), xanchor='left'
                    )

                # --- Style and Layout Updates ---
                fig.update_layout(
                    height=500,
                    plot_bgcolor='white',
                    xaxis=dict(gridcolor='#e0e0e0'),
                    yaxis=dict(showgrid=False, categoryorder='total ascending'),
                    margin=dict(l=100, r=20, t=40, b=40)
                )
                fig.update_xaxes(tickprefix=currency_prefix, tickformat=',')
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available to display for Bands.")

        with col2:
            st.markdown("###### Band Breakdown Table")
            
            # Reset index to make 'band' a column for styling
            table_data = band_analysis.reset_index().sort_values(by='Average_Cost_per_FTE', ascending=False)
            
            table_data = table_data.rename(columns={
                'band': 'Band',
                'Number_of_FTEs': 'Number of FTE',
                'Total_Labor_Cost': 'Total Labor Cost',
                'Average_Cost_per_FTE': 'Avg. Cost per FTE'
            })
            
            styled_df = table_data.style \
                .format({
                    'Total Labor Cost': "${:,.0f}",
                    'Avg. Cost per FTE': "${:,.0f}"
                }) \
                .bar(subset=['Number of FTE'], color='#d6eaf8', align='left') \
                .background_gradient(subset=['Avg. Cost per FTE'], cmap='Blues')
            
            st.dataframe(styled_df, use_container_width=True, hide_index=True)

    # Hours Analysis
    with subtab2:
        st.markdown('<h3 class="sub-header" style="font-size: 1.6rem;">Hour Utilization & Productivity Analysis</h3>', unsafe_allow_html=True)

        # analysis_totals and analysis_df are already defined and filtered from the main tab logic.

        # --- 1. Hour Utilization Waterfall Chart ---
        st.markdown("##### How Planned Hours Became Actual Hours")
        st.info("This waterfall chart shows the journey from the total planned work hours to the actual hours recorded, highlighting the role of overtime.")

        planned = analysis_totals.get('total_planned_hours', 0)
        actual = analysis_totals.get('total_actual_hours', 0)
        overtime = analysis_totals.get('total_overtime_hours', 0)
        
        # This represents the variance in regular (non-overtime) hours
        regular_hours_variance = actual - planned - overtime

        fig_waterfall = go.Figure(go.Waterfall(
            name = "Hour Breakdown",
            orientation = "v",
            measure = ["absolute", "relative", "relative", "total"],
            x = ["<b>Planned Hours</b>", "Regular Time Variance", "<b>Overtime Hours</b>", "<b>Actual Hours</b>"],
            textposition = "outside",
            text = [f"{v:,.0f}" for v in [planned, regular_hours_variance, overtime, actual]],
            y = [planned, regular_hours_variance, overtime, actual],
            connector = {"line":{"color":"rgb(63, 63, 63)"}},
            increasing = {"marker":{"color":"#2e7d32"}}, # Green for OT
            decreasing = {"marker":{"color":"#c62828"}}, # Red for negative variance
            totals = {"marker":{"color":"#0d47a1"}} # Blue for totals
        ))

        fig_waterfall.update_layout(
            title="Hour Utilization Flow",
            showlegend=False,
            height=500
        )
        st.plotly_chart(fig_waterfall, use_container_width=True)

        # --- 2. Overtime Hotspots ---
        st.markdown("---")
        st.markdown("##### Overtime Hotspots by Function / Entity")
        
        # Calculate overtime per employee, then group by function
        df_ot = analysis_df.copy()
        overtime_by_func = df_ot.groupby('Entity/Function')['overtime_hours'].sum().sort_values(ascending=False)
        
        # Only show the top 10 functions with overtime
        top_ot_functions = overtime_by_func[overtime_by_func > 0].head(10)

        if not top_ot_functions.empty:
            fig_ot_hotspots = px.bar(
                top_ot_functions,
                y=top_ot_functions.index,
                x=top_ot_functions.values,
                orientation='h',
                text=top_ot_functions.values,
                labels={'y': 'Function / Entity', 'x': 'Total Overtime Hours'}
            )
            fig_ot_hotspots.update_layout(
                yaxis={'categoryorder':'total ascending'},
                plot_bgcolor='white',
                title_text="Top 10 Functions by Overtime Hours"
            )
            fig_ot_hotspots.update_traces(texttemplate='%{text:,.0f} hrs', marker_color='#1565c0')
            st.plotly_chart(fig_ot_hotspots, use_container_width=True)
        else:
            st.info("No overtime hours were recorded for the current selection.")

        # --- 3. Absence Analysis Breakdown ---
        st.markdown("---")
        st.markdown("##### Absence Analysis")
        
        # KPI cards for absence summary
        col1, col2, col3 = st.columns(3)
        with col1:
            total_absence_hours = analysis_totals.get('total_all_absence_hours',0)
            st.metric("Total Absence Hours", f"{total_absence_hours:,.0f}")
        with col2:
            absence_cost = analysis_totals.get('total_absence_costs', 0)
            st.metric("Estimated Absence Cost", format_currency(absence_cost, selected_currency))
        with col3:
            base_salary = analysis_totals.get('total_base_salary', 0)
            absence_rate = (absence_cost / base_salary * 100) if base_salary > 0 else 0
            st.metric("Absence Cost as % of Base Salary", f"{absence_rate:.1f}%")

        # Donut chart for absence hour composition
        absence_labels = ['Sick Hours', 'Holiday Hours', 'Other Absences']
        absence_values = [
            analysis_totals.get('total_sick_hours', 0),
            analysis_totals.get('total_holiday_hours', 0),
            analysis_totals.get('total_other_absences_hours', 0)
        ]

        fig_absence_donut = go.Figure(data=[go.Pie(
            labels=absence_labels, 
            values=absence_values, 
            hole=.4,
            marker_colors=['#64b5f6', '#42a5f5', '#1976d2']
        )])
        fig_absence_donut.update_layout(
            title_text="Composition of Absence Hours",
            legend_orientation="h"
        )
        st.plotly_chart(fig_absence_donut, use_container_width=True)
    with subtab3:
        st.markdown('<h3 class="sub-header" style="font-size: 1.6rem;">Cost per FTE by Function/Entity and Band</h3>', unsafe_allow_html=True)

        # Ensure analysis_df is the correct one for the selected year
        df_for_group_analysis = analysis_df.copy()

        # Calculate all cost components per employee for more accurate Cost per FTE calculation
        df_for_group_analysis['total_individual_cost'] = (
            df_for_group_analysis['base_salary'] +
            df_for_group_analysis['work_conditions_premium'] +
            df_for_group_analysis['overtime_premium'] +
            df_for_group_analysis['other_premiums'] +
            df_for_group_analysis['annual_bonus'] +
            df_for_group_analysis['profit_sharing'] +
            df_for_group_analysis['social_contribution'] +
            df_for_group_analysis['ltips']
        )

        group_cost_analysis = df_for_group_analysis.groupby(['Entity/Function', 'band']).agg(
            Number_of_Employees=('fte', 'sum'),
            Total_Cost_of_Labor=('total_individual_cost', 'sum')
        ).reset_index()

        # Handle cases where a group might have zero employees after filtering, preventing division by zero
        group_cost_analysis['Cost_per_FTE'] = group_cost_analysis.apply(
            lambda row: row['Total_Cost_of_Labor'] / row['Number_of_Employees'] if row['Number_of_Employees'] > 0 else 0,
            axis=1
        )
        
        # Sort for better visualization (optional, but can make tables easier to read)
        group_cost_analysis = group_cost_analysis.sort_values(by=['Entity/Function', 'band'])

        st.markdown(f"#### Average Cost per FTE for {selected_year} by Function/Entity and Band")

        # Pivot the data for the heatmap
        pivot_df = group_cost_analysis.pivot(index='band', columns='Entity/Function', values='Cost_per_FTE')

        if pivot_df.empty:
            st.warning(f"No data available to display Cost per FTE heatmap for the current selection ({selected_year}, Department: {selected_dept}).")
        else:
            fig_heatmap = px.imshow(
                pivot_df,
                text_auto=".2s",  # Format text for numbers (e.g., 120k, 1.2M)
                aspect="auto",    # Let Plotly manage aspect ratio based on available space
                color_continuous_scale="Blues"
            )

            # Rotate x-axis labels (Entity/Function) and ensure they are at the BOTTOM
            fig_heatmap.update_xaxes(
                side="bottom", # Changed from "top" to "bottom"
                tickangle=-45  # Rotate labels to prevent overlap
            )

            # Dynamically adjust height based on the number of bands, with a minimum height
            # and add padding for title and x-axis labels.
            # Number of unique bands for height calculation
            num_bands = len(pivot_df.index) if pivot_df is not None else 0
            chart_height = max(600, num_bands * 35 + 200) # Min height 600, or 35px per band + 200px padding

            fig_heatmap.update_layout(
                height=chart_height,
                # Adjusted margins: top margin reduced, bottom margin increased
                margin=dict(l=100, r=50, b=170, t=80),  # l=left, r=right, b=bottom, t=top
                                                        # Increased bottom margin for rotated x-axis labels at the bottom
                                                        # Reduced top margin as x-axis labels are no longer there
            )
            
            # Optional: If text inside cells still overlaps, you can try reducing its size
            # fig_heatmap.update_traces(textfont_size=10)

            st.plotly_chart(fig_heatmap, use_container_width=True)

        st.markdown("---")
        st.markdown("#### Tabular Breakdown: Cost per FTE by Function/Entity and Band")
        # Format for display in table
        group_cost_analysis_display = group_cost_analysis.copy()

        group_cost_analysis_display['Total_Cost_of_Labor'] = group_cost_analysis_display['Total_Cost_of_Labor'].apply(lambda x: format_currency(x, selected_currency))

        group_cost_analysis_display['Cost_per_FTE'] = group_cost_analysis_display['Cost_per_FTE'].apply(
            lambda x: format_currency(x, selected_currency)
        )

        st.dataframe(group_cost_analysis_display[['Entity/Function', 'band', 'Number_of_Employees', 'Total_Cost_of_Labor', 'Cost_per_FTE']], use_container_width=True, hide_index=True)

        if selected_year == "2025 (Projected)" and num_new_hires > 0:
            st.markdown(f"""
            <div class="highlight">
                <p><strong>Note for 2025 Projection:</strong> The analysis above for 'Cost per FTE by Group' is based on the underlying 2024 employee data structure. It does not dynamically adjust average costs for the new hires added in the projection. The new hires are averaged into the <em>overall</em> 'Total Cost of Labor' on the main dashboard, but not distributed across specific Entity/Function/band groups here for this granular view.</p>
            </div>
            """, unsafe_allow_html=True)

    # --- SUB-TAB 4: Overtime Cost Analysis ---
# --- (Inside tab4) ---
# REPLACE the existing "with subtab4:" block with this new one.

with subtab4:
    st.markdown('<h3 class="sub-header" style="font-size: 1.6rem;">Overtime Cost & Composition Analysis</h3>', unsafe_allow_html=True)

    if selected_year == "2025 (Projected)":
        st.info("Overtime analysis for 2025 reflects the underlying 2024 data structure, as future overtime distribution cannot be projected at this detailed level.")
        df_for_overtime_analysis = df_2024_filtered.copy()
    else:
        df_for_overtime_analysis = analysis_df.copy()

    # --- Data Preparation ---
    required_cols = ['base_salary', 'planned_hours', 'actual_hours', 'Blue_White_Collar', 'Entity/Function']
    if not all(col in df_for_overtime_analysis.columns for col in required_cols):
        st.warning("Cannot perform detailed overtime analysis. Required columns are missing.")
    else:
        # Calculate individual overtime and cost
        df_for_overtime_analysis['hourly_rate'] = (df_for_overtime_analysis['base_salary'] / df_for_overtime_analysis['planned_hours']).replace([np.inf, -np.inf], 0).fillna(0)
        df_for_overtime_analysis['individual_overtime_hours'] = df_for_overtime_analysis['overtime_hours']
        df_for_overtime_analysis['estimated_overtime_cost'] = df_for_overtime_analysis['individual_overtime_hours'] * df_for_overtime_analysis['hourly_rate'] * 1.5

        # --- Create a two-column layout ---
        col1, col2 = st.columns([0.4, 0.6])

        # --- LEFT COLUMN: KPIs and Composition ---
        with col1:
            # --- 1. High-Level KPIs in a Card ---
            st.markdown("###### Overall Summary")
            total_ot_hours = df_for_overtime_analysis['individual_overtime_hours'].sum()
            total_ot_cost = df_for_overtime_analysis['estimated_overtime_cost'].sum()
            effective_cost_per_hour = (total_ot_cost / total_ot_hours) if total_ot_hours > 0 else 0

            # Use st.container with a border for a card-like effect
            with st.container(border=True):
                st.metric("Total Overtime Hours", f"{total_ot_hours:,.0f}")
                st.metric("Total Estimated Overtime Cost", f"${total_ot_cost:,.2f}")
                st.metric("Effective Cost per OT Hour", f"${effective_cost_per_hour:,.2f}")

            st.markdown("<br>", unsafe_allow_html=True) # Add some vertical space

            # --- 2. Overtime Composition (BC vs WC) ---
            st.markdown("###### OT Hours by Job Type")
            collar_breakdown = df_for_overtime_analysis.groupby('Blue_White_Collar')['individual_overtime_hours'].sum().sort_values(ascending=False)
            
            if not collar_breakdown.empty:
                fig_collar = go.Figure(data=[go.Pie(
                    labels=collar_breakdown.index,
                    values=collar_breakdown.values,
                    hole=.5,
                    marker_colors=['#0d47a1', '#64b5f6'], # Dark and light blue
                    # Use texttemplate for better formatting
                    texttemplate='<b>%{label}</b><br>%{percent:.1%}',
                    insidetextfont=dict(size=14, color='white'),
                    hoverinfo='label+percent+value',
                    textfont_size=14,
                )])
                fig_collar.update_layout(
                    showlegend=False, 
                    margin=dict(t=10, b=10, l=10, r=10),
                    height=250 # Adjust height for better fit
                )
                st.plotly_chart(fig_collar, use_container_width=True)
            else:
                st.info("No job type data available for breakdown.")

        # --- RIGHT COLUMN: Overtime Hotspots Chart ---
        with col2:
            st.markdown("###### Overtime Hotspots by Function/Entity")
            
            # Aggregate by function
            overtime_by_func = df_for_overtime_analysis.groupby('Entity/Function').agg(
                Total_Hours=('individual_overtime_hours', 'sum'),
                Total_Cost=('estimated_overtime_cost', 'sum')
            ).sort_values(by='Total_Cost', ascending=True)

            overtime_by_func['Effective_Rate'] = (overtime_by_func['Total_Cost'] / overtime_by_func['Total_Hours']).fillna(0)
            overtime_by_func = overtime_by_func[overtime_by_func['Total_Cost'] > 0]
            
            if not overtime_by_func.empty:
                # Create the Annotated Horizontal Bar Chart
                fig_hotspots = px.bar(
                    overtime_by_func,
                    x='Total_Cost',
                    y=overtime_by_func.index,
                    orientation='h',
                    color='Effective_Rate',
                    color_continuous_scale=px.colors.sequential.Reds,
                    labels={
                        "y": "", # Remove y-axis label for a cleaner look
                        "Total_Cost": "Total Estimated Overtime Cost ($)",
                        "Effective_Rate": "Cost per Hour"
                    }
                )

                # Add an annotation ONLY to the top bar for clarity
                top_func = overtime_by_func.iloc[-1]
                annotation_text = f"<b>{top_func['Total_Hours']:,.0f} hours</b>"
                fig_hotspots.add_annotation(
                    x=top_func['Total_Cost'] * 0.95, # Position text inside, near the end
                    y=top_func.name,
                    text=annotation_text,
                    showarrow=False,
                    font=dict(color="white", size=11),
                    xanchor='right'
                )

                # Calculate dynamic height
                dynamic_height = max(400, len(overtime_by_func) * 35 + 70)

                fig_hotspots.update_layout(
                    height=dynamic_height,
                    plot_bgcolor='white',
                    xaxis=dict(gridcolor='#e0e0e0'),
                    yaxis=dict(showgrid=False),
                    margin=dict(l=150, r=20, t=40, b=40)
                )
                st.plotly_chart(fig_hotspots, use_container_width=True)
            else:
                st.info("No overtime cost data available to display.")
                
    # --- END OF SUB-TAB 4: Overtime Hour Analysis ---
                    
    # --- SUB-TAB 5: Compensation Structure ---
    with subtab5:
        st.markdown('<h3 class="sub-header" style="font-size: 1.6rem;">Compensation Structure Analysis</h3>', unsafe_allow_html=True)
    
        df_for_comp_analysis = analysis_df.copy()
    
        # Calculate total compensation components
        df_for_comp_analysis['Total_Premiums_Individual'] = df_for_comp_analysis['work_conditions_premium'] + df_for_comp_analysis['overtime_premium'] + df_for_comp_analysis['other_premiums']
        df_for_comp_analysis['Total_Bonuses_Individual'] = df_for_comp_analysis['annual_bonus'] + df_for_comp_analysis['profit_sharing']
        df_for_comp_analysis['Total_Social_Contributions_Individual'] = df_for_comp_analysis['social_contribution']
        df_for_comp_analysis['Total_Cost_Individual'] = (
            df_for_comp_analysis['base_salary'] +
            df_for_comp_analysis['Total_Premiums_Individual'] +
            df_for_comp_analysis['Total_Bonuses_Individual'] +
            df_for_comp_analysis['Total_Social_Contributions_Individual'] +
            df_for_comp_analysis['ltips']
        )
    
        st.markdown(f"#### Average Compensation Mix by Function/Entity ({selected_year})")
        comp_by_dept = df_for_comp_analysis.groupby('Entity/Function').agg(
            Avg_Base_Salary=('base_salary', 'mean'),
            Avg_Premiums=('Total_Premiums_Individual', 'mean'),
            Avg_Bonuses=('Total_Bonuses_Individual', 'mean'),
            Avg_Social_Contributions=('Total_Social_Contributions_Individual', 'mean'),
            Avg_LTIPs=('ltips', 'mean'),
            Avg_Total_Cost=('Total_Cost_Individual', 'mean')
        ).reset_index()
    
        # Convert to percentages for stacked bar chart
        comp_by_dept_pct = comp_by_dept.set_index('Entity/Function').apply(lambda x: x / x.sum(), axis=1).reset_index()
        comp_by_dept_pct_melted = comp_by_dept_pct.melt(id_vars='Entity/Function', var_name='Compensation_Component', value_name='Percentage')
    
        fig_comp_mix = px.bar(
            comp_by_dept_pct_melted,
            x='Entity/Function',
            y='Percentage',
            color='Compensation_Component',
            title='Average Compensation Mix by Function/Entity',
            labels={'Percentage': 'Percentage of Total Cost'},
            category_orders={"Compensation_Component": ['Avg_Base_Salary', 'Avg_Premiums', 'Avg_Bonuses', 'Avg_Social_Contributions', 'Avg_LTIPs']},
            color_discrete_map={
                'Avg_Base_Salary': px.colors.sequential.Blues[7],
                'Avg_Premiums': px.colors.sequential.Blues[5],
                'Avg_Bonuses': px.colors.sequential.Blues[3],
                'Avg_Social_Contributions': px.colors.sequential.Blues[1],
                'Avg_LTIPs': px.colors.sequential.Blues[0]
            },
            height=500
        )
        fig_comp_mix.update_yaxes(tickformat=".0%") # Format Y-axis as percentage
        fig_comp_mix.update_layout(hovermode="x unified") # Nice hover behavior
        st.plotly_chart(fig_comp_mix, use_container_width=True)
    
        st.markdown("---")
        st.markdown(f"#### Average Compensation Amounts by Function/Entity ({selected_year})")
        # Format for display table
        comp_by_dept_display = comp_by_dept.copy()
        for col in comp_by_dept_display.columns:
            if 'Avg_' in col:
                # FIXED: use lambda
                comp_by_dept_display[col] = comp_by_dept_display[col].apply(
                    lambda x: format_currency(x, selected_currency)
                )
        st.dataframe(comp_by_dept_display, use_container_width=True, hide_index=True)
    
    
        st.markdown(f"#### Average Compensation Mix by Band ({selected_year})")
        comp_by_band = df_for_comp_analysis.groupby('band').agg(
            Avg_Base_Salary=('base_salary', 'mean'),
            Avg_Premiums=('Total_Premiums_Individual', 'mean'),
            Avg_Bonuses=('Total_Bonuses_Individual', 'mean'),
            Avg_Social_Contributions=('Total_Social_Contributions_Individual', 'mean'),
            Avg_LTIPs=('ltips', 'mean'),
            Avg_Total_Cost=('Total_Cost_Individual', 'mean')
        ).reset_index()
    
        comp_by_band_pct = comp_by_band.set_index('band').apply(lambda x: x / x.sum(), axis=1).reset_index()
        comp_by_band_pct_melted = comp_by_band_pct.melt(id_vars='band', var_name='Compensation_Component', value_name='Percentage')
    
        fig_comp_mix_band = px.bar(
            comp_by_band_pct_melted,
            x='band',
            y='Percentage',
            color='Compensation_Component',
            title='Average Compensation Mix by Band',
            labels={'Percentage': 'Percentage of Total Cost'},
            category_orders={"Compensation_Component": ['Avg_Base_Salary', 'Avg_Premiums', 'Avg_Bonuses', 'Avg_Social_Contributions', 'Avg_LTIPs']},
            color_discrete_map={
                'Avg_Base_Salary': px.colors.sequential.Blues[7],
                'Avg_Premiums': px.colors.sequential.Blues[5],
                'Avg_Bonuses': px.colors.sequential.Blues[3],
                'Avg_Social_Contributions': px.colors.sequential.Blues[1],
                'Avg_LTIPs': px.colors.sequential.Blues[0]
            },
            height=500
        )
        fig_comp_mix_band.update_yaxes(tickformat=".0%")
        fig_comp_mix_band.update_layout(hovermode="x unified")
        st.plotly_chart(fig_comp_mix_band, use_container_width=True)
    
        st.markdown("---")
        st.markdown(f"#### Average Compensation Amounts by Band ({selected_year})")
        comp_by_band_display = comp_by_band.copy()
        for col in comp_by_band_display.columns:
            if 'Avg_' in col:
                comp_by_band_display[col] = comp_by_band_display[col].apply(
                    lambda x: format_currency(x, selected_currency)
                )
        st.dataframe(comp_by_band_display, use_container_width=True, hide_index=True)

    # ... (inside tab4, after subtab6) ...

# analysis_df (which is either df_2023_filtered, df_2024_filtered)
# analysis_totals (which is either dept_totals_2023, dept_totals_2024, or current_totals for 2025 proj)
# selected_year, num_new_hires, selected_hire_dept, selected_band, band_avg_df (for 2025 notes)
# format_currency function is also assumed to be available.

# # --- NEW SUB-TAB 7: Cost Efficiency Ratios ---
#     with subtab7:
#         st.markdown('<h3 class="sub-header" style="font-size: 1.6rem;">Cost Efficiency Ratios</h3>', unsafe_allow_html=True)
    
#         st.markdown(f"#### Overall Efficiency Metrics ({selected_year})")
#         col_e1, col_e2, col_e3 = st.columns(3)
    
#         # Calculate Cost per Planned Hour
#         cost_per_planned_hour = analysis_totals['total_cost'] / analysis_totals['total_planned_hours'] if analysis_totals['total_planned_hours'] > 0 else 0
#         with col_e1:
#             st.metric("Cost per Planned Hour", format_currency(cost_per_planned_hour), help="Total Cost of Labor / Total Planned Hours")
    
#         # Calculate Cost per Actual Hour
#         cost_per_actual_hour = analysis_totals['total_cost'] / analysis_totals['total_actual_hours'] if analysis_totals['total_actual_hours'] > 0 else 0
#         with col_e2:
#             st.metric("Cost per Actual Hour", format_currency(cost_per_actual_hour), help="Total Cost of Labor / Total Actual Hours")
    
#         # Overtime Hours as % of Actual Hours
#         overtime_pct_actual = (analysis_totals['total_overtime_hours'] / analysis_totals['total_actual_hours'] * 100).round(2) if analysis_totals['total_actual_hours'] > 0 else 0
#         with col_e3:
#             st.metric("Overtime % of Actual Hours", f"{overtime_pct_actual:.1f}%", help="Total Overtime Hours / Total Actual Hours")
    
#         st.markdown("---")
#         st.markdown(f"#### Efficiency Ratios by Pay Group ({selected_year})")
    
#         # IMPORTANT: Ensure all necessary derived columns exist on df_for_efficiency_analysis
#         # We will create a fresh copy of analysis_df for this tab and add all needed columns.
#         if selected_year == "2025 (Projected)":
#             # For detailed, per-group analysis in 2025, it's best to base it on 2024 data
#             # unless you have a sophisticated way to distribute projected hires to groups.
#             df_for_efficiency_analysis = df_2024_filtered.copy()
#             st.info("Note for 2025 Projection: Group-level efficiency ratios are based on 2024 data, as detailed future distributions are not dynamically projected.")
#         else:
#             df_for_efficiency_analysis = analysis_df.copy() # Use the currently filtered df
    
#         # --- Re-calculate/Add necessary individual-level columns for aggregation ---
#         # 1. Total individual cost (similar to what was in subtab6)
#         df_for_efficiency_analysis['total_individual_cost'] = (
#             df_for_efficiency_analysis['base_salary'] +
#             df_for_efficiency_analysis['work_conditions_premium'] +
#             df_for_efficiency_analysis['overtime_premium'] +
#             df_for_efficiency_analysis['other_premiums'] +
#             df_for_efficiency_analysis['annual_bonus'] +
#             df_for_efficiency_analysis['profit_sharing'] +
#             df_for_efficiency_analysis['social_security_tax'] +
#             df_for_efficiency_analysis['medicare'] +
#             df_for_efficiency_analysis['er_401k'] +
#             df_for_efficiency_analysis['er_pension'] +
#             df_for_efficiency_analysis['ltips']
#         )
    
#         # 2. Individual overtime hours (similar to what was in subtab5)
#         df_for_efficiency_analysis['individual_overtime_hours'] = (df_for_efficiency_analysis['actual_hours'] - df_for_efficiency_analysis['planned_hours']).apply(lambda x: max(0, x))
    
#         # 3. Components for Absence Cost calculation (re-done here for per-dept accuracy)
#         # Ensure 'sick_days', 'holiday_days', 'other_absences' are correctly handled.
#         # The 'base_salary' / 260.0 needs to be applied per row before summing.
#         df_for_efficiency_analysis['individual_absence_cost'] = (
#             df_for_efficiency_analysis['base_salary'] / 260.0 *
#             (df_for_efficiency_analysis['sick_days'] + df_for_efficiency_analysis['holiday_days'] + df_for_efficiency_analysis['other_absences'])
#         )
    
#         # Now, perform the aggregation
#         dept_efficiency_df = df_for_efficiency_analysis.groupby('department').agg(
#             Total_Planned_Hours=('planned_hours', 'sum'),
#             Total_Actual_Hours=('actual_hours', 'sum'),
#             Total_Overtime_Hours_Dept=('individual_overtime_hours', 'sum'),
#             Total_Base_Salary_Dept=('base_salary', 'sum'), # Base for absence cost %
#             Total_Absence_Costs_Dept=('individual_absence_cost', 'sum'), # Sum the pre-calculated individual costs
#             Total_Overall_Cost=('total_individual_cost', 'sum') # Sum the pre-calculated individual costs
#         ).reset_index()
    
#         # Calculate ratios for departments, handling potential division by zero
#         dept_efficiency_df['Cost_per_Planned_Hour'] = dept_efficiency_df.apply(
#             lambda row: row['Total_Overall_Cost'] / row['Total_Planned_Hours'] if row['Total_Planned_Hours'] > 0 else 0, axis=1
#         )
#         dept_efficiency_df['Cost_per_Actual_Hour'] = dept_efficiency_df.apply(
#             lambda row: row['Total_Overall_Cost'] / row['Total_Actual_Hours'] if row['Total_Actual_Hours'] > 0 else 0, axis=1
#         )
#         dept_efficiency_df['Overtime_Pct_of_Actual'] = dept_efficiency_df.apply(
#             lambda row: (row['Total_Overtime_Hours_Dept'] / row['Total_Actual_Hours'] * 100).round(2) if row['Total_Actual_Hours'] > 0 else 0, axis=1
#         )
#         dept_efficiency_df['Absence_Cost_Pct_of_Base'] = dept_efficiency_df.apply(
#             lambda row: (row['Total_Absence_Costs_Dept'] / row['Total_Base_Salary_Dept'] * 100).round(2) if row['Total_Base_Salary_Dept'] > 0 else 0, axis=1
#         )
    
#         # Handle NaNs/Infs from division by zero (after calculation, though apply lambda handles most)
#         dept_efficiency_df = dept_efficiency_df.replace([np.inf, -np.inf], np.nan)
#         for col_ratio in ['Cost_per_Planned_Hour', 'Cost_per_Actual_Hour', 'Overtime_Pct_of_Actual', 'Absence_Cost_Pct_of_Base']:
#             dept_efficiency_df[col_ratio] = dept_efficiency_df[col_ratio].fillna(0) # Or another suitable default
    
#         fig_efficiency_dept = make_subplots(rows=2, cols=2,
#                                             subplot_titles=("Cost per Planned Hour", "Cost per Actual Hour",
#                                                             "Overtime % of Actual Hours", "Absence Cost % of Base Salary"))
    
#         # Add traces, ensuring data is passed correctly for px.bar objects
#         # px.bar returns a Figure object, data[0] extracts the trace from it
#         fig_efficiency_dept.add_trace(px.bar(dept_efficiency_df, x='department', y='Cost_per_Planned_Hour', color='department').data[0], row=1, col=1)
#         fig_efficiency_dept.add_trace(px.bar(dept_efficiency_df, x='department', y='Cost_per_Actual_Hour', color='department').data[0], row=1, col=2)
#         fig_efficiency_dept.add_trace(px.bar(dept_efficiency_df, x='department', y='Overtime_Pct_of_Actual', color='department').data[0], row=2, col=1)
#         fig_efficiency_dept.add_trace(px.bar(dept_efficiency_df, x='department', y='Absence_Cost_Pct_of_Base', color='department').data[0], row=2, col=2)
    
#         fig_efficiency_dept.update_yaxes(tickprefix="$", tickformat=",", row=1, col=1)
#         fig_efficiency_dept.update_yaxes(tickprefix="$", tickformat=",", row=1, col=2)
#         fig_efficiency_dept.update_yaxes(ticksuffix="%", row=2, col=1)
#         fig_efficiency_dept.update_yaxes(ticksuffix="%", row=2, col=2)
    
#         fig_efficiency_dept.update_layout(height=800, title_text=f"Departmental Efficiency Ratios ({selected_year})", showlegend=False)
#         st.plotly_chart(fig_efficiency_dept, use_container_width=True)
    
#         st.markdown("---")
#         st.markdown("#### Tabular Breakdown: Efficiency Ratios by Pay Group")
#         efficiency_table_display = dept_efficiency_df[[
#             'department', 'Cost_per_Planned_Hour', 'Cost_per_Actual_Hour',
#             'Overtime_Pct_of_Actual', 'Absence_Cost_Pct_of_Base'
#         ]].copy()
#         efficiency_table_display['Cost_per_Planned_Hour'] = efficiency_table_display['Cost_per_Planned_Hour'].apply(format_currency)
#         efficiency_table_display['Cost_per_Actual_Hour'] = efficiency_table_display['Cost_per_Actual_Hour'].apply(format_currency)
#         efficiency_table_display['Overtime_Pct_of_Actual'] = efficiency_table_display['Overtime_Pct_of_Actual'].apply(lambda x: f"{x:.1f}%")
#         efficiency_table_display['Absence_Cost_Pct_of_Base'] = efficiency_table_display['Absence_Cost_Pct_of_Base'].apply(lambda x: f"{x:.1f}%")
#         st.dataframe(efficiency_table_display, use_container_width=True, hide_index=True)
# Footer
st.markdown("---")
st.markdown("**Cost of Labor Dashboard** | Developed for HR Analytics")
