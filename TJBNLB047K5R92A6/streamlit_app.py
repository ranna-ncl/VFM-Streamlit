import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, lit, when, iff, nvl, try_cast, date_add, to_timestamp_ntz, to_date
from snowflake.snowpark.types import StringType, DecimalType, DateType
import re
import textdistance
from difflib import get_close_matches
import calendar
import unicodedata
from io import StringIO
from io import BytesIO
from docx import Document
from datetime import datetime
import ast  # For ast.literal_eval in cluster_voyages
from scipy.stats import zscore # For ast.literal_eval in cluster_voyages
import base64
import time
import bcrypt
import openpyxl
from plotly.subplots import make_subplots
import pydeck as pdk
import matplotlib.cm as cm
import yaml
import io
import bcrypt
import textwrap
import re
import json
import streamlit.components.v1 as components
from datetime import datetime
from datetime import date
from snowflake.snowpark.context import get_active_session
from plotly.colors import qualitative

#----------------------------------Yaml file -----------------------------------------------------
@st.cache_resource
def load_yaml_from_snowflake():
    conn = st.connection("snowflake")

    df = conn.query("""
        SELECT YAML_CONTENT
        FROM VESSOPS_D.L00_STG.VFM_RULES
    """)

    if df is None or df.empty:
        st.error("No YAML content returned!")
        return {}

    yaml_text = df["YAML_CONTENT"].iloc[0]

    if yaml_text is None:
        st.error("YAML_CONTENT is NULL!")
        return {}

    try:
        config = yaml.safe_load(yaml_text)
    except Exception as e:
        st.error(f"YAML parsing error: {e}")
        return {}

    return config
#-------------------------------------------------------------------------------------------------
#----------Initialisation---------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------

# Set page configuration
st.set_page_config(page_title="Voyage Financials", layout="wide")


# Global list to store insights for report generation
if "all_insights" not in st.session_state:
    st.session_state["all_insights"] = []

def add_insight(all_insights, title, text=None, chart=None):
    """
    Adds an insight (with optional text and chart) to the insights list.
    """
    insight = {
        "title": title,
        "text": text or "",
        "chart": chart
    }
    all_insights.append(insight)


if "sort_state" not in st.session_state:
    st.session_state.sort_state = {
        "GSS": None,
        "CII": None,
        "LF": None,
    }
    
session = get_active_session()
@st.cache_data(show_spinner=False, ttl=60)
def fetch_identity():
    session = get_active_session()
    row = session.sql("""
        SELECT
            CURRENT_USER() AS USERNAME,
            CURRENT_AVAILABLE_ROLES() AS ROLES
    """).collect()[0]
    return {
        "username": row["USERNAME"],
        "raw_roles": row["ROLES"]
    }
 
def parse_roles(raw_roles):
    if raw_roles is None:
        return []
    if isinstance(raw_roles, list):
        out = raw_roles
    elif isinstance(raw_roles, str):
        try:
            out = json.loads(raw_roles)
        except Exception:
            out = []
    else:
        try:
            out = list(raw_roles)
        except Exception:
            out = []
    return [str(r).upper() for r in out]
ALLOWED_ROLES = {
    "SYSADMINDEV",
    "CR_VOSOA_APP_VIEWER"
}
ident = fetch_identity()
current_user = ident["username"]         
available_roles = parse_roles(ident["raw_roles"])
if not set(available_roles).intersection(ALLOWED_ROLES):
    st.error("🚫 You do not have access to this application.")
    st.stop()


# Initialize session state for page navigation
if 'page' not in st.session_state:
    st.session_state.page = 'landing_page'
if 'current_query' not in st.session_state:
    st.session_state.current_query = ""
if 'current_filters' not in st.session_state:
    st.session_state.current_filters = {}
if 'filtered_data' not in st.session_state:
    st.session_state.filtered_data = pd.DataFrame()
if 'selected_primary_metric_sidebar' not in st.session_state:
    st.session_state.selected_primary_metric_sidebar = "Margin $" # Default metric
if "use_future_data" not in st.session_state:
    st.session_state.use_future_data = False

    
config = load_yaml_from_snowflake()


all_Revenue_components = config.get("revenue_components", [])
Cost_components = config.get("cost_components", [])



KEYWORDS = config.get("keywords", {})
metric_groups = config.get("metric_groups", {})
metric_display_to_col = config.get("metric_display_to_col", {})
metric_display_names = config.get("metric_display_names", {})
order_list = config.get("order_list", {})
order_type = config.get("order_type", {})



PRODUCT_KEYWORDS = config["focus_keywords"].get("product", [])
VOYAGE_KEYWORDS = config["focus_keywords"].get("voyage", [])
SHIP_KEYWORDS = config["focus_keywords"].get("ship", [])
DEPLOYMENT_KEYWORDS =config["focus_keywords"].get("deployment", [])



# --- Voyage level Data Loading Functions---------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_data():
    try:
        session = get_active_session()
        snowpark_df = session.table("VESSOPS_D.L00_STG.VFM_STREAMLIT_DATA_BACKUP")
        snowpark_df = snowpark_df.select([col(c).alias(c.upper()) for c in snowpark_df.columns])

        MAX_EXCEL_DATE_SERIAL = 2958465

        snowpark_df = (
            snowpark_df.with_column(
                "SAIL_DATE_CLEAN_STRING",
                when(col("SAIL_DATE").cast(StringType()) == "NULL", lit(None))
                .when(col("SAIL_DATE").cast(StringType()) == "null", lit(None))
                .otherwise(col("SAIL_DATE").cast(StringType()))
            )
            .with_column(
                "SAIL_DATE",
                to_date(col("SAIL_DATE_CLEAN_STRING"),"YYYY-MM-DD")
            )
            .drop("SAIL_DATE_CLEAN_STRING")
            .with_column(
                "NEW_PRTD_PAX_DAYS_CLEAN_STRING",
                when(col("NEW_PRTD_PAX_DAYS").cast(StringType()) == "NULL", lit(None))
                .when(col("NEW_PRTD_PAX_DAYS").cast(StringType()) == "null", lit(None))
                .otherwise(col("NEW_PRTD_PAX_DAYS").cast(StringType()))
            )
            .with_column(
                "NEW_PRTD_PAX_DAYS",
                nvl(
                    try_cast(col("NEW_PRTD_PAX_DAYS_CLEAN_STRING"), DecimalType(38, 10)),
                    lit(0)
                )
            )
            .drop("NEW_PRTD_PAX_DAYS_CLEAN_STRING")
            .with_column(
                "NEW_PRTD_CAPS_DAYS_CLEAN_STRING",
                when(col("NEW_PRTD_CAPS_DAYS").cast(StringType()) == "NULL", lit(None))
                .when(col("NEW_PRTD_CAPS_DAYS").cast(StringType()) == "null", lit(None))
                .otherwise(col("NEW_PRTD_CAPS_DAYS").cast(StringType()))
            )
            .with_column(
                "NEW_PRTD_CAPS_DAYS",
                nvl(
                    try_cast(col("NEW_PRTD_CAPS_DAYS_CLEAN_STRING"), DecimalType(38, 10)),
                    lit(0)
                )
            )
            .drop("NEW_PRTD_CAPS_DAYS_CLEAN_STRING")
            .with_column(
                "DO_CAP_DAYS_CLEAN_STRING",
                when(col("DO_CAP_DAYS").cast(StringType()) == "NULL", lit(None))
                .when(col("DO_CAP_DAYS").cast(StringType()) == "null", lit(None))
                .otherwise(col("DO_CAP_DAYS").cast(StringType()))
            )
            .with_column(
                "DO_CAP_DAYS",
                nvl(
                    try_cast(col("DO_CAP_DAYS_CLEAN_STRING"), DecimalType(38, 10)),
                    lit(0)
                )
            )
            .drop("DO_CAP_DAYS_CLEAN_STRING")
            .with_column(
                "PAX_DAYS_CLEAN_STRING",
                when(col("PAX_DAYS").cast(StringType()) == "NULL", lit(None))
                .when(col("PAX_DAYS").cast(StringType()) == "null", lit(None))
                .otherwise(col("PAX_DAYS").cast(StringType()))
            )
            .with_column(
                "PAX_DAYS",
                nvl(
                    try_cast(col("PAX_DAYS_CLEAN_STRING"), DecimalType(38, 10)),
                    lit(0)
                )
            )
            .drop("PAX_DAYS_CLEAN_STRING")
            .with_column(
                "COMPONENT_AMOUNT_CLEAN_STRING",
                when(col("COMPONENT_AMOUNT").cast(StringType()) == "NULL", lit(None))
                .when(col("COMPONENT_AMOUNT").cast(StringType()) == "null", lit(None))
                .otherwise(col("COMPONENT_AMOUNT").cast(StringType()))
            )
            .with_column(
                "COMPONENT_AMOUNT",
                nvl(
                    try_cast(col("COMPONENT_AMOUNT_CLEAN_STRING"), DecimalType(38, 10)),
                    lit(0)
                )
            )
            .drop("COMPONENT_AMOUNT_CLEAN_STRING")
        )

        # Calculate REVENUE and COST
        snowpark_df = snowpark_df.with_column(
            "REVENUE",
            iff(col("M0_AND_M1").isin(all_Revenue_components), col("COMPONENT_AMOUNT"), lit(0))
        ).with_column(
            "COST",
            iff(col("M0_AND_M1").isin(Cost_components), col("COMPONENT_AMOUNT"), lit(0))
        )

        # Handle division by zero for LF
        snowpark_df = snowpark_df.with_column(
            "LF",
            iff(col("NEW_PRTD_CAPS_DAYS") == 0, lit(0), col("NEW_PRTD_PAX_DAYS") / col("NEW_PRTD_CAPS_DAYS"))
        )

        df = snowpark_df.to_pandas()
        df = df[df['VOYAGEBUCKET']!='Bucket 1']
        return df
    except Exception as e:
        st.error(f"FATAL DATA LOADING ERROR: {e}")
        st.stop()

# Account level data ------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_accnt_data():
    try:
        # Get the active Snowflake Snowpark session
        session = get_active_session()

        # Load and filter the Snowflake table
        from snowflake.snowpark.functions import col  # ✅ Ensure this is imported

        df = session.table("VESSOPS_D.L00_STG.VFM_SNOW_M0_M1_BACKUP") \
            .filter(
                ~(
                    (col("BUSINESS_UNIT_DESCRIPTION") == "NCL00-Norwegian Cruise Line") &
                    (col("OPERATING_UNIT_DESCRIPTION") == "NCL Pride of America")
                )
            ) \
            .filter(
                ~col("OPERATING_UNIT_DESCRIPTION").isin(["Common Shipside", "Common Shoreside"])
            )
        # Convert to pandas DataFrame
        df_accnt = df.to_pandas()

        df_accnt = df_accnt[df_accnt["VOYAGEBUCKET"].isin(["Bucket 3", "Bucket 4"])]

        # Normalize data types
        df_accnt["ACCOUNTING_PERIOD"] = df_accnt["ACCOUNTING_PERIOD"].astype(int)

        return df_accnt

    except SnowparkSQLException as e:
        st.error(f"⚠️ Snowflake query failed: {e}")
        return pd.DataFrame()  # return empty DataFrame on error

    except Exception as e:
        st.error(f"⚠️ An unexpected error occurred: {e}")
        return pd.DataFrame()

# the image data from the table--------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_image(use_value: str, return_url: bool = False):
    try:
        session = get_active_session()
        df = session.table("VESSOPS_D.L00_STG.VFM_IMAGE")
        df = df.select([col(c).alias(c.upper()) for c in df.columns])
        df = df.filter(col("USE") == use_value)

        df_pd = df.to_pandas()

        if df_pd.empty:
            st.warning(f"⚠️ No image found in VFM_IMAGE for USE='{use_value}'")
            return None

        image_bytes = df_pd["IMAGE_3D"].iloc[0]
        logo_b64 = base64.b64encode(image_bytes).decode("utf-8")

        if return_url:
            return f"data:image/png;base64,{logo_b64}"
        return logo_b64

    except Exception as e:
        st.error(f"FATAL IMAGE LOADING ERROR for USE='{use_value}': {e}")
        st.stop()

#--------Future Data ------------------
@st.cache_data(show_spinner="Loading future data...")
def load_future_data():
    session = get_active_session()
    query = """
        SELECT *
        FROM "VESSOPS_D"."L00_STG"."VFM_STREAMLIT_FUTURE_DATA_BACKUP"
    """
    return session.sql(query).to_pandas()

#---------------------------------------------------------------------------------------------------------------------------------------------------        
# ----NLP and Filter Extraction for Query Detection Function Block starts Here--------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def detect_query_type(query):
    query = query.lower()
    for qtype, keywords in KEYWORDS.items():
        if any(word in query for word in keywords):
            return qtype
    return None
def correct_query(user_query):
    if not user_query or not isinstance(user_query, str):
        return ""

    # ----------------------------
    # LOAD YAML CONFIG VALUES
    # ----------------------------
    domain_terms = config.get("domain_terms", [])
    preserve_terms = config.get("preserve_terms", [])
    ship_mapping = config.get("ship_mapping", {})

    # ----------------------------
    # PREPROCESSING
    # ----------------------------
    original_query = user_query.strip().lower()
    replacement_lookup = {}

    # Build dictionary: "norwegian bliss" → "bls"
    for code, names_list in ship_mapping.items():
        for name in names_list:
            replacement_lookup[name.lower()] = code

    # Replace ship full names with codes
    regex_patterns = sorted(map(re.escape, replacement_lookup.keys()), key=len, reverse=True)
    combined_regex = re.compile(r'\b(?:' + '|'.join(regex_patterns) + r')\b', re.IGNORECASE)

    processed_query = combined_regex.sub(lambda match: replacement_lookup[match.group(0).lower()], original_query)
    original_query = processed_query

    # ----------------------------
    # SPELL-CORRECTION
    # ----------------------------
    words = re.findall(r'\b\w+\b', original_query)
    corrected_words = []

    for word in words:

        # 1. Keep preserved terms as is
        if word in preserve_terms:
            corrected_words.append(word)
            continue

        # 2. Recognized domain terms remain unchanged
        if word in domain_terms:
            corrected_words.append(word)
            continue

        # 3. Correct spelling (Jaro-Winkler)
        best_match = word
        max_similarity = 0.8

        # match domain terms
        for term in domain_terms:
            similarity = textdistance.jaro_winkler(word, term)
            if similarity > max_similarity:
                max_similarity = similarity
                best_match = term

        # match preserve terms
        for term in preserve_terms:
            similarity = textdistance.jaro_winkler(word, term)
            if similarity > max_similarity:
                max_similarity = similarity
                best_match = term

        corrected_words.append(best_match)

    return " ".join(corrected_words)


# Normalise Text    

@st.cache_data
def normalize_text(text):
    if not isinstance(text, str):
        text = str(text) if text is not None else ""
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII').lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Filter function

def extract_filters_from_query(query, filtered_df):
    query_lower = normalize_text(query)
    ship_codes = []
    product_codes = []
    SHIP_CLASSs = [] # To capture ship names like "Alaska"
    components = []

    ship_codes_normalized = {normalize_text(str(ship)): ship for ship in filtered_df["SHIP_CD"].dropna().unique()}
    ou_codes_normalized = {normalize_text(str(ou)): ou for ou in filtered_df["SHIP_CLASS"].dropna().unique()}
    voyage_ids = filtered_df['VOYAGE_ID'].astype(str).unique().tolist()
    voyage_ids_upper = [v.upper() for v in voyage_ids if v]
    voyage_ids_original = filtered_df['VOYAGE_ID'].astype(str).dropna().unique().tolist()
    voyage_ids_normalized = {normalize_text(v): v for v in voyage_ids_original}

    # Find any matching voyage IDs inside query
    voyage_ids = [v for v in voyage_ids_upper if v in query.upper()]
    components_normalized = {normalize_text(str(component)): component for component in filtered_df["M0_AND_M1"].dropna().unique()}

    for normalized_ship_cd, original_ship_cd in ship_codes_normalized.items():
        if re.search(r'\b' + re.escape(normalized_ship_cd) + r'\b', query_lower):
            ship_codes.append(original_ship_cd)
    for normalized_ou, original_ou in ou_codes_normalized.items():
        if re.search(r'\b' + re.escape(normalized_ou) + r'\b', query_lower):
            SHIP_CLASSs.append(original_ou) # Store original ship name
    for normalized_voyage, original_voyage in voyage_ids_normalized.items():
        # Exact match
        if re.search(r'\b' + re.escape(normalized_voyage) + r'\b', query_lower):
            voyage_ids.append(original_voyage)
        # Prefix match (e.g. "BLS" → all BLS voyages)
        elif query_lower.strip().startswith(normalized_voyage[:3].lower()):  
            if original_voyage not in voyage_ids:
                voyage_ids.append(original_voyage)

    for normalized_component, original_component in components_normalized.items():
        if re.search(r'\b' + re.escape(normalized_component) + r'\b', query_lower):
            components.append(original_component)

    product_codes_normalized_rollup = {normalize_text(str(product)): product for product in filtered_df["RM_ROLLUP_PRODUCT_DESC"].dropna().unique()}
    #st.write(product_codes_normalized_rollup)
    #product_codes_normalized_product = {normalize_text(str(product)): product for product in filtered_df["PRODUCT"].dropna().unique()}

    for normalized_product, original_product in product_codes_normalized_rollup.items():
        if re.search(r'\b' + re.escape(normalized_product) + r'\b', query_lower):
            product_codes.append(original_product)
    # for normalized_product, original_product in product_codes_normalized_product.items():
    #     if re.search(r'\b' + re.escape(normalized_product) + r'\b', query_lower):
    #         product_codes.append(original_product)

    years = re.findall(r"\b(20\d{2})\b", query_lower)
    month_name_to_num = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
    month_name_to_num.update({abbr.lower(): i for i, abbr in enumerate(calendar.month_abbr) if abbr})
    month_names = list(month_name_to_num.keys())

    accounting_periods = set(str(p) for p in filtered_df["ACCOUNTING_PERIOD"].dropna().unique())
    months = set()

    quarter_map = {
        'q1': ['1', '2', '3'], 'q2': ['4', '5', '6'],
        'q3': ['7', '8', '9'], 'q4': ['10', '11', '12']
    }
    for q, q_months in quarter_map.items():
        if q in query_lower:
            months.update([m for m in q_months if m in accounting_periods])

    half_year_map = {
        'h1': ['1', '2', '3', '4', '5', '6'],
        'h2': ['7', '8', '9', '10', '11', '12']
    }
    for h, h_months in half_year_map.items():
        if h in query_lower:
            months.update([m for m in h_months if m in accounting_periods])

    range_match = re.search(
        rf"\b({'|'.join(month_names)})\s*(to|through|-)\s*({'|'.join(month_names)})\b",
        query_lower
    )
    if range_match:
        start_name, _, end_name = range_match.groups()
        start_num = month_name_to_num[start_name]
        end_num = month_name_to_num[end_name]
        if start_num <= end_num:
            month_range = [str(i) for i in range(start_num, end_num + 1)]
        else:
            month_range = [str(i) for i in list(range(start_num, 13)) + list(range(1, end_num + 1))]
        months.update([m for m in month_range if m in accounting_periods])

    for name, num in month_name_to_num.items():
        if re.search(r'\b' + re.escape(name) + r'\b', query_lower):
            months.add(str(num))

    month_digit_matches = re.findall(r"\b(0?[1-9]|1[0-2])\b", query_lower)
    months.update(month_digit_matches)
    months = set(str(int(m)) for m in months)

    match = re.search(r"last\s+(\d+)\s+months?", query_lower)
    if match:
        n_months = int(match.group(1))
        all_periods_sorted = sorted(accounting_periods, key=lambda x: int(x))
        months.update(all_periods_sorted[-n_months:])

    return ship_codes, product_codes, years, list(months), voyage_ids, SHIP_CLASSs,components # Return SHIP_CLASSs too

# extract orders from queries
def extract_orders_from_query(query):
    order_list = {
    'gss': ['gss', 'guest satisfaction score', 'guest satisfaction', 'guest score'],
    'cii': ['cii', 'carbon intensity indicator', 'carbon intensity', 'carbon indicator','carbon','emissions'],
    'lf':['load factor','load','occupancy','lf']}

    order_type = {
        'asc': ['asc', 'ascending', 'lowest to highest', 'low to high'],
        'dsc': ['dsc', 'descending', 'highest to lowest', 'high to low','desc']}
    

    query = query.lower()
    selected_column = None
    selected_order = None

    for order_key, order_vals in order_list.items():
        for val in order_vals:
            if re.search(r'\b' + re.escape(val) + r'\b', query):
                selected_column = order_key
                break
        if selected_column:
            break
            
    for order_key, order_vals in order_type.items():
        for val in order_vals:
            if re.search(r'\b' + re.escape(val) + r'\b', query):
                selected_order = order_key
                break
        if selected_order:
            break

    # Default to 'dsc' if column is selected but order not found
    if selected_column and not selected_order:
        selected_order = 'dsc'

    return selected_column, selected_order
#-----------------------------------------------------------------------------------------------------------------------------------------------------
# --- Charting Functions-------
#--------------------------------------------------------------------------------------------------------------------------------------------------
# General Bar Chart function
def create_bar_chart(df, x_col, y_col, title, color=None, extra_hover=None,key=None):
    if df.empty:
        st.warning(f"No data to display for {title}.")
        return

    custom_data_cols = [x_col, y_col]
    if extra_hover and extra_hover in df.columns:
        custom_data_cols.append(extra_hover)

    fig = px.bar(
        df,
        x=x_col,
        y=y_col,
        color=color,
        title=title,
        custom_data=custom_data_cols
    )
    hover_parts = [
        f"{x_col}: %{{customdata[0]}}",
        f"{y_col}: %{{customdata[1]:,.2f}}"
    ]
    if extra_hover and extra_hover in df.columns:
        hover_parts.append(f"{extra_hover}: %{{customdata[2]:.1%}}") # Format LF as percentage

    hover_template = "<br>".join(hover_parts) + "<extra></extra>"
    fig.update_traces(hovertemplate=hover_template)
    fig.update_layout(xaxis_title=x_col, yaxis_title=metric_display_names.get(y_col, y_col))
    st.plotly_chart(fig,use_container_width=True, key=key)

# General Dual Bar Chart function
@st.cache_data
def create_dual_bar_chart(
    df, category_col, left_col, right_col, title, y1_label="Metric 1", y2_label="Metric 2",
    lf_col="LF", height=600, key=None
):
    if df.empty:
        st.warning(f"No data to display for {title}.")
        return

    df_sorted = df.sort_values(by=right_col, ascending=False).copy()
    categories = df_sorted[category_col].astype(str)

    left_original = df_sorted[left_col]
    right_original = df_sorted[right_col]

    # Handle cases where max value might be zero to avoid division by zero
    left_max = left_original.max()
    right_max = right_original.max()

    left_scaled = left_original / left_max if left_max != 0 else pd.Series([0]*len(left_original))
    right_scaled = right_original / right_max if right_max != 0 else pd.Series([0]*len(right_original))

    lf_values = df_sorted[lf_col].fillna(0).astype(float) if lf_col in df_sorted.columns else pd.Series([0] * len(df_sorted))

    customdata_left = list(zip(left_original, lf_values))
    customdata_right = list(zip(right_original, lf_values))

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=categories, y=left_scaled, name=f"{y1_label} (normalized)", marker=dict(color="skyblue"),
            customdata=customdata_left,
            hovertemplate=(
                f"Total {y1_label}: %{{customdata[0]:,.0f}}<br>"
                f"{category_col}: %{{x}}<br>"
                "Load Factor: %{customdata[1]:.1%}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            x=categories, y=right_scaled, name=f"{y2_label} (normalized)", marker=dict(color="salmon"),
            customdata=customdata_right,
            hovertemplate=(
                f"Total {y2_label}: %{{customdata[0]:,.0f}}<br>"
                f"{category_col}: %{{x}}<br>"
                "Load Factor: %{customdata[1]:.1%}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        title=title, barmode="group", xaxis=dict(title=category_col, tickangle=45),
        yaxis=dict(title="Normalized Scale (0–1)"), template="plotly_white", height=600, bargap=0.3,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True,key=key)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Product centric  helper functions----------------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## 1.TOP& BOTTOM PERFORMER (product/SHIP centric )

def plot_overall_entity_performance(df, group_col, metric_col, metric_display_name, entity_type, key_prefix,
                                       order_column=None, order_type=None):
    global all_insights

    st.subheader(f"📊 Overall {entity_type} Performance (All Years)")

    use_custom_order = (order_column is not None) and (order_type is not None)
    df_processed = df.copy()

    # --- Tooltip mapping (calculate related details) ---
    try:
        if entity_type.lower() == "product":
            tooltip_mapping = (
                df_processed.groupby(['RM_ROLLUP_PRODUCT_DESC', 'SHIP_CD'])['VOYAGE_ID']
                .nunique().reset_index()
                .rename(columns={'RM_ROLLUP_PRODUCT_DESC': group_col, 'VOYAGE_ID': 'Voyage_Count'})
            )
            tooltip_mapping['Tooltip_Info'] = tooltip_mapping.apply(
                lambda x: f"{x['SHIP_CD']}: {x['Voyage_Count']} voyages", axis=1
            )
            tooltip_mapping = (
                tooltip_mapping.groupby(group_col)['Tooltip_Info']
                .apply(lambda x: "<br>".join(x)).reset_index()
            )
        elif entity_type.lower() == "ship":
            tooltip_mapping = (
                df_processed.groupby(['SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC'])['VOYAGE_ID']
                .nunique().reset_index()
                .rename(columns={'SHIP_CD': group_col, 'VOYAGE_ID': 'Voyage_Count'})
            )
            tooltip_mapping['Tooltip_Info'] = tooltip_mapping.apply(
                lambda x: f"{x['RM_ROLLUP_PRODUCT_DESC']}: {x['Voyage_Count']} voyages", axis=1
            )
            tooltip_mapping = (
                tooltip_mapping.groupby(group_col)['Tooltip_Info']
                .apply(lambda x: "<br>".join(x)).reset_index()
            )
        else:
            tooltip_mapping = pd.DataFrame({group_col: [], 'Tooltip_Info': []})
    except Exception:
        tooltip_mapping = pd.DataFrame({group_col: [], 'Tooltip_Info': []})

    tooltip_mapping['Tooltip_Info'] = tooltip_mapping.get('Tooltip_Info', pd.Series(dtype=str)).fillna("No voyage data")

    # --- Aggregation for main metric ---
    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = df_processed.groupby(group_col, as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = (
            df_processed.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'])[metric_col]
            .first().reset_index(name='t1')
        )
        df2 = df2.groupby(group_col, as_index=False)['t1'].sum()
        overall_agg = df1.merge(df2, on=group_col, how='inner')
        overall_agg['Total_Metric'] = overall_agg['COMPONENT_AMOUNT'] / overall_agg['t1']
    elif metric_display_name == 'Passenger Days':
        df2 = (
            df_processed.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'])
            ['NEW_PRTD_PAX_DAYS'].first().reset_index()
        )
        overall_agg = df2.groupby(group_col, as_index=False)['NEW_PRTD_PAX_DAYS'].sum()
        overall_agg.rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'}, inplace=True)
    elif metric_display_name == 'Capacity Days':
        df2 = (
            df_processed.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'])
            ['NEW_PRTD_CAPS_DAYS'].first().reset_index()
        )
        overall_agg = df2.groupby(group_col, as_index=False)['NEW_PRTD_CAPS_DAYS'].sum()
        overall_agg.rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'}, inplace=True)
    else:
        overall_agg = df_processed.groupby(group_col).agg(Total_Metric=(metric_col, 'sum')).reset_index()

    # --- Merge Tooltip_Info ---
    overall_agg = overall_agg.merge(tooltip_mapping, on=group_col, how='left')
    overall_agg['Tooltip_Info'] = overall_agg['Tooltip_Info'].fillna("No voyage data available")

    # --- Handle custom order if applicable ---
    if use_custom_order:
        order_column = order_column.upper()
        order_column_avg_name = f"{order_column}_Average"
        if order_column in ['GSS', 'CII']:
            order_col_for_agg = 'CII_SCORE' if order_column in ['CII', 'CII_SCORE'] else 'GSS'
            temp_df = df_processed.groupby(['VOYAGE_ID', group_col], as_index=False)[order_col_for_agg].first()
            order_col_avg_df = temp_df.groupby(group_col, as_index=False)[order_col_for_agg].mean()
            order_col_avg_df.rename(columns={order_col_for_agg: order_column_avg_name}, inplace=True)
            overall_agg = overall_agg.merge(order_col_avg_df, on=group_col, how='left')
        elif order_column in ['LOAD FACTOR', 'LF']:
            temp_df = df_processed.groupby(['VOYAGE_ID', group_col], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
            temp_df = temp_df.groupby(group_col, as_index=False).sum()
            temp_df[order_column_avg_name] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
            overall_agg = overall_agg.merge(temp_df[[group_col, order_column_avg_name]], on=group_col, how='left')
    else:
        order_column_avg_name = None

    # Define colors palette
    colors = px.colors.qualitative.Plotly
    
    # Get all unique entities in the dataset (not the sorted dataframe)
    unique_entities = df_processed[group_col].unique()
    entity_color_map = {entity: colors[i % len(colors)] for i, entity in enumerate(unique_entities)}

    # --- Sorting ---
    if use_custom_order and order_column_avg_name in overall_agg.columns:
        overall_agg.sort_values(by=order_column_avg_name, ascending=(order_type.lower() == 'asc'), inplace=True)
        sorting_info_text = f"by average '{order_column}' in {order_type} order."
    else:
        overall_agg.sort_values(by='Total_Metric', ascending=False, inplace=True)
        sorting_info_text = f"by '{metric_display_name}' in descending order (default)."

    # --- Plotting ---

    if not overall_agg.empty:
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        is_dark = st.get_option("theme.base") == "dark"
        
        fig.update_layout(
            hoverlabel=dict(
                align='left',
                bgcolor='black' if is_dark else 'white',
                font_color='white' if is_dark else 'black',
                font_size=13
            )
        )

        # ✅ Bar: full tooltip with Related Details
        bar_customdata = overall_agg[['Total_Metric', 'Tooltip_Info']].values
        fig.add_trace(
            go.Bar(
                x=overall_agg[group_col],
                y=overall_agg['Total_Metric'],
                name=metric_display_name,
                marker_color=[entity_color_map[x] for x in overall_agg[group_col]],
                customdata=bar_customdata,
                hovertemplate=(
                    f"<b>{entity_type}:</b> %{{x}}<br>"
                    f"<b>{metric_display_name}:</b> %{{y:.2f}}<br>"
                    "<b>Related Details:</b><br>%{customdata[1]}<extra></extra>"
                ),
                showlegend=True,
            ),
            secondary_y=False,
        )

        # ✅ Line: only show the sorting metric (if applicable)
        if use_custom_order and order_column_avg_name in overall_agg.columns:
            line_customdata = overall_agg[[order_column_avg_name]].values
            fig.add_trace(
                go.Scatter(
                    x=overall_agg[group_col],
                    y=overall_agg[order_column_avg_name],
                    mode='lines+markers',
                    name=f"{order_column} (Avg)",
                    line=dict(color='red', width=2),
                    marker=dict(size=8, color='red'),
                    customdata=line_customdata,
                    hovertemplate=f"<b>{order_column} (Avg):</b> %{{y:.2f}}<extra></extra>",
                    showlegend=True,
                ),
                secondary_y=True,
            )
            fig.update_yaxes(title_text=f"{order_column} (Avg)", secondary_y=True)

        fig.update_layout(
            title=f"Overall {metric_display_name} by {entity_type} (All Filtered Years)",
            xaxis_title=entity_type,
            yaxis_title=metric_display_name,
            hovermode="x unified",
            legend=dict(x=1.05, y=1, xanchor='left', yanchor='top'),
        )
        fig.update_xaxes(tickangle=45)

        st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_overall_plot")
        # --- Generate Insights (Unchanged) ---

        st.info(f"""*Graph is sorted {sorting_info_text}*""")
        if len(overall_agg) > 0:
            if len(overall_agg.iloc[:,0]) == 1:
                if use_custom_order:
                    top_entity_order = overall_agg.loc[overall_agg[order_column_avg_name].idxmax()]
                    top_entity_insight = overall_agg.loc[overall_agg['Total_Metric'].idxmax()]
                    insight_text = f"""
                    🚀 **Value Snapshot: {top_entity_insight[group_col]}**
                    
                    This summary highlights the key performance metrics for the selected **{entity_type}**.
                    
                    - **Total Activity ({metric_display_name}):** The **{top_entity_insight['Total_Metric']:,.2f}** recorded for this metric reflects the overall scale of its activity within this category.
                    - **Operational Efficiency ({order_column}):** This performance metric stands at **{top_entity_order[order_column_avg_name]:,.2f}**, serving as a benchmark for operational effectiveness and compliance.
                    """
                else:
                    top_entity_insight = overall_agg.loc[overall_agg['Total_Metric'].idxmax()]
                    insight_text = f"""
                    **Findings for Overall {entity_type} Performance**
                    
                    - **Key Insight:** The {entity_type} *{top_entity_insight[group_col]}* achieved a {metric_display_name} of **{top_entity_insight['Total_Metric']:,.2f}**, representing its overall performance in this category.  
                    """
            else:
                if use_custom_order:
                    top_entity_order = overall_agg.loc[overall_agg[order_column_avg_name].idxmax()]
                    bottom_entity_order = overall_agg.loc[overall_agg[order_column_avg_name].idxmin()]
                    top_entity_insight = overall_agg.loc[overall_agg['Total_Metric'].idxmax()]
                    bottom_entity_insight = overall_agg.loc[overall_agg['Total_Metric'].idxmin()]
                    avg_metric = overall_agg['Total_Metric'].mean()
        
                    insight_text = f"""
                    🚀 **Executive Summary: Performance Highs & Lows**
                    
                    This analysis identifies the top and bottom performers across all selected **{entity_type}s**.
                    
                    📈 **Total Activity: {metric_display_name}**
                    
                    * **Leader:** **{top_entity_insight[group_col]}** sets the benchmark, achieving a total **{metric_display_name}** of **{top_entity_insight['Total_Metric']:,.2f}**.
                    * **Underperformer:** **{bottom_entity_insight[group_col]}** recorded the lowest total at **{bottom_entity_insight['Total_Metric']:,.2f}**, indicating an area for closer review.
                    * **Group Benchmark:** The overall average **{metric_display_name}** across all entities is **{avg_metric:,.2f}**.
                    
                    ---
                    
                    🎯 **Operational Efficiency: {order_column}**
                    
                    * **Best Practice:** **{top_entity_order[group_col]}** demonstrates the **highest operational efficiency** with a **{order_column}** of **{top_entity_order[order_column_avg_name]:,.2f}**.
                    * **Focus Area:** **{bottom_entity_order[group_col]}** shows the **lowest efficiency** at **{bottom_entity_order[order_column_avg_name]:,.2f}**, highlighting potential performance constraints.
                    """
                else:
                    top_entity_insight = overall_agg.loc[overall_agg['Total_Metric'].idxmax()]
                    bottom_entity_insight = overall_agg.loc[overall_agg['Total_Metric'].idxmin()]
                    avg_metric = overall_agg['Total_Metric'].mean()
        
                    insight_text = f"""
                    🚀 **Executive Summary: Performance Highs & Lows**
                    
                    This analysis identifies the top and bottom performers across all selected **{entity_type}s**.
                    
                    📈 **Total Activity: {metric_display_name}**
                    
                    * **Leader:** **{top_entity_insight[group_col]}** leads the performance chart with a total **{metric_display_name}** of **{top_entity_insight['Total_Metric']:,.2f}**.
                    * **Underperformer:** **{bottom_entity_insight[group_col]}** recorded the lowest total at **{bottom_entity_insight['Total_Metric']:,.2f}**, signaling a need for further review.
                    * **Group Benchmark:** The average **{metric_display_name}** across all entities stands at **{avg_metric:,.2f}**.
                    """
            st.markdown(insight_text)

            # Placeholder for add_insight
            add_insight(st.session_state["all_insights"], title= f"Overall {entity_type} Performance", text = insight_text, chart = fig )
        
        else:
            st.info(f"No overall {entity_type} data available for performance plot.")
            # Placeholder for add_insight
            add_insight(st.session_state["all_insights"], title= f"Overall {entity_type} Performance", text = f"No overall {entity_type} data available.")

# 2.To compare over period(product/ship centric)
def plot_entity_performance_comparison(df, group_col, metric_col, metric_display_name, entity_type, key_prefix):

    global all_insights
    st.subheader(f"📊 {entity_type} Performance Comparison")

    unique_entities = sorted(df[group_col].dropna().unique().tolist())
    unique_years = sorted(df['FISCAL_YEAR'].dropna().unique().astype(int).tolist())
    unique_months = sorted(df['ACCOUNTING_PERIOD'].dropna().unique().astype(int).tolist())
    
    if 'QUARTER_INT' not in df.columns:
        df['QUARTER'] = 'Q' + ((df['ACCOUNTING_PERIOD'] - 1) // 3 + 1).astype(str)
        df['QUARTER_INT'] = df['QUARTER'].str.replace('Q', '').astype(int)

    unique_quarters_int = sorted(df['QUARTER_INT'].dropna().unique().tolist())
    unique_quarters_str = [f'Q{q}' for q in unique_quarters_int]

    if not unique_entities or not unique_years:
        st.info(f"No {entity_type} or yearly data available for performance comparison.")
        add_insight(     st.session_state["all_insights"],     title= f"{entity_type} Performance Comparison", text = f"No {entity_type} or yearly data available.")
        return

    # User selections for granularity
    granularity_option = st.selectbox(
        f"Select Granularity for {entity_type} Comparison:",
        options=["Whole Year", "Monthly", "Quarterly"],
        key=f"{key_prefix}_granularity_selector"
    )

    selected_period = None
    filtered_comparison_df = pd.DataFrame() # Initialize empty DataFrame

    if granularity_option == "Whole Year":
        group_by_cols = ['FISCAL_YEAR', group_col]
        
        if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
            df1 = df.groupby(group_by_cols, as_index=False)['COMPONENT_AMOUNT'].sum()
            df2 = df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
            df2 = df2.groupby(group_by_cols, as_index=False)['t1'].sum()
            overall_agg = df1.merge(df2, on=group_by_cols, how='inner')
            overall_agg['Total_Metric'] = overall_agg['COMPONENT_AMOUNT'] / overall_agg['t1']


            filtered_comparison_df = overall_agg

                   
        elif metric_display_name == 'Passenger Days':
            df2 = (
                df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_PAX_DAYS']
                .first()
                .reset_index()
            )
        
            overall_agg = (
                df2.groupby(group_by_cols, as_index=False)['NEW_PRTD_PAX_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
            )
            filtered_comparison_df = overall_agg
        elif metric_display_name == 'Capacity Days':
            df2 = (
                df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_CAPS_DAYS']
                .first()
                .reset_index()
            )
        
            overall_agg = (
                df2.groupby(group_by_cols, as_index=False)['NEW_PRTD_CAPS_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
            )
            filtered_comparison_df = overall_agg
            
        else:
            filtered_comparison_df = df.groupby(group_by_cols).agg(
                Total_Metric=(metric_col, 'sum')
            ).reset_index()
        
        filtered_comparison_df['Period_Label'] = filtered_comparison_df['FISCAL_YEAR'].astype(str)

    elif granularity_option == "Monthly":
        selected_month = st.selectbox(
            "Select Month:",
            options=unique_months,
            format_func=lambda x: pd.to_datetime(x, format='%m').strftime('%B'),
            key=f"{key_prefix}_month_selector"
        )
        selected_period = selected_month
        
        temp_df = df[df['ACCOUNTING_PERIOD'] == selected_month].copy()
        group_by_cols = ['FISCAL_YEAR', group_col]

        if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
            df1 = temp_df.groupby(group_by_cols, as_index=False)['COMPONENT_AMOUNT'].sum()
            df2 = temp_df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
            df2 = df2.groupby(group_by_cols, as_index=False)['t1'].sum()
            overall_agg = df1.merge(df2, on=group_by_cols, how='inner')
            overall_agg['Total_Metric'] = overall_agg['COMPONENT_AMOUNT'] / overall_agg['t1']
            
            filtered_comparison_df = overall_agg

        elif metric_display_name == 'Passenger Days':
            df2 = (
                df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_PAX_DAYS']
                .first()
                .reset_index()
            )
        
            overall_agg = (
                df2.groupby(group_by_cols, as_index=False)['NEW_PRTD_PAX_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
            )
            filtered_comparison_df = overall_agg
        elif metric_display_name == 'Capacity Days':
            df2 = (
                df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_CAPS_DAYS']
                .first()
                .reset_index()
            )
        
            overall_agg = (
                df2.groupby(group_by_cols, as_index=False)['NEW_PRTD_CAPS_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
            )
            filtered_comparison_df = overall_agg
        else:
            filtered_comparison_df = temp_df.groupby(group_by_cols).agg(
                Total_Metric=(metric_col, 'sum')
            ).reset_index()
            
        filtered_comparison_df['Period_Label'] = filtered_comparison_df['FISCAL_YEAR'].astype(str)


    elif granularity_option == "Quarterly":
        selected_quarter_str = st.selectbox(
            "Select Quarter:",
            options=unique_quarters_str,
            key=f"{key_prefix}_quarter_selector"
        )
        selected_quarter_int = int(selected_quarter_str.replace('Q', ''))
        selected_period = selected_quarter_str
        
        temp_df = df[df['QUARTER_INT'] == selected_quarter_int].copy()

        group_by_cols = ['FISCAL_YEAR', group_col]

        if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
            df1 = temp_df.groupby(group_by_cols, as_index=False)['COMPONENT_AMOUNT'].sum()
            df2 = temp_df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
            df2 = df2.groupby(group_by_cols, as_index=False)['t1'].sum()
            overall_agg = df1.merge(df2, on=group_by_cols, how='inner')
            overall_agg['Total_Metric'] = overall_agg['COMPONENT_AMOUNT'] / overall_agg['t1']
            
            filtered_comparison_df = overall_agg


        elif metric_display_name == 'Passenger Days':
            df2 = (
                df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_PAX_DAYS']
                .first()
                .reset_index()
            )
        
            overall_agg = (
                df2.groupby(group_by_cols, as_index=False)['NEW_PRTD_PAX_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
            )
            filtered_comparison_df = overall_agg
        elif metric_display_name == 'Capacity Days':
            df2 = (
                df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_CAPS_DAYS']
                .first()
                .reset_index()
            )
        
            overall_agg = (
                df2.groupby(group_by_cols, as_index=False)['NEW_PRTD_CAPS_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
            )
            filtered_comparison_df = overall_agg
        else:
            filtered_comparison_df = temp_df.groupby(group_by_cols).agg(
                Total_Metric=(metric_col, 'sum')
            ).reset_index()

        filtered_comparison_df['Period_Label'] = filtered_comparison_df['FISCAL_YEAR'].astype(str)

    #st.write(filtered_comparison_df)
    if not filtered_comparison_df.empty:
        # --- Add Related Details (Products per Ship or Ships per Product) ---
            # --- Build Related Details dynamically per row based on granularity ---
        related_texts = []
    
        for _, row in filtered_comparison_df.iterrows():
            year = int(row['FISCAL_YEAR'])
            entity = row[group_col]
    
            # Select subset of df according to granularity
            if granularity_option == "Whole Year":
                subset = df[df['FISCAL_YEAR'] == year]
            elif granularity_option == "Monthly":
                # Use month-specific subset per year
                subset = df[(df['FISCAL_YEAR'] == year) & (df['ACCOUNTING_PERIOD'] == selected_month)]
            elif granularity_option == "Quarterly":
                subset = df[(df['FISCAL_YEAR'] == year) & (df['QUARTER_INT'] == selected_quarter_int)]
            else:
                subset = df[df['FISCAL_YEAR'] == year]
    
            # Build related mapping
            if entity_type.lower() == "ship":
                subset = subset[subset['SHIP_CD'] == entity]
                grp = (
                    subset.groupby('RM_ROLLUP_PRODUCT_DESC')['VOYAGE_ID']
                    .nunique()
                    .reset_index(name='Voyage_Count')
                )
                id_col = name_col = 'RM_ROLLUP_PRODUCT_DESC'
                empty_msg = "No related products found"
            else:  # Product level
                subset = subset[subset['RM_ROLLUP_PRODUCT_DESC'] == entity]
                grp = (
                    subset.groupby('SHIP_CD')['VOYAGE_ID']
                    .nunique()
                    .reset_index(name='Voyage_Count')
                )
                id_col = name_col = 'SHIP_CD'
                empty_msg = "No related ships found"
    
            # Format for tooltip
            if not grp.empty:
                grp = grp.sort_values(by=['Voyage_Count', name_col], ascending=[False, True])
                lines = [
                    f"{name}: {cnt} voyage{'s' if cnt != 1 else ''}"
                    for name, cnt in zip(grp[name_col], grp['Voyage_Count'])
                ]
                related_text = "<br>".join(lines)
            else:
                related_text = empty_msg
    
            related_texts.append(related_text)
    
        # Attach computed Related_Info to DataFrame
        filtered_comparison_df = filtered_comparison_df.reset_index(drop=True)
        filtered_comparison_df['Related_Info'] = related_texts

        
        # Attach Related_Info to filtered_comparison_df
        filtered_comparison_df = filtered_comparison_df.reset_index(drop=True)
        filtered_comparison_df['Related_Info'] = related_texts
        #st.write(filtered_comparison_df)
        # Sort by Period_Label (Year) to ensure consistent ordering of colored bars within each group
        filtered_comparison_df = filtered_comparison_df.sort_values(by='Period_Label')

        # Calculate an overall ranking for entities to sort the X-axis categories
        # Sort by total metric across all displayed periods for the entity type (ascending)
        entity_ranking = filtered_comparison_df.groupby(group_col)['Total_Metric'].sum().sort_values(ascending=False) 
        sorted_entities_for_xaxis = entity_ranking.index.tolist()

        chart_title = f"{metric_display_name} by {entity_type} - {granularity_option}"
        if selected_period:
            if granularity_option == "Monthly":
                chart_title += f" (Month: {pd.to_datetime(selected_period, format='%m').strftime('%B')})"
            elif granularity_option == "Quarterly":
                chart_title += f" (Quarter: {selected_period})"
        #st.write(filtered_comparison_df)
        # --- Create the px.bar figure (unchanged) ---
        fig = px.bar(
            filtered_comparison_df,
            x=group_col,
            y='Total_Metric',
            color='Period_Label',
            barmode='group',
            title=chart_title,
            labels={group_col: entity_type, 'Total_Metric': metric_display_name, 'Period_Label': 'Year'},
            category_orders={group_col: sorted_entities_for_xaxis},
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        
        # --- Now attach per-trace customdata so year & related-info match correctly ---
        # Each trace created by px.bar corresponds to one Period_Label (year).
        # We'll loop traces and set trace.customdata to the rows for that year only.
        for trace in fig.data:
            trace_name = trace.name  # this is the Period_Label value (e.g., '2023')
            # Subset rows for this year / period_label in the same order as they appear on the trace.x
            sub = filtered_comparison_df[filtered_comparison_df['Period_Label'] == trace_name].copy()
        
            # Ensure ordering of sub matches trace.x (trace.x contains entity names in the same sequence)
            # Convert trace.x to list (sometimes it's a tuple)
            trace_x_list = list(trace.x)
        
            # Reindex sub to match trace_x_list
            if len(sub) > 0:
                sub = sub.set_index(group_col).reindex(trace_x_list).reset_index()
        
                # Build customdata columns: Period_Label (for the Year) and Related_Info (multi-line string)
                # Use Period_Label as string (already), Related_Info contains <br> separated lines for hover
                customdata = sub[['Period_Label', 'Related_Info']].values.tolist()
        
                # Assign customdata to this trace
                trace.customdata = customdata
        
                # Set hovertemplate for this trace to include Related Details (multi-line)
                # Use escaped Plotly tokens inside f-string ({{ }}) so Python doesn't try to interpolate them
                trace.hovertemplate = (
                    f"<b>{entity_type}</b>: %{{x}}<br>"
                    "Year: %{customdata[0]}<br>"
                    f"{metric_display_name}: %{{y:,.2f}}<br><br>"
                    "<b>Related Details:</b><br>%{customdata[1]}<extra></extra>"
                )
            else:
                # If no matching rows (unlikely), set empty customdata and default hovertemplate
                trace.customdata = []
                trace.hovertemplate = (
                    f"<b>{entity_type}</b>: %{{x}}<br>"
                    f"{metric_display_name}: %{{y:,.2f}}<extra></extra>"
                )
        
        # --- Layout adjustments (keep your existing config) ---
        fig.update_layout(
            xaxis_title=entity_type,
            yaxis_title=metric_display_name,
            legend_title="Year"
        )
        fig.update_xaxes(tickangle=45)
        
        st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_comparison_plot")

        insight_text = f"**{entity_type} Performance Comparison - {granularity_option}:**\n\n"

        # Overall analysis across all years/periods displayed in the chart
        if not filtered_comparison_df.empty:
            # Find the top/bottom performing entities overall (across all years/periods in the plot)
            overall_entity_performance = filtered_comparison_df.groupby(group_col)['Total_Metric'].sum().sort_values(ascending=False) # Sort descending for insights

            if not overall_entity_performance.empty:
                top_entity_overall = overall_entity_performance.index[0]
                top_metric_overall = overall_entity_performance.iloc[0]
                insight_text += f"- **Overall Top Performing {entity_type}**: **{top_entity_overall}** with a total {metric_display_name} of {top_metric_overall:,.2f}.\n"

                if len(overall_entity_performance) > 1:
                    bottom_entity_overall = overall_entity_performance.index[-1]
                    bottom_metric_overall = overall_entity_performance.iloc[-1]
                    insight_text += f"- **Overall Lowest Performing {entity_type}**: **{bottom_entity_overall}** with a total {metric_display_name} of {bottom_metric_overall:,.2f}.\n"
                else:
                     insight_text += f"- Only one {entity_type} (**{top_entity_overall}**) available for comparison.\n"
        
        add_insight(     st.session_state["all_insights"],     title= chart_title, text = insight_text, chart = fig)

    else:
        st.info(f"No data available for the selected {granularity_option} comparison.")
        add_insight(     st.session_state["all_insights"],     title= f"{entity_type} Performance Comparison", text = f"No data for {entity_type} in the selected period.")

# 3.comparision of ship class ( product centric)

def plot_product_ship_class_summary(df, selected_product, metric_col, metric_display_name, order_column=None, order_type=None):
 
    global all_insights
    st.subheader(f"🚢 Ship Class Performance for {selected_product} by Year")

    # --- Handle None for order_column and order_type ---
    use_custom_order = (order_column is not None) and (order_type is not None)
    
    order_col_for_agg = None
    if use_custom_order:
        order_column = order_column.upper()
        if order_column in ['CII', 'CII_SCORE']:
            order_col_for_agg = 'CII_SCORE'
        elif order_column in ['GSS']:
            order_col_for_agg = 'GSS'
        elif order_column in ['LOAD FACTOR', 'LF']:
            order_col_for_agg = 'PAX_DAYS'  # Use PAX_DAYS for the complex LF calculation
        order_column_avg_name = f"{order_column}_Average"
        sorting_info_text = f"*Graph is sorted by average '{order_column}' in {order_type} order.*"
        st.info(sorting_info_text)
    else:
        order_column_avg_name = None
        sorting_info_text = f"*Graph is sorted by '{metric_display_name}' in descending order (default).*"
        st.info(sorting_info_text)

    product_df = df[df['RM_ROLLUP_PRODUCT_DESC'] == selected_product].copy()

    if product_df.empty:
        st.warning(f"No data for product '{selected_product}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Ship Class Performance for {selected_product}", text = f"No data for product '{selected_product}'.")
        return

    unique_years_in_product = sorted(product_df['FISCAL_YEAR'].dropna().unique().tolist())

    if not unique_years_in_product:
        st.info(f"No yearly data available for product '{selected_product}' to compare ship classes.")
        add_insight(     st.session_state["all_insights"],     title= f"Ship Class Performance for {selected_product}", text = f"No yearly data available for product '{selected_product}' to compare ship classes.")
        return

    # Create a consistent color map for all ship classes across all years
    all_unique_ship_classes = sorted(df['SHIP_CLASS'].dropna().unique().tolist())
    colors = px.colors.qualitative.Plotly
    ship_class_color_map = {sc: colors[i % len(colors)] for i, sc in enumerate(all_unique_ship_classes)}

    # --- Pre-calculate min/max for consistent y-axes ---
    all_years_agg = pd.DataFrame()
    for year in unique_years_in_product:
        year_product_df = product_df[product_df['FISCAL_YEAR'] == year].copy()
        if year_product_df.empty:
            continue

        # Aggregation Logic for Primary Metric
        if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
            df1 = year_product_df.groupby('SHIP_CLASS', as_index=False)['COMPONENT_AMOUNT'].sum()
            df2 = year_product_df.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CLASS', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
            df2 = df2.groupby('SHIP_CLASS', as_index=False)['t1'].sum()
            yearly_class_agg = df1.merge(df2, on='SHIP_CLASS', how='inner')
            yearly_class_agg['Total_Metric'] = yearly_class_agg['COMPONENT_AMOUNT'] / yearly_class_agg['t1']
            
        elif metric_display_name == 'Passenger Days':
            df2 = (
                year_product_df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CLASS','SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_PAX_DAYS']
                .first()
                .reset_index()
            )
        
            yearly_class_agg= (
                df2.groupby('SHIP_CLASS', as_index=False)['NEW_PRTD_PAX_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
            )

        elif metric_display_name == 'Capacity Days':
            df2 = (
                year_product_df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD','SHIP_CLASS', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_CAPS_DAYS']
                .first()
                .reset_index()
            )
        
            yearly_class_agg = (
                year_product_df.groupby('SHIP_CLASS', as_index=False)['NEW_PRTD_CAPS_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
            )

        else:
            yearly_class_agg = year_product_df.groupby('SHIP_CLASS').agg(
                Total_Metric=(metric_col, 'sum')
            ).reset_index()

        yearly_class_agg['FISCAL_YEAR'] = year
        # --- Add Related Details per Ship Class & Year ---
        related_details_map = {}
        year_subset = product_df[product_df["FISCAL_YEAR"] == year]
        
        for ship_class, sub_df in year_subset.groupby("SHIP_CLASS"):
            ship_info = []
            for ship_cd, ship_cd_df in sub_df.groupby("SHIP_CD"):
                voyage_count = ship_cd_df["VOYAGE_ID"].nunique()
                ship_info.append(f"{ship_cd} – {voyage_count} voyages")
            related_details_map[ship_class] = "<br>".join(ship_info)
        
        # Attach Related_Info column
        yearly_class_agg["Related_Info"] = yearly_class_agg["SHIP_CLASS"].map(
            related_details_map
        )
        #st.write(yearly_class_agg)
        # Aggregation Logic for Order Column
        if use_custom_order:
            if order_column in ['GSS', 'CII']:
                order_col_for_agg = 'CII_SCORE' if order_column == 'CII' else 'GSS'
                order_col_avg_df = year_product_df.groupby(['VOYAGE_ID', 'SHIP_CLASS'], as_index=False)[order_col_for_agg].first()
                order_col_avg_df = order_col_avg_df.groupby('SHIP_CLASS', as_index=False)[order_col_for_agg].mean().rename(columns={order_col_for_agg: order_column_avg_name})
                yearly_class_agg = yearly_class_agg.merge(order_col_avg_df, on='SHIP_CLASS', how='left')
            elif order_column in ['LOAD FACTOR', 'LF']:
                temp_df = year_product_df.groupby(['VOYAGE_ID', 'SHIP_CLASS'], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
                temp_df = temp_df.groupby('SHIP_CLASS', as_index=False).sum()
                temp_df[order_column_avg_name] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
                yearly_class_agg = yearly_class_agg.merge(temp_df[['SHIP_CLASS', order_column_avg_name]], on='SHIP_CLASS', how='left')
        
        all_years_agg = pd.concat([all_years_agg, yearly_class_agg], ignore_index=True)

    y1_min, y1_max = all_years_agg['Total_Metric'].min(), all_years_agg['Total_Metric'].max()
    y1_range = [y1_min * 0.95, y1_max * 1.05]
    
    if use_custom_order:
        y2_min, y2_max = all_years_agg[order_column_avg_name].min(), all_years_agg[order_column_avg_name].max()
        y2_range = [y2_min * 0.95, y2_max * 1.05]
    else:
        y2_range = None
    # --- End of pre-calculation ---

    # Dynamically create columns for plots to organize them (2 columns per row)
    num_years = len(unique_years_in_product)
    cols_per_row = 2
    rows_needed = (num_years + cols_per_row - 1) // cols_per_row
    
    for row_idx in range(rows_needed):
        row_cols = st.columns(cols_per_row)
        for col_offset in range(cols_per_row):
            year_idx = row_idx * cols_per_row + col_offset
            if year_idx < num_years:
                year = unique_years_in_product[year_idx]
                with row_cols[col_offset]:
                    st.markdown(f"#### **Year: {year}**")
                    yearly_class_agg = all_years_agg[all_years_agg['FISCAL_YEAR'] == year].copy()

                    # --- Sorting Logic ---
                    if use_custom_order and order_column_avg_name in yearly_class_agg.columns:
                        is_ascending = (order_type.lower() == 'asc')
                        yearly_class_agg = yearly_class_agg.sort_values(by=order_column_avg_name, ascending=is_ascending)
                        sorting_info_text = f"by average '{order_column}' in {order_type} order."
                    else:
                        yearly_class_agg = yearly_class_agg.sort_values(by='Total_Metric', ascending=False)
                        sorting_info_text = f"by '{metric_display_name}' in descending order (default)."

                    if not yearly_class_agg.empty:
                        # --- Plotting with dual axis ---
                        if use_custom_order and order_column_avg_name in yearly_class_agg.columns:
                            if order_column in ['LOAD FACTOR', 'LF']:
                                yearly_class_agg['hover_text_line'] = f"{order_column}: " + (yearly_class_agg[order_column_avg_name] * 100).round(2).astype(str) + '%'
                                yaxis2_title = f"{order_column}"
                            elif order_column in ['GSS', 'CII']:
                                yearly_class_agg['hover_text_line'] = f"{order_column} (Average): " + yearly_class_agg[order_column_avg_name].round(2).astype(str)
                                yaxis2_title = f"{order_column} (Average)"
                            else:
                                yearly_class_agg['hover_text_line'] = f"{order_column_avg_name}: " + yearly_class_agg[order_column_avg_name].round(2).astype(str)
                                yaxis2_title = f"{order_column_avg_name}"
                            
                            yearly_class_agg['Total_Metric_Formatted'] = yearly_class_agg['Total_Metric'].round(2).astype(str)
                            
                            fig = make_subplots(specs=[[{"secondary_y": True}]])
                            fig.add_trace(
                                go.Bar(
                                    x=yearly_class_agg['SHIP_CLASS'],
                                    y=yearly_class_agg['Total_Metric'],
                                    name=f"{metric_display_name}",
                                    marker_color=[ship_class_color_map[sc] for sc in yearly_class_agg['SHIP_CLASS']],
                                    showlegend=False,
                                    hovertemplate=(
                                        f"<b>Ship Class:</b> %{{x}}<br>"
                                        f"<b>{metric_display_name}:</b> %{{y:,.2f}}<br><br>"
                                        "<b>Related Details:</b><br>%{customdata[0]}<extra></extra>"
                                    ),
                                    customdata=yearly_class_agg[['Related_Info']]
                                    ),
                                secondary_y=False,
                            )
                            fig.add_trace(
                                go.Scatter(
                                    x=yearly_class_agg['SHIP_CLASS'],
                                    y=yearly_class_agg[order_column_avg_name],
                                    mode='lines+markers',
                                    name=f"{yaxis2_title}",
                                    line=dict(color='red', width=2),
                                    marker=dict(size=8, color='red'),
                                    showlegend=True,
                                    hovertemplate= f"</b> %{{customdata[1]}}<extra></extra>",
                                    customdata=yearly_class_agg[['Total_Metric_Formatted', 'hover_text_line']]
                                ),
                                secondary_y=True,
                            )
                            fig.update_layout(
                                title_text=f"{metric_display_name} by Ship Class for {selected_product} - {year}",
                                xaxis_title="Ship Class",
                                yaxis_title=metric_display_name,
                                yaxis2_title=yaxis2_title,
                                yaxis2=dict(overlaying='y', side='right'),
                                legend=dict(x=1.05, y=1, xanchor='left', yanchor='top'),
                                hovermode="x unified"
                            )
                            fig.update_yaxes(range=y1_range, secondary_y=False)
                            fig.update_yaxes(range=y2_range, secondary_y=True)
                            fig.update_xaxes(tickangle=45)
                            st.plotly_chart(fig, use_container_width=True, key=f"prod_ship_class_yearly_{selected_product}_{year}")
                        
                        else:
                            # Fallback to single-axis bar chart using plotly.express
                            fig = px.bar(
                                yearly_class_agg, x='SHIP_CLASS', y='Total_Metric',
                                title=f"{metric_display_name} by Ship Class for {selected_product} - {year}",
                                labels={'SHIP_CLASS': 'Ship Class', 'Total_Metric': metric_display_name},
                                color='SHIP_CLASS',
                                color_discrete_map=ship_class_color_map
                            )
                            # ---- Assign per-trace customdata so Related_Info aligns with each trace's bars ----
                            # Each fig.data trace corresponds to one SHIP_CLASS (trace.name). We must assign customdata
                            # per-trace and aligned to trace.x order so Plotly doesn't broadcast the first value.
                            for trace in fig.data:
                                trace_x = list(trace.x)  # the x-values shown for this trace (ship class names)
                                customdata_for_trace = []
                                for x_val in trace_x:
                                    # Look up the Related_Info for this ship class from this year's dataframe
                                    match = yearly_class_agg.loc[yearly_class_agg['SHIP_CLASS'] == x_val, 'Related_Info']
                                    if not match.empty and pd.notna(match.values[0]):
                                        related_text = match.values[0]
                                    else:
                                        related_text = "No related items"
                                    # customdata must be a list of lists (rows), even if single column
                                    customdata_for_trace.append([related_text])
                            
                                trace.customdata = customdata_for_trace
                            
                                # per-trace hovertemplate reading the first column of customdata
                                trace.hovertemplate = (
                                    "<b>Ship Class:</b> %{x}<br>"
                                    f"<b>{metric_display_name}:</b> %{{y:,.2f}}<br><br>"
                                    "<b>Related Details:</b><br>%{customdata[0]}<extra></extra>"
                                )
                            
                            # keep unified hover
                            fig.update_layout(hovermode="x unified")
                            fig.update_yaxes(range=y1_range)
                            fig.update_xaxes(tickangle=45)
                            st.plotly_chart(fig, use_container_width=True, key=f"prod_ship_class_yearly_{selected_product}_{year}")
                        
                        # Generate insights for yearly ship class comparison
                    if len(yearly_class_agg) > 0:
                        if len(yearly_class_agg.iloc[:, 0]) == 1:
                            if use_custom_order:
                                top_class_order = yearly_class_agg.loc[yearly_class_agg[order_column_avg_name].idxmax()]
                                top_class_metric = yearly_class_agg.loc[yearly_class_agg['Total_Metric'].idxmax()]
                                insight_text = f"""
                                🚢 **Ship Class Snapshot (Product: {selected_product}, Year: {year})**
                                
                                This summary highlights the key performance metrics for the selected **Ship Class**.
                                
                                - **Total Activity ({metric_display_name}):** The **{top_class_metric['SHIP_CLASS']}** class recorded a total **{metric_display_name}** of **{top_class_metric['Total_Metric']:,.2f}**, representing its operational scale for the year.  
                                - **Operational Efficiency ({order_column}):** Performance efficiency stands at **{top_class_order[order_column_avg_name]:,.2f}**, marking it as the benchmark for fleet operations in this segment.
                                """
                            else:
                                top_class_metric = yearly_class_agg.loc[yearly_class_agg['Total_Metric'].idxmax()]
                                insight_text = f"""
                                **Ship Class Performance Summary (Product: {selected_product}, Year: {year})**
                                
                                - **{top_class_metric['SHIP_CLASS']}** recorded a {metric_display_name} of **{top_class_metric['Total_Metric']:,.2f}**, reflecting its overall performance for the selected product and year.  
                                """
                        else:
                            if use_custom_order:
                                top_class_order = yearly_class_agg.loc[yearly_class_agg[order_column_avg_name].idxmax()]
                                bottom_class_order = yearly_class_agg.loc[yearly_class_agg[order_column_avg_name].idxmin()]
                                top_class_metric = yearly_class_agg.loc[yearly_class_agg['Total_Metric'].idxmax()]
                                bottom_class_metric = yearly_class_agg.loc[yearly_class_agg['Total_Metric'].idxmin()]
                                avg_metric = yearly_class_agg['Total_Metric'].mean()
                    
                                insight_text = f"""
                                🚀 **Fleet Performance Overview (Product: {selected_product}, Year: {year})**
                                
                                This overview compares the **best and lowest performing ship classes** for the selected product and year.
                                
                                ⚓ **Total Activity: {metric_display_name}**
                                
                                * **Leading the Fleet:** The **{top_class_metric['SHIP_CLASS']}** class achieved the highest {metric_display_name}, totaling **{top_class_metric['Total_Metric']:,.2f}**.  
                                * **Trailing Segment:** The **{bottom_class_metric['SHIP_CLASS']}** class posted the lowest {metric_display_name} at **{bottom_class_metric['Total_Metric']:,.2f}**, indicating potential underutilization.  
                                * **Fleet Benchmark:** The average {metric_display_name} across all ship classes is **{avg_metric:,.2f}**.  
                                
                                ---
                                
                                ⚙️ **Operational Efficiency: {order_column}**
                                
                                * **Top Performer:** **{top_class_order['SHIP_CLASS']}** leads on efficiency with a **{order_column}** of **{top_class_order[order_column_avg_name]:,.2f}**.  
                                * **Improvement Scope:** **{bottom_class_order['SHIP_CLASS']}** registered the lowest efficiency at **{bottom_class_order[order_column_avg_name]:,.2f}**, signaling an opportunity for process optimization.  
                                """
                            else:
                                top_class_metric = yearly_class_agg.loc[yearly_class_agg['Total_Metric'].idxmax()]
                                bottom_class_metric = yearly_class_agg.loc[yearly_class_agg['Total_Metric'].idxmin()]
                                avg_metric = yearly_class_agg['Total_Metric'].mean()
                    
                                insight_text = f"""
                                🚀 **Fleet Performance Overview (Product: {selected_product}, Year: {year})**
                                
                                This overview compares the **top and bottom performing ship classes** for the selected product and year.
                                
                                ⚓ **Total Activity: {metric_display_name}**
                                
                                * **Leading the Fleet:** The **{top_class_metric['SHIP_CLASS']}** class recorded the highest {metric_display_name} of **{top_class_metric['Total_Metric']:,.2f}**, setting the benchmark for the year.  
                                * **Trailing Segment:** The **{bottom_class_metric['SHIP_CLASS']}** class registered the lowest total at **{bottom_class_metric['Total_Metric']:,.2f}**, marking it as an area for further review.  
                                * **Fleet Benchmark:** The average {metric_display_name} across all ship classes is **{avg_metric:,.2f}**.  
                                """
                    
                        st.markdown(insight_text)
                        add_insight(
                            st.session_state["all_insights"],
                            title=f"Ship Class Performance for {selected_product} - {year}",
                            text=insight_text,
                            chart=fig
                        )
                    
                    else:
                        st.info(f"No ship class data available for product '{selected_product}' for year {year}.")
                        add_insight(
                            st.session_state["all_insights"],
                            title=f"Ship Class Performance for {selected_product} - {year}",
                            text=f"No ship class data available for {selected_product} for year {year}."
                        )


#Ship Class Trend Over Time( product centric)
def plot_ship_class_over_years(df, primary_metric_col, metric_display_name):
    st.markdown("### Ship Class Trend Over Time")

    # Filter invalid or zero metric rows
    df = df[df['SHIP_CD'].notna() & df[primary_metric_col].notna()]
    df = df[df[primary_metric_col] != 0]

    # Create quarter column from ACCOUNTING_PERIOD (assumed numeric 1–12)
    df['QUARTER'] = ((df['ACCOUNTING_PERIOD'] - 1) // 3 + 1).astype(int)
    df['QUARTER_LABEL'] = df['FISCAL_YEAR'].astype(str) + "-Q" + df['QUARTER'].astype(str)

    # Create PERIOD_DATE and PERIOD_LABEL from month
    df['PERIOD_DATE'] = pd.to_datetime(df['FISCAL_YEAR'].astype(str) + "-" + df['ACCOUNTING_PERIOD'].astype(str).str.zfill(2) + "-01")
    df['PERIOD_LABEL'] = df['FISCAL_YEAR'].astype(str) + "-" + df['ACCOUNTING_PERIOD'].astype(str).str.zfill(2)

    # Toggle for Month vs Quarter view
    view_type = st.radio("View By:", options=["Monthly", "Quarterly"], horizontal=True)

    if view_type == "Quarterly":
        grouped = df.groupby(['QUARTER_LABEL', 'SHIP_CD'])[primary_metric_col].sum().reset_index()
        grouped['ORDER'] = grouped['QUARTER_LABEL'].apply(lambda x: int(x.split('-')[0]) * 10 + int(x[-1]))
        grouped = grouped.sort_values('ORDER')
        x_col = 'QUARTER_LABEL'
    else:
        grouped = df.groupby(['PERIOD_LABEL', 'PERIOD_DATE', 'SHIP_CD'])[primary_metric_col].sum().reset_index()
        grouped = grouped.sort_values('PERIOD_DATE')
        x_col = 'PERIOD_LABEL'

    if grouped.empty:
        st.info("No valid ship class trend data found.")
        return

    fig = px.line(
        grouped,
        x=x_col,
        y=primary_metric_col,
        color='SHIP_CD',
        markers=True,
        labels={
            x_col: "Period",
            primary_metric_col: metric_display_name,
            'SHIP_CD': 'Ship CD'
        },
        title=f"{metric_display_name} Trend by Ships over Time ({view_type})"
    )

    fig.update_layout(
        xaxis_title="Period",
        yaxis_title=metric_display_name,
        xaxis=dict(type='category', tickangle=-45)
    )

    st.plotly_chart(fig, use_container_width=True)

       
# 4.Compare ships of same class across the year ( product centric) 

def plot_ship_class_yearly_comparison(df, selected_ship_class, metric_col, metric_display_name, order_column=None, order_type=None):

    global all_insights
    st.subheader(f"📈 Ship Comparison within {selected_ship_class} by Year")
    
    # --- Handle None for order_column and order_type ---
    use_custom_order = (order_column is not None) and (order_type is not None)
    
    # Map the order_column name to the actual DataFrame column name
    order_col_for_agg = None
    if use_custom_order:
        order_column = order_column.upper()
        if order_column in ['CII', 'CII_SCORE']:
            order_col_for_agg = 'CII_SCORE'
        elif order_column in ['GSS']:
            order_col_for_agg = 'GSS'
        elif order_column in ['LOAD FACTOR', 'LF']:
            order_col_for_agg = 'PAX_DAYS' # Use PAX_DAYS for the complex LF calculation
        order_column_avg_name = f"{order_column}_Average"
        sorting_info_text = f"*Graph is sorted by average '{order_column}' in {order_type} order.*"
        st.info(sorting_info_text)
    else:
        order_column_avg_name = None
        sorting_info_text = f"*Graph is sorted by '{metric_display_name}' in descending order (default).*"
        st.info(sorting_info_text)

    df_filtered_class = df[df['SHIP_CLASS'] == selected_ship_class].copy()

    if df_filtered_class.empty:
        st.warning(f"No data for ship class '{selected_ship_class}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Ship Comparison within {selected_ship_class}", text = f"No data for ship class '{selected_ship_class}'.")
        return
    
    unique_years_in_class = sorted(df_filtered_class['FISCAL_YEAR'].dropna().unique().tolist())

    if not unique_years_in_class:
        st.info(f"No yearly data available for ship class '{selected_ship_class}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Ship Comparison within {selected_ship_class}", text = f"No yearly data available for ship class '{selected_ship_class}'.")
        return

    # Create a consistent color map for all ships
    all_unique_ships = sorted(df['SHIP_CD'].dropna().unique().tolist())
    colors = px.colors.qualitative.D3
    ship_color_map = {ship: colors[i % len(colors)] for i, ship in enumerate(all_unique_ships)}

    # First pass: compute global max Total_Metric across all years
    max_metric_value = 0
    for year in unique_years_in_class:
        year_df = df_filtered_class[df_filtered_class['FISCAL_YEAR'] == year].copy()
    
        if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
            # Aggregate component amount per ship
            df1 = year_df.groupby('SHIP_CD', as_index=False)['COMPONENT_AMOUNT'].sum()
    
            # Take first value of metric_col at voyage level, then sum per ship
            df2 = (
                year_df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )[metric_col]
                .first()
                .reset_index()
                .groupby('SHIP_CD', as_index=False)[metric_col]
                .sum()
                .rename(columns={metric_col: 't1'})
            )
    
            yearly_ship_agg = df1.merge(df2, on='SHIP_CD', how='inner')
            yearly_ship_agg['Total_Metric'] = yearly_ship_agg['COMPONENT_AMOUNT'] / yearly_ship_agg['t1']
    
        elif metric_display_name == 'Passenger Days':
            df2 = (
                year_df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_PAX_DAYS']
                .first()
                .reset_index()
                .groupby('SHIP_CD', as_index=False)['NEW_PRTD_PAX_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
            )
            yearly_ship_agg = df2
    
        elif metric_display_name == 'Capacity Days':
            df2 = (
                year_df.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_CAPS_DAYS']
                .first()
                .reset_index()
                .groupby('SHIP_CD', as_index=False)['NEW_PRTD_CAPS_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
            )
            yearly_ship_agg = df2
    
        else:
            yearly_ship_agg = (
                year_df.groupby('SHIP_CD', as_index=False)
                .agg(Total_Metric=(metric_col, 'sum'))
            )
    
        # Update max value if results exist
        if not yearly_ship_agg.empty:
            max_metric_value = max(max_metric_value, yearly_ship_agg['Total_Metric'].max())

    
    # Dynamically create columns for plots to organize them
    num_years = len(unique_years_in_class)
    cols_per_row = 2
    rows_needed = (num_years + cols_per_row - 1) // cols_per_row

    for row_idx in range(rows_needed):
        row_cols = st.columns(cols_per_row)
        for col_offset in range(cols_per_row):
            year_idx = row_idx * cols_per_row + col_offset
            if year_idx < num_years:
                year = unique_years_in_class[year_idx]
                with row_cols[col_offset]:
                    st.markdown(f"#### **Year: {year}**")
                    year_df = df_filtered_class[df_filtered_class['FISCAL_YEAR'] == year].copy()
                    
                    # --- Aggregation Logic for Primary Metric ---

                    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
                        df1 = year_df.groupby('SHIP_CD', as_index=False)['COMPONENT_AMOUNT'].sum()
                        df2 = (
                            year_df.groupby(
                                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                            )[metric_col]
                            .first()
                            .reset_index()
                            .groupby('SHIP_CD', as_index=False)[metric_col]
                            .sum()
                            .rename(columns={metric_col: 't1'})
                        )
                        yearly_ship_agg = df1.merge(df2, on='SHIP_CD', how='inner')
                        yearly_ship_agg['Total_Metric'] = yearly_ship_agg['COMPONENT_AMOUNT'] / yearly_ship_agg['t1']
                    
                    elif metric_display_name == 'Passenger Days':
                        df2 = (
                            year_df.groupby(
                                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                            )['NEW_PRTD_PAX_DAYS']
                            .first()
                            .reset_index()
                            .groupby('SHIP_CD', as_index=False)['NEW_PRTD_PAX_DAYS']
                            .sum()
                            .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
                        )
                        yearly_ship_agg = df2
                    
                    elif metric_display_name == 'Capacity Days':
                        df2 = (
                            year_df.groupby(
                                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                            )['NEW_PRTD_CAPS_DAYS']
                            .first()
                            .reset_index()
                            .groupby('SHIP_CD', as_index=False)['NEW_PRTD_CAPS_DAYS']
                            .sum()
                            .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
                        )
                        yearly_ship_agg = df2
                    
                    else:
                        yearly_ship_agg = (
                            year_df.groupby('SHIP_CD', as_index=False)
                            .agg(Total_Metric=(metric_col, 'sum'))
                        )

                    
                    # --- Aggregation Logic for Order Column ---
                    if use_custom_order:
                        if order_column in ['GSS', 'CII']:
                            order_col_for_agg = 'CII_SCORE' if order_column == 'CII' else 'GSS'
                            order_col_avg_df = year_df.groupby(['VOYAGE_ID', 'SHIP_CD'], as_index=False)[order_col_for_agg].first()
                            order_col_avg_df = order_col_avg_df.groupby('SHIP_CD', as_index=False)[order_col_for_agg].mean().rename(columns={order_col_for_agg: order_column_avg_name})
                            yearly_ship_agg = yearly_ship_agg.merge(order_col_avg_df, on='SHIP_CD', how='left')

                        elif order_column in ['LOAD FACTOR', 'LF']:
                            temp_df = year_df.groupby(['VOYAGE_ID', 'SHIP_CD'], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
                            temp_df = temp_df.groupby('SHIP_CD', as_index=False).sum()
                            temp_df[order_column_avg_name] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
                            yearly_ship_agg = yearly_ship_agg.merge(temp_df[['SHIP_CD', order_column_avg_name]], on='SHIP_CD', how='left')

                    # --- Sorting Logic ---
                    if use_custom_order and order_column_avg_name in yearly_ship_agg.columns:
                        is_ascending = (order_type.lower() == 'asc')
                        yearly_ship_agg = yearly_ship_agg.sort_values(by=order_column_avg_name, ascending=is_ascending)
                        sorting_info_text = f"by average '{order_column}' in {order_type} order."
                    else:
                        yearly_ship_agg = yearly_ship_agg.sort_values(by='Total_Metric', ascending=False)
                        sorting_info_text = f"by '{metric_display_name}' in descending order (default)."
                    
                    # --- Add Related Detail: Unique Voyages per Ship ---
                    voyage_counts = (
                        year_df.groupby('SHIP_CD')['VOYAGE_ID'].nunique().reset_index(name='Unique_Voyages')
                    )
                    yearly_ship_agg = yearly_ship_agg.merge(voyage_counts, on='SHIP_CD', how='left')
                    yearly_ship_agg['Related_Info'] = (
                        "Unique Voyages: " + yearly_ship_agg['Unique_Voyages'].astype(int).astype(str)
                    )

                    if not yearly_ship_agg.empty:
                        # --- Plotting with dual axis ---
                        if use_custom_order and order_column_avg_name in yearly_ship_agg.columns:
                            # Create a fully formatted string for the tooltip's sorting metric line
                            if order_column in ['LOAD FACTOR', 'LF']:
                                yearly_ship_agg['hover_text_line'] = f"{order_column}: " + (yearly_ship_agg[order_column_avg_name] * 100).round(2).astype(str) + '%'
                                yaxis2_title = f"{order_column}"
                            elif order_column in ['GSS', 'CII']:
                                yearly_ship_agg['hover_text_line'] = f"{order_column} (Average): " + yearly_ship_agg[order_column_avg_name].round(2).astype(str)
                                yaxis2_title = f"{order_column} (Average)"
                            else:
                                yearly_ship_agg['hover_text_line'] = f"{order_column_avg_name}: " + yearly_ship_agg[order_column_avg_name].round(2).astype(str)
                                yaxis2_title = f"{order_column_avg_name}"
                            
                            yearly_ship_agg['Total_Metric_Formatted'] = yearly_ship_agg['Total_Metric'].round(2).astype(str)
                            

                            fig = make_subplots(specs=[[{"secondary_y": True}]])
                            
                            # Add bar trace
                            fig.add_trace(
                                go.Bar(
                                    x=yearly_ship_agg['SHIP_CD'],
                                    y=yearly_ship_agg['Total_Metric'],
                                    name=f"{metric_display_name}",
                                    marker_color=[ship_color_map[sc] for sc in yearly_ship_agg['SHIP_CD']],
                                    showlegend=False,
                                    hovertemplate=(
                                        "<b>Ship:</b> %{x}<br>"
                                        f"<b>{metric_display_name}:</b> %{{y:,.2f}}<br>"
                                        "%{customdata[1]}<br>"
                                        "<b>Related Details:</b> %{customdata[2]}<extra></extra>"
                                    ),
                                    customdata=yearly_ship_agg[['Total_Metric_Formatted', 'hover_text_line', 'Related_Info']]
                                ),
                                secondary_y=False,
                            )

                            # Add line trace for the order column
                            fig.add_trace(
                                go.Scatter(
                                    x=yearly_ship_agg['SHIP_CD'],
                                    y=yearly_ship_agg[order_column_avg_name],
                                    mode='lines+markers',
                                    name=f"{yaxis2_title}",
                                    line=dict(color='red', width=2),
                                    marker=dict(size=8, color='red'),
                                    showlegend=True,
                                    hovertemplate="<b>%{customdata[1]}</b><extra></extra>",
                                    customdata=yearly_ship_agg[['Total_Metric_Formatted', 'hover_text_line', 'Related_Info']]
                                ),
                                secondary_y=True,
                            )

                            # Update layout
                            fig.update_layout(
                                title_text=f"{metric_display_name} for Ships in {selected_ship_class} - {year}",
                                xaxis_title="Ship",
                                yaxis_title=metric_display_name,
                                yaxis2_title=yaxis2_title,
                                yaxis2=dict(overlaying='y', side='right'),
                                legend=dict(x=1.05, y=1, xanchor='left', yanchor='top'),
                                hovermode="x unified"
                            )
                            fig.update_xaxes(tickangle=45)
                            fig.update_yaxes(range=[0, max_metric_value * 1.15], secondary_y=False) # Apply uniform Y-axis to primary axis
                            st.plotly_chart(fig, use_container_width=True, key=f"ship_class_comp_{selected_ship_class}_{year}")
                        
                        else:
                            # Fallback to single-axis bar chart using plotly.express
                            fig = px.bar(
                                yearly_ship_agg, x='SHIP_CD', y='Total_Metric',
                                title=f"{metric_display_name} for Ships in {selected_ship_class} - {year}",
                                labels={'SHIP_CD': 'Ship', 'Total_Metric': metric_display_name},
                                color='SHIP_CD',
                                color_discrete_map=ship_color_map
                            )
                            # Assign per-trace Related Info correctly (like previous fix)
                            for trace in fig.data:
                                trace_x = list(trace.x)
                                customdata_for_trace = []
                                for x_val in trace_x:
                                    match = yearly_ship_agg.loc[yearly_ship_agg['SHIP_CD'] == x_val, 'Related_Info']
                                    related_text = match.values[0] if not match.empty else "No related info"
                                    customdata_for_trace.append([related_text])
                                trace.customdata = customdata_for_trace
                                trace.hovertemplate = (
                                    "<b>Ship:</b> %{x}<br>"
                                    f"<b>{metric_display_name}:</b> %{{y:,.2f}}<br>"
                                    "<b>Related Details:</b> %{customdata[0]}<extra></extra>"
                                )
                            
                            fig.update_layout(hovermode="x unified")
                            fig.update_xaxes(tickangle=45)
                            fig.update_yaxes(range=[0, max_metric_value * 1.15])
                            st.plotly_chart(fig, use_container_width=True, key=f"ship_class_comp_{selected_ship_class}_{year}")

                        # Generate insights for yearly ship comparison
                        if len(yearly_ship_agg) > 0:
                            if len(yearly_ship_agg.iloc[:, 0]) == 1:
                                if use_custom_order:
                                    top_ship_order = yearly_ship_agg.loc[yearly_ship_agg[order_column_avg_name].idxmax()]
                                    top_ship_metric = yearly_ship_agg.loc[yearly_ship_agg['Total_Metric'].idxmax()]
                                    insight_text = f"""
                                    🚢 **Ship Performance Snapshot ({selected_ship_class}, {year})**
                                    
                                    This summary provides a focused view of performance for the selected **ship** within the **{selected_ship_class}** class.
                                    
                                    - **Operational Scale ({metric_display_name}):** Ship **{top_ship_metric['SHIP_CD']}** recorded a total **{metric_display_name}** of **{top_ship_metric['Total_Metric']:,.2f}**, representing its overall contribution during the year.  
                                    - **Operational Efficiency ({order_column}):** Efficiency measured at **{top_ship_order[order_column_avg_name]:,.2f}**, setting a benchmark for other vessels in this class.  
                                    """
                                else:
                                    top_ship_metric = yearly_ship_agg.loc[yearly_ship_agg['Total_Metric'].idxmax()]
                                    insight_text = f"""
                                    **Ship Performance Summary ({selected_ship_class}, {year})**
                                    
                                    - **{top_ship_metric['SHIP_CD']}** achieved a {metric_display_name} of **{top_ship_metric['Total_Metric']:,.2f}**, reflecting its overall performance within the {selected_ship_class} class for the year.  
                                    """
                            else:
                                if use_custom_order:
                                    top_ship_order = yearly_ship_agg.loc[yearly_ship_agg[order_column_avg_name].idxmax()]
                                    bottom_ship_order = yearly_ship_agg.loc[yearly_ship_agg[order_column_avg_name].idxmin()]
                                    top_ship_metric = yearly_ship_agg.loc[yearly_ship_agg['Total_Metric'].idxmax()]
                                    bottom_ship_metric = yearly_ship_agg.loc[yearly_ship_agg['Total_Metric'].idxmin()]
                                    avg_metric = yearly_ship_agg['Total_Metric'].mean()
                        
                                    insight_text = f"""
                                    🚀 **Fleet Comparison Insights ({selected_ship_class}, {year})**
                                    
                                    This overview compares the **top and bottom performing ships** within the **{selected_ship_class}** class for {year}.
                                    
                                    ⚓ **Total Activity: {metric_display_name}**
                                    
                                    * **Top Performing Ship:** **{top_ship_metric['SHIP_CD']}** led the fleet with a total **{metric_display_name}** of **{top_ship_metric['Total_Metric']:,.2f}**.  
                                    * **Lowest Performing Ship:** **{bottom_ship_metric['SHIP_CD']}** registered the lowest {metric_display_name}, recording **{bottom_ship_metric['Total_Metric']:,.2f}**.  
                                    * **Fleet Benchmark:** The average {metric_display_name} across ships in this class is **{avg_metric:,.2f}**.  
                                    
                                    ---
                                    
                                    ⚙️ **Operational Efficiency: {order_column}**
                                    
                                    * **Highest Efficiency:** **{top_ship_order['SHIP_CD']}** demonstrated the strongest operational efficiency, scoring **{top_ship_order[order_column_avg_name]:,.2f}**.  
                                    * **Improvement Area:** **{bottom_ship_order['SHIP_CD']}** recorded the lowest efficiency at **{bottom_ship_order[order_column_avg_name]:,.2f}**, suggesting potential operational bottlenecks.  
                                    """
                                else:
                                    top_ship_metric = yearly_ship_agg.loc[yearly_ship_agg['Total_Metric'].idxmax()]
                                    bottom_ship_metric = yearly_ship_agg.loc[yearly_ship_agg['Total_Metric'].idxmin()]
                                    avg_metric = yearly_ship_agg['Total_Metric'].mean()
                        
                                    insight_text = f"""
                                    🚀 **Fleet Comparison Insights ({selected_ship_class}, {year})**
                                    
                                    This overview highlights the **best and lowest performing ships** in the **{selected_ship_class}** class for the selected year.
                                    
                                    ⚓ **Total Activity: {metric_display_name}**
                                    
                                    * **Top Performing Ship:** **{top_ship_metric['SHIP_CD']}** recorded a total **{metric_display_name}** of **{top_ship_metric['Total_Metric']:,.2f}**, setting the performance standard for the year.  
                                    * **Lowest Performing Ship:** **{bottom_ship_metric['SHIP_CD']}** recorded the minimum value at **{bottom_ship_metric['Total_Metric']:,.2f}**, marking it for deeper review.  
                                    * **Fleet Benchmark:** The average {metric_display_name} across ships in this class is **{avg_metric:,.2f}**.  
                                    """
                        
                            st.markdown(insight_text)
                            add_insight(
                                st.session_state["all_insights"],
                                title=f"Ship Comparison in {selected_ship_class} - {year}",
                                text=insight_text,
                                chart=fig
                            )
                        
                        else:
                            st.info(f"No data for ships in {selected_ship_class} for year {year}.")
                            add_insight(
                                st.session_state["all_insights"],
                                title=f"Ship Comparison in {selected_ship_class} - {year}",
                                text=f"No data for ships in {selected_ship_class} for year {year}."
                            )


# 5.component breakdown by ship class( product centric)
def plot_component_breakdown_by_ship_class(df, metric_col, metric_display_name):

    global all_insights
    st.subheader(f"📊 Component Breakdown by Ship Class")

    # Aggregate Margin by Ship Class and Component

    if metric_display_name in ('Passenger Days', 'Capacity Days'):
        st.info("Passenger Days and Capacity Days are identical across all components, so this metric isn’t meaningful for component-level breakdowns.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Contribution for SHIPCLASS", 
            text = f"{metric_display_name} is not applicable for component-level breakdowns."
        )
        return
    if metric_display_name in ('Per Capacity Day (Margin PCD)','Per Passenger Day (Margin PPD)'):
        df1 = df.groupby(['SHIP_CLASS', 'M0_AND_M1'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID','M0_AND_M1'])[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['SHIP_CLASS', 'M0_AND_M1'], as_index=False)['t1'].sum()
        component_breakdown = df1.merge(df2, on=['SHIP_CLASS', 'M0_AND_M1'], how='inner')
        component_breakdown['Total_Amount'] = component_breakdown['COMPONENT_AMOUNT'] / component_breakdown['t1']
    else:
        component_breakdown = df.groupby(['SHIP_CLASS', 'M0_AND_M1']).agg(
            Total_Amount=('COMPONENT_AMOUNT', 'sum')
        ).reset_index()


    
    if component_breakdown.empty:
        st.warning("No data available for component breakdown by ship class.")
        add_insight(     st.session_state["all_insights"],     title= "Component Breakdown by Ship Class", text = "No data available.")
        return

        # Use a grouped bar chart to show component breakdown per ship class
    # Step 1: Create a color column
    component_breakdown['ColorLabel'] = component_breakdown['Total_Amount'].apply(
    lambda x: 'Revenue' if x >= 0 else 'Cost')

    
    # Step 2: Plot using grouped bars, grouped by component, but colored by green/red
    fig = px.bar(
        component_breakdown,
        x='M0_AND_M1',
        y='Total_Amount',
        color='ColorLabel',  # This will be either green or red  # Optional: split each component into a separate panel
        title="Component Breakdown by Ship Class",
        labels={
            'M0_AND_M1': 'M0_AND_M1',
            'Total_Amount': 'Margin',
            'ColorLabel': 'M0&M1'
        },
        barmode='group',
        color_discrete_map={'Revenue': 'green', 'Cost': 'red'}
    )

# Group bars by component within each ship class
 # Use a vivid palette for components
    
    fig.update_layout(xaxis_title="Ship Class", yaxis_title="Margin")
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True, key="comp_breakdown_ship_class")

    # Generate insights
    if not component_breakdown.empty:
        total_per_class = component_breakdown.groupby('SHIP_CLASS')['Total_Amount'].sum().reset_index()
        top_class_overall = total_per_class.loc[total_per_class['Total_Amount'].idxmax()]
        bottom_class_overall = total_per_class.loc[total_per_class['Total_Amount'].idxmin()] if len(total_per_class) > 1 else None

        insight_text = f"""
        **Findings for Component Breakdown by Ship Class:**
        - **Highest Total Margin**: **{top_class_overall['SHIP_CLASS']}** with total amount of {top_class_overall['Total_Amount']:,.2f}.
        """
        st.markdown(insight_text)
        add_insight(     st.session_state["all_insights"],     title= "Component Breakdown by Ship Class", text = insight_text, chart = fig)

# 6.Shows components box plot for a selected product and ship ( ship centric)
def plot_component_boxplot_zscore(df, selected_product, selected_ship,metric_col, metric_display_name, type_option, selected_m0_m1_components):
    """Shows components box plot for a selected product and ship."""

    global all_insights


    st.warning("##### 📢 Please note:\n Outlier analysis is limited to **2023 and 2024**. Due to incomplete data, **results for 2025 may not be reliable**.")

    selected_metric_display_name = metric_display_name
    selected_ship_for_detail = selected_ship
    global all_insights

    df = df[df['VOYAGEBUCKET']!='Bucket 1']
    st.subheader(f"📦 Component Box Plot for {selected_ship} (Product: {selected_product})")

    df_filtered = df[
        (df['RM_ROLLUP_PRODUCT_DESC'] == selected_product) &
        (df['SHIP_CD'] == selected_ship)
    ].copy()

    df_filtered = df_filtered[df_filtered['M0_AND_M1'].isin(selected_m0_m1_components)].copy()

    if df_filtered.empty:
        st.warning(f"No data for selected product '{selected_product}' and ship '{selected_ship}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Box Plot for {selected_ship} ({selected_product})", text = "No data available.")
        return

    if metric_display_name in ('Passenger Days', 'Capacity Days'):
        st.info(f"{metric_display_name} is identical across components, so it’s excluded from this analysis.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Box Plot for {selected_ship} ({selected_product})",
            text = f"{metric_display_name} is not applicable for component-level breakdowns."
        )
        return pd.DataFrame(), None

    # Ensure 'COMPONENT_AMOUNT' is the column for Z-score calculation
    if 'COMPONENT_AMOUNT' not in df_filtered.columns:
        st.error("COMPONENT_AMOUNT column not found for calculation.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Box Plot for {selected_ship} ({selected_product})", text = "COMPONENT_AMOUNT column not found.")
        return

    # Calculate Z-scores per component. Groupby M0_AND_M1 for context-aware Z-scores.
    # Define the base grouping for aggregation
    agg_cols = ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'M0_AND_M1']

    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        # 1. Aggregate COMPONENT_AMOUNT at Year-Month level
        df1 = df_filtered.groupby(agg_cols, as_index=False)['COMPONENT_AMOUNT'].sum()
    
        # 2. Get first metric_col per voyage, then sum to Year-Month level
        df2 = df_filtered.groupby(
            ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID', 'M0_AND_M1']
        )[metric_col].first().reset_index(name='t1')
    
        df2 = df2.groupby(agg_cols, as_index=False)['t1'].sum()
    
        # 3. Merge and calculate Total_Amount
        component_breakdown = pd.merge(df1, df2, on=agg_cols, how='inner')
        component_breakdown['Total_Amount'] = component_breakdown['COMPONENT_AMOUNT'] / component_breakdown['t1']
        # 4. Z-score across all Year-Month values per component (M0_AND_M1 only)
        component_breakdown['Z_Score'] = component_breakdown.groupby('M0_AND_M1')['Total_Amount'].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0
        )
        component_breakdown['Z_Score'] = component_breakdown['Z_Score'].fillna(0)
    
        df_filtered = component_breakdown
    
    else:
        # Z-score for raw COMPONENT_AMOUNT — aggregate at Year-Month level first
        df_grouped = df_filtered.groupby(agg_cols, as_index=False)['COMPONENT_AMOUNT'].sum()
    
        df_grouped['Z_Score'] = df_grouped.groupby('M0_AND_M1')['COMPONENT_AMOUNT'].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0
        )
        df_grouped['Z_Score'] = df_grouped['Z_Score'].fillna(0)
    
        df_filtered = df_grouped
        df_filtered['Metric'] = df_filtered['COMPONENT_AMOUNT']
    
    if not df_filtered.empty:
        # Add Year-Month column for tooltip
        df_filtered['YEAR_MONTH'] = df_filtered['FISCAL_YEAR'].astype(str) + '-' + df_filtered['ACCOUNTING_PERIOD'].astype(str).str.zfill(2)
        if metric_display_name == 'Per Capacity Day (Margin PCD)':
            df_filtered['PCD'] = df_filtered['Total_Amount']
            df_filtered['Metric'] = df_filtered['Total_Amount']
            hover_metric = 'PCD'
        elif metric_display_name == 'Per Passenger Day (Margin PPD)':
            df_filtered['PPD'] = df_filtered['Total_Amount']
            df_filtered['Metric'] = df_filtered['Total_Amount']
            hover_metric = 'PPD'
        else:
            hover_metric = None  # No normalized value to show

    grouped_df_detail = df_filtered

    grouped_df_detail['Outlier'] = False
    outliers_list_detail = [] 

    actual_components_in_detail_df = grouped_df_detail['M0_AND_M1'].unique()
    components_to_process_for_outliers_detail = actual_components_in_detail_df

    for comp_name_iter in components_to_process_for_outliers_detail: 
        comp_data_iter = grouped_df_detail[grouped_df_detail['M0_AND_M1'] == comp_name_iter].copy() 
        if comp_data_iter.empty or comp_data_iter['Metric'].isnull().all():
            continue
        q1_iter = comp_data_iter['Metric'].quantile(0.25)
        q3_iter = comp_data_iter['Metric'].quantile(0.75)
        iqr_iter = q3_iter - q1_iter
        lower_bound_iter = q1_iter - 1.5 * iqr_iter
        mask_iter = (comp_data_iter['Metric'] < lower_bound_iter)
        original_indices = comp_data_iter.index[mask_iter]
        grouped_df_detail.loc[original_indices, 'Outlier'] = True
        if mask_iter.any():
            outliers_list_detail.append(comp_data_iter[mask_iter])

    outliers_df_detail = pd.DataFrame()
    if outliers_list_detail:
        outliers_df_detail = pd.concat(outliers_list_detail)
        if not outliers_df_detail.empty: 
             outliers_df_detail = outliers_df_detail.sort_values(by='Z_Score', key=lambda x: x.abs(), ascending=False)

    # --- PLOT 1 (Boxplot) ---
    fig1 = go.Figure()
    components_in_data_plot1 = grouped_df_detail['M0_AND_M1'].unique()

    for comp in components_in_data_plot1:
        comp_df_fig1 = grouped_df_detail[grouped_df_detail['M0_AND_M1'] == comp] 
        if comp_df_fig1.empty:
            continue
        raw_comp_mean = comp_df_fig1['Metric'].mean()
        raw_comp_median = comp_df_fig1['Metric'].median()
        raw_comp_q1 = comp_df_fig1['Metric'].quantile(0.25)
        raw_comp_q3 = comp_df_fig1['Metric'].quantile(0.75)
        raw_comp_min = comp_df_fig1['Metric'].min()
        raw_comp_max = comp_df_fig1['Metric'].max()

        custom_data_for_points = comp_df_fig1.apply(lambda row: [
            row['Metric'], row['Z_Score'],
            raw_comp_mean, raw_comp_median, raw_comp_q1, raw_comp_q3, raw_comp_min, raw_comp_max,
            row['FISCAL_YEAR'], row['ACCOUNTING_PERIOD'] 
        ], axis=1).tolist()
        
        fig1.add_trace(go.Box(
            y=comp_df_fig1['Z_Score'],
            x=[comp] * len(comp_df_fig1), 
            name=comp,
            boxpoints='all', 
            jitter=0.5,
            pointpos=-1.8,
            marker=dict(color='skyblue', size=6, opacity=0.7),
            line=dict(color='darkcyan'),
            width=0.4, 
            customdata=custom_data_for_points,
            hovertemplate=(
                f"<b>Component: {comp}</b><br>"
                "Fiscal Year: %{customdata[8]}<br>"
                "Month: %{customdata[9]}<br>"
                f"{selected_metric_display_name}: %{{customdata[0]:.2f}}<br>"
                "Z-Score: %{customdata[1]:.2f}<br>"
                "<br>--- Component Stats ---<br>"
                f"Mean ({selected_metric_display_name}): %{{customdata[2]:.2f}}<br>"
                f"Median ({selected_metric_display_name}): %{{customdata[3]:.2f}}<br>"
                f"Q1 ({selected_metric_display_name}): %{{customdata[4]:.2f}}<br>"
                f"Q3 ({selected_metric_display_name}): %{{customdata[5]:.2f}}<br>"
                f"Min ({selected_metric_display_name}): %{{customdata[6]:.2f}}<br>"
                f"Max ({selected_metric_display_name}): %{{customdata[7]:.2f}}<br>"
                "<extra></extra>" 
            ),
            showlegend=False
        ))

        outlier_points_fig1 = comp_df_fig1[comp_df_fig1['Outlier']]
        if not outlier_points_fig1.empty:
            custom_data_for_outliers_fig1 = outlier_points_fig1.apply(lambda row: [
                row['Metric'], row['Z_Score'],
                raw_comp_mean, raw_comp_median, raw_comp_q1, raw_comp_q3, raw_comp_min, raw_comp_max,
                row['FISCAL_YEAR'], row['ACCOUNTING_PERIOD']
            ], axis=1).tolist()

            fig1.add_trace(go.Scatter(
                x=[comp] * len(outlier_points_fig1),
                y=outlier_points_fig1['Z_Score'],
                mode='markers',
                marker=dict(color='red', size=10, symbol='circle', line=dict(width=0, color='DarkRed')), 
                name=f"{comp} Outliers",
                customdata=custom_data_for_outliers_fig1, 
                 hovertemplate=( 
                    f"<b>OUTLIER - Component: {comp}</b><br>"
                    "Fiscal Year: %{customdata[8]}<br>"
                    "Month: %{customdata[9]}<br>"
                    f"{selected_metric_display_name}: %{{customdata[0]:.2f}}<br>"
                    "Z-Score: %{customdata[1]:.2f}<br>"
                    "<extra></extra>"
                ),
                showlegend=False
            ))
            
    fig1.update_layout(
        title_text=f"Normalized by Component for {selected_ship_for_detail}<br>(Metric: {selected_metric_display_name})",
        yaxis_title=f"{selected_metric_display_name}",
        xaxis_title=f"Component",
        template="plotly_white",
        boxmode='group', 
        width=1200,
        height=700, 
        margin=dict(l=50, r=50, t=100, b=80), 
        xaxis=dict(tickangle=45) 
    )
    st.plotly_chart(fig1, use_container_width=True) 

    # Detailed Outlier Table for Boxplot
    if not outliers_df_detail.empty:
        insight_df = outliers_df_detail[
            ['M0_AND_M1', 'FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'Metric']
        ].sort_values(by='Metric', ascending=True)

    
        if not insight_df.empty:
            st.markdown(f"### 📌 All {type_option} Outliers for {selected_ship_for_detail}, {selected_product}")
            st.dataframe(insight_df.rename(columns={
                'M0_AND_M1': 'Component',
                'FISCAL_YEAR': 'Fiscal Year',
                'ACCOUNTING_PERIOD': 'Month',
                'Metric': selected_metric_display_name
            }), use_container_width=True)

            # Convert rows to bullet points
            outlier_bullets = "\n".join(
                f"- Component: {row['M0_AND_M1']}, Year: {row['FISCAL_YEAR']}, "
                f"Month: {row['ACCOUNTING_PERIOD']}, {selected_metric_display_name}: {row['Metric']:,.2f}"
                for _, row in insight_df.iterrows()
            )
            st.write('The data points highlighted as outlier in the amay not be an ideal outlier it may be due to  ')
            # Final formatted text
            insight_text = f"""
            Sorted Outliers Overview (All {type_option} Components):
            {outlier_bullets}
            """
            
            # Append to the `all_insights` list
            add_insight(     st.session_state["all_insights"],     title= f"Outlier Details (All {type_option} Components), for {selected_ship_for_detail}, {selected_product}",
                text = insight_text, chart = fig1
            )


    # --- PLOT 2 (Component Timeseries) ---
    st.subheader(f"📈 {selected_metric_display_name} Distribution Time Series for {type_option} by Year (Ship - *{selected_ship_for_detail}*, Product - *{selected_product}*)") # Added emoji
    options_for_plot2_filter = ["-- Select Component --"] + sorted([str(item) for item in grouped_df_detail['M0_AND_M1'].unique()])
    
    if not options_for_plot2_filter:
        st.warning(f"No M0_&_M1 components available for Plot 2 for *{selected_ship_for_detail}*.")
        return

    st.markdown("#### 🧩 Select the Component to compare yearly values ")
    selected_m0m1_for_plot2 = st.selectbox(
        f"Select Component for Time Series (Ship: *{selected_ship_for_detail}*,Non-Fuel):",
        options_for_plot2_filter,
        key="selectbox_plot2_component_detail" 
    )

    # ✅ Only show plot if something is selected
    if selected_m0m1_for_plot2!= "-- Select Component --":

        df_plot2_component_detail = grouped_df_detail[grouped_df_detail['M0_AND_M1'] == selected_m0m1_for_plot2].copy()
        
        if df_plot2_component_detail.empty:
            st.warning(f"No data for component '{selected_m0m1_for_plot2}' on {selected_ship_for_detail}.")
            return
    
        fig2 = go.Figure()
        if not df_plot2_component_detail['Metric'].empty:
            comp_mean = df_plot2_component_detail['Metric'].mean()
            comp_median = df_plot2_component_detail['Metric'].median()
            comp_q1 = df_plot2_component_detail['Metric'].quantile(0.25)
            comp_q3 = df_plot2_component_detail['Metric'].quantile(0.75)
            comp_iqr = comp_q3 - comp_q1
            comp_upper_fence = comp_q3 + 1.5 * comp_iqr
            comp_lower_fence = comp_q1 - 1.5 * comp_iqr
        else: 
            comp_mean, comp_median, comp_upper_fence, comp_lower_fence = np.nan, np.nan, np.nan, np.nan
    
        unique_years_plot2 = sorted(df_plot2_component_detail['FISCAL_YEAR'].unique())
        color_sequence = ['#00ffff','#55b4fc', '#95f7b1', '#f9f871', '#8a8bd4' ] 
        all_months_numeric = list(range(1, 13)) 
    
        for i, year in enumerate(unique_years_plot2):
            year_data = df_plot2_component_detail[df_plot2_component_detail['FISCAL_YEAR'] == year].copy()
            year_data.sort_values(by='ACCOUNTING_PERIOD', inplace=True) 
            if year_data.empty:
                continue
            point_marker_colors = []
            point_marker_sizes = []
            point_marker_symbols = []
            point_marker_line_colors = []
            point_marker_line_widths = []
            customdata_plot2 = []
    
            for _, row in year_data.iterrows():
                is_outlier = row['Outlier']
                point_marker_colors.append('red' if is_outlier else color_sequence[i % len(color_sequence)])
                point_marker_sizes.append(10 if is_outlier else 6) 
                point_marker_symbols.append('circle' if is_outlier else 'circle') 
                point_marker_line_colors.append('DarkRed' if is_outlier else color_sequence[i % len(color_sequence)])
                point_marker_line_widths.append(1.5 if is_outlier else 0)
                customdata_plot2.append([
                    row['FISCAL_YEAR'], row['ACCOUNTING_PERIOD'], row['Metric'],
                    comp_mean, comp_median, comp_upper_fence, comp_lower_fence
                ])
            
            fig2.add_trace(go.Scatter(
                x=year_data['ACCOUNTING_PERIOD'],
                y=year_data['Metric'],
                mode='markers+lines',
                name=f"FY {year}",
                line=dict(color=color_sequence[i % len(color_sequence)], width=2),
                marker=dict(
                    color=point_marker_colors,
                    size=point_marker_sizes,
                    symbol=point_marker_symbols,
                    line=dict(
                        color=point_marker_line_colors, 
                        width=point_marker_line_widths
                    )
                ),
                customdata=customdata_plot2,
                hovertemplate=(
                    "<b>Year: %{customdata[0]}</b><br>"
                    "Month: %{customdata[1]}<br>"
                    f"{selected_metric_display_name}: %{{customdata[2]:.2f}}<br>"
                    "<br>"
                    "--- Component Stats ---<br>"
                    "Mean: %{customdata[3]:.2f}<br>"
                    "Median: %{customdata[4]:.2f}<br>"
                    "Upper Fence: %{customdata[5]:.2f}<br>"
                    "Lower Fence: %{customdata[6]:.2f}<br>"
                    "<extra></extra>"
                )
            ))
    
        shared_stat_scatter_props = {'mode': 'lines', 'hoverinfo': 'skip', 'showlegend': True}
        shared_stat_line_props = {'dash': 'dot', 'width': 1.5}
        if not np.isnan(comp_mean):
            fig2.add_trace(go.Scatter(x=all_months_numeric, y=[comp_mean]*len(all_months_numeric), name='Mean', 
                                      line={'color':'lightgreen', **shared_stat_line_props}, **shared_stat_scatter_props))
        if not np.isnan(comp_median):
            fig2.add_trace(go.Scatter(x=all_months_numeric, y=[comp_median]*len(all_months_numeric), name='Median', 
                                      line={'color':'lemonchiffon', **shared_stat_line_props}, **shared_stat_scatter_props))
        if not np.isnan(comp_upper_fence):
            fig2.add_trace(go.Scatter(x=all_months_numeric, y=[comp_upper_fence]*len(all_months_numeric), name='Upper Fence', 
                                      line={'color':'#9467bd', **shared_stat_line_props}, **shared_stat_scatter_props))
        if not np.isnan(comp_lower_fence):
            fig2.add_trace(go.Scatter(x=all_months_numeric, y=[comp_lower_fence]*len(all_months_numeric), name='Lower Fence', 
                                      line={'color':'darkred', **shared_stat_line_props}, **shared_stat_scatter_props))
    
        fig2.update_layout(
            title=f"Monthly {selected_metric_display_name} for {selected_m0m1_for_plot2} on {selected_ship_for_detail} ({type_option}) by Year",
            yaxis_title=selected_metric_display_name,
            xaxis_title="ACCOUNTING_PERIOD",
            template="plotly_white",
            width=1100, 
            height=600, 
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(
                tickmode='array',
                tickvals=all_months_numeric, 
                ticktext=[str(m) for m in all_months_numeric],
                type='category',
                categoryorder='array', 
                categoryarray=all_months_numeric 
            )
        )
        st.plotly_chart(fig2, use_container_width=True)

        if not outliers_df_detail.empty:
            insight_df = outliers_df_detail[outliers_df_detail['M0_AND_M1'] == selected_m0m1_for_plot2]
            
            if not insight_df.empty:
                lowest_outlier = insight_df.sort_values(by='Metric', ascending=True).iloc[0]
            
                # Calculate the mean from the full dataset (not just outliers)
                all_vals_df = df_plot2_component_detail[df_plot2_component_detail['M0_AND_M1'] == selected_m0m1_for_plot2]
                mean_value = all_vals_df['Metric'].mean()
            
                # Calculate absolute and percentage deviation
                deviation = lowest_outlier['Metric'] - mean_value
                percent_delta = (deviation / mean_value) * 100 if mean_value != 0 else 0
    
                insight_text = (
                    f"#### **🔎 Finding: Most Deviated Outlier for {lowest_outlier['M0_AND_M1']}** ####\n "
                    f"- 📅 Fiscal Year: **{lowest_outlier['FISCAL_YEAR']}**, "
                    f" 📆 Month: **{lowest_outlier['ACCOUNTING_PERIOD']}**, "
                    f" 💡 Component: **{lowest_outlier['M0_AND_M1']}**\n "
                    f"- 📉 Outlier Value: **{lowest_outlier['Metric']:,.2f}**, "
                    f" 📊 Mean Value: **{mean_value:,.2f}**\n "
                    f"- 🧮 Deviation: **{deviation:,.2f}**, "
                    f" 📈 % Delta: **{percent_delta:.2f}%**"
                )
    
                st.markdown(insight_text)
    
                # Append to the `all_insights` list
                add_insight(     st.session_state["all_insights"],     title= f"Most Deviated Outlier for {lowest_outlier['M0_AND_M1']}, for {selected_ship_for_detail}, {selected_product}",
                    text = insight_text, chart = fig2
                )
        
    return outliers_df_detail, selected_m0m1_for_plot2



#7.outlier_voyage_analysis( ship/product centric)

def outlier_voyage_analysis(df_accnt, outliers_df_detail, selected_m0m1_for_plot2, selected_ship, selected_metric_display_name, selected_product, available_months, selected_outlier_month, selected_outlier_year):

    
    # Filter the voyage data
    df_filtered = df_accnt[
        (df_accnt['RM_ROLLUP_PRODUCT_DESC'] == selected_product) &
        (df_accnt['SHIP_CD'] == selected_ship) &
        (df_accnt['FISCAL_YEAR'] == selected_outlier_year) &
        (df_accnt['ACCOUNTING_PERIOD'] == selected_outlier_month) &
        (df_accnt['M0_AND_M1'] == selected_m0m1_for_plot2)
    ]

   # Step 1: Groupby on numeric fields
    df_voy = df_filtered.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'MXP_VOYAGE_CODE']).agg({
        'ADJUSTED_FINAL_AMOUNT_NEW': 'sum',
        'NEW_PRTD_PAX_DAYS': 'mean',
        'NEW_PRTD_CAPS_DAYS': 'mean'
    }).reset_index()

    
    # Step 2: Pick non-aggregated fields from original data (1 row per voyage)
    columns_to_add = ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'MXP_VOYAGE_CODE', 
                      'CONVERTED_SAIL_DAY_QTY', 'GSS', 'PORTCD_ACTIVITY', 'PF_CII_SCORE', 'PF_CII_RATINGS', 'STRADDLE_FLAG']
    
    # Make sure these columns exist
    available_cols = [col for col in columns_to_add if col in df_filtered.columns]
    
    # Drop duplicates to ensure 1-to-1 merge
    df_nonagg = df_filtered[available_cols].drop_duplicates(subset=['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'MXP_VOYAGE_CODE'])
    
    # Step 3: Merge into the aggregated dataframe
    df_voy = pd.merge(df_voy, df_nonagg, on=['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'MXP_VOYAGE_CODE'], how='left')
    
    if df_voy.empty:
        st.warning("No voyage data found for selected filters.")
        return

    # Compute per day metrics
    df_voy['VOY_PCD'] = df_voy['ADJUSTED_FINAL_AMOUNT_NEW'] / df_voy['NEW_PRTD_CAPS_DAYS'].replace(0, np.nan)
    df_voy['VOY_PPD'] = df_voy['ADJUSTED_FINAL_AMOUNT_NEW'] / df_voy['NEW_PRTD_PAX_DAYS'].replace(0, np.nan)

    # Select the correct metric column
    if selected_metric_display_name == "Margin $":
        metric_col = 'ADJUSTED_FINAL_AMOUNT_NEW'
    elif selected_metric_display_name == "Per Capacity Day (Margin PCD)":  
        metric_col = 'VOY_PCD'
    elif selected_metric_display_name == "Per Passenger Day (Margin PPD)":
        metric_col = 'VOY_PPD'
    else:
        st.error("Unsupported metric selected for trend plot.")
        return

    # Format sail date
    #df_voy['FORMATTED_SAIL_DATE'] = pd.to_datetime(df_voy['CONVERTED_SAIL_DATE'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')

    # Sort voyages by selected metric
    df_voy = df_voy.sort_values(by=metric_col, ascending=True)

    st.subheader(f"🚢 Voyage Level break down for Year - {selected_outlier_year}, Month - {selected_outlier_month}")
    # Prepare the plot
    fig = px.bar(
    df_voy,
    x='MXP_VOYAGE_CODE',
    y=metric_col,
    color='STRADDLE_FLAG',  # <-- categorical coloring
    custom_data=[
        'NEW_PRTD_CAPS_DAYS',
        'NEW_PRTD_PAX_DAYS',
        'STRADDLE_FLAG',
        'CONVERTED_SAIL_DAY_QTY',
        'GSS',
        'PORTCD_ACTIVITY',
        'PF_CII_SCORE', 
        'PF_CII_RATINGS'      
    ],
    title=f"Voyage-Level {selected_metric_display_name} for {selected_m0m1_for_plot2} - {selected_ship} ({selected_product})",
    labels={
        'MXP_VOYAGE_CODE': 'Voyage Code',
        metric_col: selected_metric_display_name,
        'STRADDLE_FLAG': 'Voyage Type'
    },
    color_discrete_map={
        'straddle': '#2F4F4F',
        'Non-straddle': '#20B2AA', #Light Sea Green
        'Non-Period-Voyage': 'red'
    }
    )


    fig.update_traces(
        text=df_voy[metric_col].round(2),
        textposition='outside',
        hovertemplate=
            "<b>%{x}</b><br>" +
            f"{selected_metric_display_name}: $%{{y:,.2f}}<br>" +  # ✅ use y instead of customdata[0]
            "Capacity Days: %{customdata[0]}<br>" +
            "Passenger Days: %{customdata[1]}<br>" +
            "Straddle Flag : %{customdata[2]}<br>" +
            "Sail Day Quantity: %{customdata[3]}<br>" +
            "GSS: %{customdata[4]}<br>" +
            "Itinerary: %{customdata[5]}<br>" +
            "CII Score: %{customdata[6]:,.2f}<br>" +
            "CII Rating: %{customdata[7]}<br>"
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis_title=None,
        height=550,
        bargap = 0.5
    )

    st.plotly_chart(fig, use_container_width=True)

    return df_voy


#8.outlier_analysis( ship/product centric)
def plot_non_outlier_voyage_mon_lvl_delta_accnts(df, selected_outlier_year, selected_outlier_month, selected_metric_display_name, selected_m0m1_for_plot2, selected_product, selected_ship, available_outlier_months):
    
    global all_insights
    
    results = []
    available_outlier_months = sorted(available_outlier_months)

    df = df[
        (df['FISCAL_YEAR'] == selected_outlier_year) &
        (df['RM_ROLLUP_PRODUCT_DESC'] == selected_product) &
        (df['SHIP_CD'] == selected_ship) &
        (df['M0_AND_M1'] == selected_m0m1_for_plot2)
    ]

    df = df.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'ACCOUNT'])[ 
        ['ADJUSTED_FINAL_AMOUNT_NEW', 'NEW_PRTD_PAX_DAYS', 'NEW_PRTD_CAPS_DAYS']
    ].sum().reset_index()

    df['ADJUSTED_FINAL_AMOUNT_NEW'] = pd.to_numeric(df['ADJUSTED_FINAL_AMOUNT_NEW'], errors='coerce')
    df['NEW_PRTD_CAPS_DAYS'] = pd.to_numeric(df['NEW_PRTD_CAPS_DAYS'], errors='coerce')
    df['NEW_PRTD_PAX_DAYS'] = pd.to_numeric(df['NEW_PRTD_PAX_DAYS'], errors='coerce')

    df['Account_lvl_PCD'] = df['ADJUSTED_FINAL_AMOUNT_NEW'] / df['NEW_PRTD_CAPS_DAYS'].replace(0, np.nan)
    df['Account_lvl_PPD'] = df['ADJUSTED_FINAL_AMOUNT_NEW'] / df['NEW_PRTD_PAX_DAYS'].replace(0, np.nan)

    if selected_metric_display_name == "Margin $":
        metric_col = 'ADJUSTED_FINAL_AMOUNT_NEW'
    elif selected_metric_display_name == "Per Capacity Day (Margin PCD)":  
        metric_col = 'Account_lvl_PCD'
    elif selected_metric_display_name == "Per Passenger Day (Margin PPD)":
        metric_col = 'Account_lvl_PPD'
    else:
        st.error("Unsupported metric selected for trend plot.")
        return

    df_outlier = df[df['ACCOUNTING_PERIOD'] == selected_outlier_month]
    df_other = df

    valid_accounts = []
    filtered_data = []

    for _, row in df_outlier.iterrows():
        acc = row['ACCOUNT']
        val = row[metric_col]
        historical = df_other[df_other['ACCOUNT'] == acc][metric_col].dropna()
        mean_val = historical.mean()

        q1 = historical.quantile(0.25)
        q3 = historical.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr

        if val < lower:
            valid_accounts.append(acc)
            filtered_data.append({
                'Account': acc,
                'Ship': selected_ship,
                'Month': selected_outlier_month,
                'Component': selected_m0m1_for_plot2,
                'Outlier_Amount': val,
                'Mean_Value': mean_val,
                'Deviation_from_Mean': val - mean_val
            })

    if not filtered_data:
        st.warning("Individual account-level Deviations are minor and fall within expected limits, so they are not classified as outliers. However, when aggregated over time, these small deviations accumulate and may appear as outliers in the time-series view. Therefore, no accounts are flagged as outliers, despite period-level deviations.")
        return []

    final_df = pd.DataFrame(filtered_data)
    final_df['Outlier_Abs'] = final_df['Outlier_Amount'].abs()
    final_df['Deviation_from_Mean_Abs'] = final_df['Deviation_from_Mean'].abs()
    final_df['Mean_Value_Abs'] = final_df['Mean_Value'].abs()
    final_df = final_df.sort_values(by='Deviation_from_Mean_Abs', ascending=False).reset_index(drop=True)
    final_df.insert(0, "Rank", range(1, len(final_df) + 1))

    st.subheader(f"\U0001F4BC Accounts with Outlier Behavior for year - {selected_outlier_year}, Month - {selected_outlier_month}")

    style_format = {
        'Outlier_Amount': '${:,.0f}' if selected_metric_display_name == "Margin $" else '${:,.4f}',
        'Mean_Value': '${:,.0f}' if selected_metric_display_name == "Margin $" else '${:,.4f}',
        'Deviation_from_Mean': '${:,.0f}' if selected_metric_display_name == "Margin $" else '${:,.4f}'
    }

    st.dataframe(
        final_df[['Rank', 'Ship', 'Month', 'Component', 'Account', 'Outlier_Amount', 'Mean_Value', 'Deviation_from_Mean']].style.format(style_format),
        use_container_width=True
    )

    # Grouped bar chart: Outlier vs Mean
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=final_df['Account'],
        y=final_df['Outlier_Abs'],
        name=f'Outlier Month {selected_outlier_month}',
        marker_color='aqua',
        offsetgroup=0,
        text=[f"${y:,.0f}" if y < 1_000_000 else f"${y/1_000_000:.1f}M" for y in final_df['Outlier_Abs']],
        textposition='outside',
        hovertemplate='Account: %{x}<br>Outlier: %{y:$,.2f}<extra></extra>'
    ))

    fig.add_trace(go.Bar(
        x=final_df['Account'],
        y=final_df['Mean_Value_Abs'],
        name='Mean Value',
        marker_color='orange',
        offsetgroup=1,
        text=[f"${y:,.0f}" if y < 1_000_000 else f"${y/1_000_000:.1f}M" for y in final_df['Mean_Value_Abs']],
        textposition='outside',
        hovertemplate='Account: %{x}<br>Mean: %{y:$,.2f}<extra></extra>'
    ))

    fig.update_layout(
        barmode='group',
        title=f'Outlier vs Mean Bar Chart for {selected_m0m1_for_plot2} ({selected_ship})',
        xaxis_title='Account',
        yaxis_title='Amount ($)',
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)
    
    if not final_df.empty:
            insight_df = final_df[
                ['Ship', 'Month', 'Component', 'Account', 'Outlier_Amount', 'Mean_Value', 'Deviation_from_Mean']
            ]
        
            if not insight_df.empty:
                # Convert rows to bullet points
                outlier_bullets = "\n".join(
                    f"- Account: {row['Account']}, Outlier Value: {row['Outlier_Amount']:,.2f}, "
                    f"Mean Value: {row['Mean_Value']:,.2f}, Deviation from Mean: {row['Deviation_from_Mean']:,.2f}\n"
                    for _, row in insight_df.iterrows()
                )
        
                # Final formatted text
                insight_text = f"""All Outlier Accounts:\n\n{outlier_bullets}"""
        
                add_insight(     st.session_state["all_insights"],     title= f"📌 All Outlier Accounts for Year: {selected_outlier_year}, Month: {selected_outlier_month} for {selected_m0m1_for_plot2}:",
                        text = insight_text, chart = fig
                    )

    return final_df['Account'].tolist()


# Account Trend ( SHIP/PRODUCT CENTRIC)
def plot_account_time_series(selected_account, selected_account_id, df_accnt, selected_product, selected_ship, selected_m0m1_for_plot2, type_option, selected_metric_display_name, selected_outlier_year, selected_outlier_month):

    global all_insights
    
    st.subheader(f"📈 Account Trend: {selected_account} | {selected_m0m1_for_plot2}")

    df_accnt['ACCOUNT_ID'] = df_accnt['ACCOUNT'].str.split('-').str[0]

    df_accnt = df_accnt[
        (df_accnt['RM_ROLLUP_PRODUCT_DESC'] == selected_product) &
        (df_accnt['SHIP_CD'] == selected_ship) &
        (df_accnt['M0_AND_M1'] == selected_m0m1_for_plot2) &
        (df_accnt['ACCOUNT_ID'] == selected_account_id)
    ]

    df_accnt = df_accnt.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'ACCOUNT_ID'])[
        ['ADJUSTED_FINAL_AMOUNT_NEW', 'NEW_PRTD_PAX_DAYS', 'NEW_PRTD_CAPS_DAYS']
    ].sum().reset_index()

    df_accnt['ADJUSTED_FINAL_AMOUNT_NEW'] = pd.to_numeric(df_accnt['ADJUSTED_FINAL_AMOUNT_NEW'], errors='coerce')
    df_accnt['NEW_PRTD_CAPS_DAYS'] = pd.to_numeric(df_accnt['NEW_PRTD_CAPS_DAYS'], errors='coerce')
    df_accnt['NEW_PRTD_PAX_DAYS'] = pd.to_numeric(df_accnt['NEW_PRTD_PAX_DAYS'], errors='coerce')

    df_accnt['Account_lvl_PCD'] = df_accnt['ADJUSTED_FINAL_AMOUNT_NEW'] / df_accnt['NEW_PRTD_CAPS_DAYS'].replace(0, np.nan)
    df_accnt['Account_lvl_PPD'] = df_accnt['ADJUSTED_FINAL_AMOUNT_NEW'] / df_accnt['NEW_PRTD_PAX_DAYS'].replace(0, np.nan)

    if df_accnt.empty:
        st.warning("No data available for selected filters.")
        return

    if selected_metric_display_name == "Margin $":
        metric_col = 'ADJUSTED_FINAL_AMOUNT_NEW'
    elif selected_metric_display_name == "Per Capacity Day (Margin PCD)":
        metric_col = 'Account_lvl_PCD'
    elif selected_metric_display_name == "Per Passenger Day (Margin PPD)":
        metric_col = 'Account_lvl_PPD'
    else:
        st.error("Unsupported metric selected for trend plot.")
        return

    df_accnt['Metric'] = pd.to_numeric(df_accnt[metric_col], errors='coerce')
    df_accnt['Month'] = df_accnt['ACCOUNTING_PERIOD']
    df_accnt['Year'] = df_accnt['FISCAL_YEAR']

    # Show selected year plot first
    years = sorted(df_accnt['Year'].unique())
    all_months_numeric = list(range(1, 13))
    color_sequence = ['#00ffff', '#55b4fc', '#95f7b1', '#f9f871', '#8a8bd4']

    def create_chart(df_year, year, color):
        df_year = df_year.copy()
        year_mean = df_year['Metric'].mean()
        year_median = df_year['Metric'].median()
        q1 = df_year['Metric'].quantile(0.25)
        q3 = df_year['Metric'].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        df_year['Outlier'] = df_year['Metric'] < lower_bound

        fig = go.Figure()
        df_year_grouped = df_year.groupby('Month')['Metric'].mean().reindex(all_months_numeric).reset_index()

        fig.add_trace(go.Scatter(
            x=df_year_grouped['Month'],
            y=df_year_grouped['Metric'],
            mode='lines+markers',
            name=f"{year}",
            marker=dict(color=color),
            line=dict(color=color),
            hovertemplate=(
                f"Year: {year}<br>" +
                "Month: %{x}<br>" +
                "Value: %{y:.2f}" +
                f"<br><br>Mean: {year_mean:.2f}<br>Median: {year_median:.2f}<extra></extra>"
            )
        ))

        outliers = df_year[df_year['Outlier']]
        if not outliers.empty:
            fig.add_trace(go.Scatter(
                x=outliers['Month'],
                y=outliers['Metric'],
                mode='markers',
                marker=dict(color='red', size=10, symbol='circle'),
                name='Outliers',
                customdata=outliers[['Year']].values,
                hovertemplate=(
                    "Year: %{customdata[0]}<br>" +
                    "Month: %{x}<br>" +
                    "Value: %{y:.2f}" +
                    f"<br><br>Mean: {year_mean:.2f}<br>Median: {year_median:.2f}<extra></extra>"
                )
            ))

        shared_stat_props = {'mode': 'lines', 'hoverinfo': 'skip', 'showlegend': True, 'line': {'dash': 'dot', 'width': 1.5}}
        fig.add_trace(go.Scatter(x=all_months_numeric, y=[year_mean] * 12, name='Mean', line_color='lightgreen', **shared_stat_props))
        fig.add_trace(go.Scatter(x=all_months_numeric, y=[year_median] * 12, name='Median', line_color='lemonchiffon', **shared_stat_props))
        fig.add_trace(go.Scatter(x=all_months_numeric, y=[upper_bound] * 12, name='Upper Fence', line_color='#9467bd', **shared_stat_props))
        fig.add_trace(go.Scatter(x=all_months_numeric, y=[lower_bound] * 12, name='Lower Fence', line_color='darkred', **shared_stat_props))

        fig.update_layout(
            title=f"📊 Account Time Series - {selected_account} ({year})",
            xaxis=dict(title="Month", dtick=1),
            yaxis=dict(title=selected_metric_display_name),
            height=500,
            template="plotly_white"
        )
        return fig, outliers

    # Plot for selected outlier year
    fig, outliers = create_chart(df_accnt[df_accnt['Year'] == selected_outlier_year], selected_outlier_year, color_sequence[0])
    st.plotly_chart(fig, use_container_width=True)

    if not outliers.empty:
        df = df_accnt[df_accnt['FISCAL_YEAR'] == selected_outlier_year]
        insight_df = outliers[outliers['ACCOUNTING_PERIOD'] == selected_outlier_month]

        Mean_Value = df['Metric'].mean()
        deviation = insight_df['Metric'].iloc[0] - Mean_Value
        delta = (deviation/Mean_Value)

        # Final formatted text
        insight_text = (
            "#### 🔎 Account Findings: \n"
            f"The Account: **{selected_account}** is an outlier for the month: **{selected_outlier_month}** "
            f"with a **{selected_metric_display_name}: {insight_df['Metric'].iloc[0]:,.3f}**, the mean value is **{Mean_Value:,.3f}**, "
            f"it is deviated from the mean by **{deviation:,.3f}**, with a % delta of **{delta:,.2%}**"
        )

        st.markdown(insight_text)

        add_insight(     st.session_state["all_insights"],     title= f"Outlier Account: {selected_account} for Year: {selected_outlier_year}, Month: {selected_outlier_month} for {selected_m0m1_for_plot2}:",
                text = insight_text, chart = fig
            )

    # Allow user to select additional years to compare
    other_years = [y for y in years if y != selected_outlier_year]
    if other_years:
        st.subheader("Compare with Other Years:")
        selected_comparisons = st.multiselect("Select Additional Years", options=other_years)

        if selected_comparisons and isinstance(selected_comparisons, list) and len(selected_comparisons) > 0:
            columns = st.columns(len(selected_comparisons))
            for i, year in enumerate(selected_comparisons):
                with columns[i]:
                    fig, outliers = create_chart(df_accnt[df_accnt['Year'] == year], year, color_sequence[(i+1) % len(color_sequence)])
                    st.plotly_chart(fig, use_container_width=True)
        # Continue using 'columns[...]' as intended
        else:
            st.warning("No comparisons selected or comparison list is invalid.")


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Ship Centric Helper Function------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------------------
# 3.Plots of product going to that ship across each year(ship centric)
def plot_product_yearly_for_ship(df, selected_ship, metric_col, metric_display_name, order_column=None, order_type=None):
    global all_insights
    st.subheader(f"🚢 Product Performance for {selected_ship} by Year")

    # --- Handle None for order_column and order_type ---
    use_custom_order = (order_column is not None) and (order_type is not None)
    
    # Map the order_column name to the actual DataFrame column name
    order_col_for_agg = None
    if use_custom_order:
        order_column = order_column.upper()
        if order_column in ['CII', 'CII_SCORE']:
            order_col_for_agg = 'CII_SCORE'
        elif order_column in ['GSS']:
            order_col_for_agg = 'GSS'
        elif order_column in ['LOAD FACTOR', 'LF']:
            order_col_for_agg = 'PAX_DAYS'  # Use PAX_DAYS for the complex LF calculation
        order_column_avg_name = f"{order_column}_Average"
    
    ship_df = df[df['SHIP_CD'] == selected_ship].copy()
    if ship_df.empty:
        st.warning(f"No data for ship '{selected_ship}'.")
        add_insight(st.session_state["all_insights"], title=f"Product Performance for {selected_ship}", text=f"No data for ship '{selected_ship}'.")
        return

    unique_years_in_ship = sorted(ship_df['FISCAL_YEAR'].dropna().unique().tolist())
    
    if not unique_years_in_ship:
        st.info(f"No yearly data available for ship '{selected_ship}'.")
        add_insight(st.session_state["all_insights"], title=f"Product Performance for {selected_ship}", text=f"No yearly data available for ship '{selected_ship}'.")
        return

    # First pass: compute global max Total_Metric across all years and products for a consistent Y-axis
    max_metric_value = 0
    
    # Pre-calculate the aggregated data for all years to find the global max
    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = ship_df.groupby(['FISCAL_YEAR','RM_ROLLUP_PRODUCT_DESC'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = ship_df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['FISCAL_YEAR','RM_ROLLUP_PRODUCT_DESC'], as_index=False)['t1'].sum()
        product_yearly_all = df1.merge(df2, on=['FISCAL_YEAR','RM_ROLLUP_PRODUCT_DESC'], how='inner')
        product_yearly_all['Total_Metric'] = product_yearly_all['COMPONENT_AMOUNT'] / product_yearly_all['t1']

    elif metric_display_name == 'Passenger Days':
        product_yearly_all = (
            ship_df.groupby(
                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD',
                 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
            )['NEW_PRTD_PAX_DAYS']
            .first()
            .reset_index()
            .groupby(['RM_ROLLUP_PRODUCT_DESC', 'FISCAL_YEAR'], as_index=False)['NEW_PRTD_PAX_DAYS']
            .sum()
            .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
        )

    elif metric_display_name == 'Capacity Days':
        product_yearly_all = (
            ship_df.groupby(
                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD',
                 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
            )['NEW_PRTD_CAPS_DAYS']
            .first()
            .reset_index()
            .groupby(['RM_ROLLUP_PRODUCT_DESC', 'FISCAL_YEAR'], as_index=False)['NEW_PRTD_CAPS_DAYS']
            .sum()
            .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
        )

    else:
        product_yearly_all = ship_df.groupby(['RM_ROLLUP_PRODUCT_DESC', 'FISCAL_YEAR']).agg(
            Total_Metric=(metric_col, 'sum')
        ).reset_index()

    if not product_yearly_all.empty:
        max_metric_value = product_yearly_all['Total_Metric'].max()

    # Dynamically create columns for plots to organize them
    num_years = len(unique_years_in_ship)
    cols_per_row = 2
    rows_needed = (num_years + cols_per_row - 1) // cols_per_row

    for row_idx in range(rows_needed):
        row_cols = st.columns(cols_per_row)
        for col_offset in range(cols_per_row):
            year_idx = row_idx * cols_per_row + col_offset
            if year_idx < num_years:
                year = unique_years_in_ship[year_idx]
                with row_cols[col_offset]:
                    st.markdown(f"#### **Year: {year}**")
                    year_df = ship_df[ship_df['FISCAL_YEAR'] == year].copy()

                    # --- Aggregation Logic for Primary Metric ---
                    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
                        df1 = year_df.groupby(['RM_ROLLUP_PRODUCT_DESC'], as_index=False)['COMPONENT_AMOUNT'].sum()
                        df2 = year_df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
                        df2 = df2.groupby(['RM_ROLLUP_PRODUCT_DESC'], as_index=False)['t1'].sum()
                        product_yearly = df1.merge(df2, on=['RM_ROLLUP_PRODUCT_DESC'], how='inner')
                        product_yearly['Total_Metric'] = product_yearly['COMPONENT_AMOUNT'] / product_yearly['t1']

                    elif metric_display_name == 'Passenger Days':
                        product_yearly = (
                            year_df.groupby(
                                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD',
                                 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                            )['NEW_PRTD_PAX_DAYS']
                            .first()
                            .reset_index()
                            .groupby(['RM_ROLLUP_PRODUCT_DESC', 'FISCAL_YEAR'], as_index=False)['NEW_PRTD_PAX_DAYS']
                            .sum()
                            .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
                        )
                    
                    elif metric_display_name == 'Capacity Days':
                        product_yearly = (
                            year_df.groupby(
                                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD',
                                 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                            )['NEW_PRTD_CAPS_DAYS']
                            .first()
                            .reset_index()
                            .groupby(['RM_ROLLUP_PRODUCT_DESC', 'FISCAL_YEAR'], as_index=False)['NEW_PRTD_CAPS_DAYS']
                            .sum()
                            .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
                        )   
                    else:
                        product_yearly = year_df.groupby(['RM_ROLLUP_PRODUCT_DESC']).agg(
                            Total_Metric=(metric_col, 'sum')
                        ).reset_index()

                    # --- Add Unique Voyage Count for Tooltip ---
                    voyage_count_df = year_df.groupby('RM_ROLLUP_PRODUCT_DESC', as_index=False)['VOYAGE_ID'].nunique().rename(columns={'VOYAGE_ID': 'Unique_Voyages'})
                    product_yearly = product_yearly.merge(voyage_count_df, on='RM_ROLLUP_PRODUCT_DESC', how='left')

                    # --- Aggregation Logic for Order Column ---
                    if use_custom_order:
                        if order_column in ['GSS', 'CII']:
                            order_col_for_agg = 'CII_SCORE' if order_column == 'CII' else 'GSS'
                            order_col_avg_df = year_df.groupby(['VOYAGE_ID', 'RM_ROLLUP_PRODUCT_DESC'], as_index=False)[order_col_for_agg].first()
                            order_col_avg_df = order_col_avg_df.groupby('RM_ROLLUP_PRODUCT_DESC', as_index=False)[order_col_for_agg].mean().rename(columns={order_col_for_agg: order_column_avg_name})
                            product_yearly = product_yearly.merge(order_col_avg_df, on='RM_ROLLUP_PRODUCT_DESC', how='left')
                        elif order_column in ['LOAD FACTOR', 'LF']:
                            temp_df = year_df.groupby(['VOYAGE_ID', 'RM_ROLLUP_PRODUCT_DESC'], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
                            temp_df = temp_df.groupby('RM_ROLLUP_PRODUCT_DESC', as_index=False).sum()
                            temp_df[order_column_avg_name] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
                            product_yearly = product_yearly.merge(temp_df[['RM_ROLLUP_PRODUCT_DESC', order_column_avg_name]], on='RM_ROLLUP_PRODUCT_DESC', how='left')

                    # --- Sorting Logic ---
                    if use_custom_order and order_column_avg_name in product_yearly.columns:
                        is_ascending = (order_type.lower() == 'asc')
                        product_yearly = product_yearly.sort_values(by=order_column_avg_name, ascending=is_ascending)
                        sorting_info_text = f"by average '{order_column}' in {order_type} order."
                    else:
                        product_yearly = product_yearly.sort_values(by='Total_Metric', ascending=False)
                        sorting_info_text = f"by '{metric_display_name}' in descending order (default)."

                    if not product_yearly.empty:
                        # --- Plotting with dual axis (Sorted) ---
                        if use_custom_order and order_column_avg_name in product_yearly.columns:
                            if order_column in ['LOAD FACTOR', 'LF']:
                                product_yearly['hover_text_line'] = f"{order_column}: " + (product_yearly[order_column_avg_name] * 100).round(2).astype(str) + '%'
                                yaxis2_title = f"{order_column}"
                            elif order_column in ['GSS', 'CII']:
                                product_yearly['hover_text_line'] = f"{order_column} (Average): " + product_yearly[order_column_avg_name].round(2).astype(str)
                                yaxis2_title = f"{order_column} (Average)"
                            else:
                                product_yearly['hover_text_line'] = f"{order_column_avg_name}: " + product_yearly[order_column_avg_name].round(2).astype(str)
                                yaxis2_title = f"{order_column_avg_name}"

                            product_yearly['Total_Metric_Formatted'] = product_yearly['Total_Metric'].round(2).astype(str)

                            fig = make_subplots(specs=[[{"secondary_y": True}]])

                            # --- Bar trace (with Unique Voyage count in tooltip) ---
                            fig.add_trace(
                                go.Bar(
                                    x=product_yearly['RM_ROLLUP_PRODUCT_DESC'],
                                    y=product_yearly['Total_Metric'],
                                    name=f"",
                                    customdata=product_yearly[['Unique_Voyages', 'hover_text_line']],
                                    hovertemplate="<b>Product:</b> %{x}<br>"
                                                  f"<b>{metric_display_name}:</b> %{{y:,.2f}}<br>"
                                                  f"<b>Unique Voyages:</b> %{{customdata[0]}}<br>"
                    
                                ),
                                secondary_y=False,
                            )

                            # --- Line trace (secondary axis) ---
                            fig.add_trace(
                                go.Scatter(
                                    x=product_yearly['RM_ROLLUP_PRODUCT_DESC'],
                                    y=product_yearly[order_column_avg_name],
                                    mode='lines+markers',
                                    name=f"{yaxis2_title}",
                                    line=dict(color='red', width=2),
                                    marker=dict(size=8, color='red'),
                                    customdata=product_yearly[['Unique_Voyages', 'hover_text_line']]
                                ),
                                secondary_y=True,
                            )

                            fig.update_layout(
                                title_text=f"{metric_display_name} by Product for {selected_ship} - {year}",
                                xaxis_title="Product",
                                yaxis_title=metric_display_name,
                                yaxis2_title=yaxis2_title,
                                yaxis2=dict(overlaying='y', side='right'),
                                legend=dict(x=1.05, y=1, xanchor='left', yanchor='top'),
                                barmode='group',
                                hovermode="x unified"
                            )
                            fig.update_xaxes(tickangle=45)
                            fig.update_yaxes(range=[0, max_metric_value * 1.15], secondary_y=False)
                            st.plotly_chart(fig, use_container_width=True, key=f"product_yearly_for_ship_{selected_ship}_{year}")

                        # --- Unsorted (single axis bar) ---
                        else:
                            fig = px.bar(
                                product_yearly,
                                x='RM_ROLLUP_PRODUCT_DESC', y='Total_Metric',
                                title=f"{metric_display_name} by Product for {selected_ship} - {year}",
                                labels={'RM_ROLLUP_PRODUCT_DESC': 'Product', 'Total_Metric': metric_display_name},
                                barmode='group'
                            )
                            fig.update_traces(
                                customdata=product_yearly[['Unique_Voyages']],
                                hovertemplate="<b>Product:</b> %{x}<br>"
                                              f"<b>{metric_display_name}:</b> %{{y:,.2f}}<br>"
                                              f"<b>Unique Voyages:</b> %{{customdata[0]}}<extra></extra>",
                            )
                            fig.update_xaxes(tickangle=45)
                            fig.update_yaxes(range=[0, max_metric_value * 1.15])
                            st.plotly_chart(fig, use_container_width=True, key=f"product_yearly_for_ship_{selected_ship}_{year}")

                        # --- Insights generation (unchanged) ---
                        if len(product_yearly) > 0:
                            if len(product_yearly.iloc[:, 0]) == 1:
                                if use_custom_order:
                                    top_product_order = product_yearly.loc[product_yearly[order_column_avg_name].idxmax()]
                                    top_product_metric = product_yearly.loc[product_yearly['Total_Metric'].idxmax()]
                                    # Clean display strings to prevent markdown glitches
                                    safe_metric_display = metric_display_name.replace("$", "\\$")
                                    safe_order_column = order_column.replace("$", "\\$") if order_column else order_column
                                    
                                    insight_text = f"""
                                    🧭 **Product Performance Snapshot ({selected_ship}, {year})**
                                    
                                    This summary highlights the performance of the **product** carried by **{selected_ship}** during **{year}**.
                                    
                                    - **Overall Contribution ({safe_metric_display}):**  
                                      Product **{top_product_metric['RM_ROLLUP_PRODUCT_DESC']}** achieved a total {safe_metric_display} of **{top_product_metric['Total_Metric']:,.2f}**, reflecting its impact on this ship's operations.  
                                    - **Operational Efficiency ({safe_order_column}):**  
                                      Efficiency measured at **{top_product_order[order_column_avg_name]:,.2f}**, setting a benchmark for performance within this voyage segment.  
                                    """
                                else:
                                    top_product_metric = product_yearly.loc[product_yearly['Total_Metric'].idxmax()]
                                    insight_text = f"""
                                    **Product Performance Summary ({selected_ship}, {year})**
                                    
                                    - **{top_product_metric['RM_ROLLUP_PRODUCT_DESC']}** achieved a {metric_display_name} of **{top_product_metric['Total_Metric']:,.2f}**, highlighting its strong contribution to {selected_ship}'s performance for the year.  
                                    """
                            else:
                                if use_custom_order:
                                    top_product_order = product_yearly.loc[product_yearly[order_column_avg_name].idxmax()]
                                    bottom_product_order = product_yearly.loc[product_yearly[order_column_avg_name].idxmin()]
                                    top_product_metric = product_yearly.loc[product_yearly['Total_Metric'].idxmax()]
                                    bottom_product_metric = product_yearly.loc[product_yearly['Total_Metric'].idxmin()]
                                    avg_metric = product_yearly['Total_Metric'].mean()
                        
                                    insight_text = f"""
                                    🚀 **Product Performance Insights ({selected_ship}, {year})**
                                    
                                    This overview highlights the **top and bottom performing products** carried by **{selected_ship}** during **{year}**.
                                    
                                    📦 **Total Activity: {metric_display_name}**
                                    
                                    * **Top Performing Product:** **{top_product_metric['RM_ROLLUP_PRODUCT_DESC']}** led the product mix with a total {metric_display_name} of **{top_product_metric['Total_Metric']:,.2f}**.  
                                    * **Lowest Performing Product:** **{bottom_product_metric['RM_ROLLUP_PRODUCT_DESC']}** recorded the minimum {metric_display_name} at **{bottom_product_metric['Total_Metric']:,.2f}**, suggesting lower contribution potential.  
                                    * **Product Benchmark:** The average {metric_display_name} across all products on {selected_ship} was **{avg_metric:,.2f}**.  
                                    
                                    ---
                                    
                                    ⚙️ **Operational Efficiency: {order_column}**
                                    
                                    * **Highest Efficiency:** **{top_product_order['RM_ROLLUP_PRODUCT_DESC']}** demonstrated the strongest efficiency score of **{top_product_order[order_column_avg_name]:,.2f}**.  
                                    * **Improvement Opportunity:** **{bottom_product_order['RM_ROLLUP_PRODUCT_DESC']}** had the lowest efficiency at **{bottom_product_order[order_column_avg_name]:,.2f}**, marking a key focus area for optimization.  
                                    """
                                else:
                                    top_product_metric = product_yearly.loc[product_yearly['Total_Metric'].idxmax()]
                                    bottom_product_metric = product_yearly.loc[product_yearly['Total_Metric'].idxmin()]
                                    avg_metric = product_yearly['Total_Metric'].mean()
                        
                                    insight_text = f"""
                                    🚀 **Product Performance Insights ({selected_ship}, {year})**
                                    
                                    This overview captures the **best and lowest performing products** on **{selected_ship}** during the year.
                                    
                                    📦 **Total Activity: {metric_display_name}**
                                    
                                    * **Top Performing Product:** **{top_product_metric['RM_ROLLUP_PRODUCT_DESC']}** achieved a total {metric_display_name} of **{top_product_metric['Total_Metric']:,.2f}**, setting the benchmark for the year.  
                                    * **Lowest Performing Product:** **{bottom_product_metric['RM_ROLLUP_PRODUCT_DESC']}** recorded the lowest {metric_display_name} at **{bottom_product_metric['Total_Metric']:,.2f}**, requiring further review.  
                                    * **Product Benchmark:** The average {metric_display_name} across all products on this ship was **{avg_metric:,.2f}**.  
                                    """
                        
                            st.markdown(insight_text)
                            add_insight(
                                st.session_state["all_insights"],
                                title=f"Product Performance for {selected_ship} - {year}",
                                text=insight_text,
                                chart=fig
                            )
                                          

                        else:
                            st.info(f"No product data for ship '{selected_ship}' in year {year}.")
                            add_insight(st.session_state["all_insights"], title=f"Product Performance for {selected_ship} - {year}", text=f"No product data for {selected_ship} in year {year}.")

#4.Trend across months of the year for the selected product on this ship(ship centric)
def plot_product_monthly_trend_for_ship(df, selected_ship, selected_product, metric_col, metric_display_name):
    """Shows trends across months of the year for a selected product on a ship."""
    global all_insights
    st.subheader(f"📈 Monthly Trend for {selected_product} (Ship: {selected_ship})")
    
    df_filtered = df[
        (df['SHIP_CD'] == selected_ship) &
        (df['RM_ROLLUP_PRODUCT_DESC'] == selected_product)
    ].copy()

    if df_filtered.empty:
        st.warning(f"No data for selected ship '{selected_ship}' and product '{selected_product}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Monthly Trend for {selected_product} on {selected_ship}", text = "No data available.")
        return
    
    if metric_display_name in ('Per Capacity Day (Margin PCD)','Per Passenger Day (Margin PPD)'):
        df1 = df_filtered.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df_filtered.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD'], as_index=False)['t1'].sum()
        monthly_trend = df1.merge(df2, on=['FISCAL_YEAR','ACCOUNTING_PERIOD'], how='inner')
        monthly_trend['Total_Metric'] = monthly_trend['COMPONENT_AMOUNT'] / monthly_trend['t1']

    elif metric_display_name == 'Passenger Days':
        monthly_trend  = (
                df_filtered.groupby(
                    ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD',
                     'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
                )['NEW_PRTD_PAX_DAYS']
                .first()
                .reset_index()
                .groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD'], as_index=False)['NEW_PRTD_PAX_DAYS']
                .sum()
                .rename(columns={'NEW_PRTD_PAX_DAYS': 'Total_Metric'})
            )
                    
    elif metric_display_name == 'Capacity Days':
        monthly_trend = (
            df_filtered.groupby(
                ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD',
                 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']
            )['NEW_PRTD_CAPS_DAYS']
            .first()
            .reset_index()
            .groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD'], as_index=False)['NEW_PRTD_CAPS_DAYS']
            .sum()
            .rename(columns={'NEW_PRTD_CAPS_DAYS': 'Total_Metric'})
        )   

    else:   
        monthly_trend = df_filtered.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD']).agg(
            Total_Metric=(metric_col, 'sum')
        ).reset_index()
    # Corrected: Use .str.zfill(2) to apply zfill to each string in the Series
    monthly_trend["SORT_KEY"] = monthly_trend["FISCAL_YEAR"].astype(str) + monthly_trend["ACCOUNTING_PERIOD"].astype(str).str.zfill(2)
    monthly_trend = monthly_trend.sort_values(by="SORT_KEY")

    if not monthly_trend.empty:
        fig = px.line(
            monthly_trend, x='ACCOUNTING_PERIOD', y='Total_Metric', color='FISCAL_YEAR', markers=True,
            title=f"{metric_display_name} Monthly Trend for {selected_product} on {selected_ship}",
            labels={'ACCOUNTING_PERIOD': 'Accounting Period', 'Total_Metric': metric_display_name, 'FISCAL_YEAR': 'Fiscal Year'},
            color_discrete_sequence=px.colors.qualitative.Dark24
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True, key=f"product_monthly_trend_{selected_ship}_{selected_product}")

        # Generate insights for trend
        if len(monthly_trend) > 1:
            first_val = monthly_trend['Total_Metric'].iloc[0]
            last_val = monthly_trend['Total_Metric'].iloc[-1]
            percentage_change = ((last_val - first_val) / first_val) * 100 if first_val != 0 else 0

            trend_desc = "relatively stable"
            if percentage_change > 10:
                trend_desc = "an increasing trend"
            elif percentage_change < -10:
                trend_desc = "a decreasing trend"
            
            insight_text = f"""
            **Findings for {selected_product} Monthly Trend on {selected_ship}:**
            - The trend for **{selected_product}** shows **{trend_desc}** from {monthly_trend['FISCAL_YEAR'].iloc[0]} (Period {monthly_trend['ACCOUNTING_PERIOD'].iloc[0]}) to {monthly_trend['FISCAL_YEAR'].iloc[-1]} (Period {monthly_trend['ACCOUNTING_PERIOD'].iloc[-1]}).
            - The change over the entire period is approximately **{percentage_change:,.2f}%**.
            """
        else:
            insight_text = f"**Findings for {selected_product} Monthly Trend on {selected_ship}:** Not enough data points to determine a clear trend."
        
        st.markdown(insight_text)
        add_insight(     st.session_state["all_insights"],     title= f"Monthly Trend for {selected_product} on {selected_ship}", text = insight_text, chart = fig)
    else:
        st.info(f"No monthly trend data for product '{selected_product}' on ship '{selected_ship}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Monthly Trend for {selected_product} on {selected_ship}", text = "No data for product trend.")

# 5.Ranks components for a given ship and product (ship-centric flow)
def rank_components_by_ship_product(df, selected_ship, selected_product,metric_col,metric_display_name):

    global all_insights
    st.subheader(f"📊 Component Ranking (Ship: {selected_ship}, Product: {selected_product})")

    df_filtered = df[
        (df['SHIP_CD'] == selected_ship) &
        (df['RM_ROLLUP_PRODUCT_DESC'] == selected_product)
    ].copy()

    if df_filtered.empty:
        st.warning(f"No data for selected ship '{selected_ship}' and product '{selected_product}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Ranking for {selected_ship}, {selected_product}", text = "No data available.")
        return

    if metric_display_name in ('Passenger Days', 'Capacity Days'):
        st.info(f"{metric_display_name} is identical across components, so it’s excluded from this analysis.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Box Plot for {selected_ship} ({selected_product})",
            text = f"{metric_display_name} is not applicable for component-level breakdowns."
        )
        return pd.DataFrame(), None

    if metric_display_name in ('Per Capacity Day (Margin PCD)','Per Passenger Day (Margin PPD)'):
        df1 = df_filtered.groupby(['M0_AND_M1'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df_filtered.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID','M0_AND_M1'])[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['M0_AND_M1'], as_index=False)['t1'].sum()
        component_ranking = df1.merge(df2, on=['M0_AND_M1'], how='inner')
        component_ranking['Total_Amount'] = component_ranking['COMPONENT_AMOUNT'] / component_ranking['t1']
 
    else:   
        component_ranking = df_filtered.groupby('M0_AND_M1').agg(
            Total_Amount=('COMPONENT_AMOUNT', 'sum')
        ).reset_index().sort_values(by="Total_Amount", ascending=False)

    component_ranking['ColorLabel'] = component_ranking['Total_Amount'].apply(
    lambda x: 'Revenue' if x >= 0 else 'Cost')

    if not component_ranking.empty:
        if metric_display_name == 'Per Capacity Day (Margin PCD)':
            hover_metric = 'PCD'
        elif metric_display_name == 'Per Passenger Day (Margin PPD)':
            hover_metric = 'PPD'
        else:
            hover_metric = 'Margin $'  # No normalized value to show
        fig = px.bar(
        component_ranking,
        x='M0_AND_M1',
        y='Total_Amount',
        color='ColorLabel',  # This will be either green or red  # Optional: split each component into a separate panel
        title=f"{metric_display_name} by M0/M1 for {selected_ship}, {selected_product}",
        labels={
            'M0_AND_M1': 'M0_AND_M1',
            'Total_Amount': 'Margin',
            'ColorLabel': 'M0&M1'
        },
        barmode='group',
        color_discrete_map={'Revenue': 'green', 'Cost': 'red'}
    )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True, key=f"comp_ranking_{selected_ship}_{selected_product}")

        # Generate insights
        if len(component_ranking) > 0:
            top_component = component_ranking.iloc[0]
            # Fix for ValueError: The truth value of a Series is ambiguous.
            # bottom_component will be None if len(component_ranking) is 1.
            bottom_component = component_ranking.iloc[-1] if len(component_ranking) > 1 else None

            insight_text = f"""
            **Findings for Component Ranking:**
            - The **highest contributing component** is **{top_component['M0_AND_M1']}** with an amount of {top_component['Total_Amount']:,.2f}.
            """
            # Use 'is not None' for explicit boolean evaluation to prevent ValueError
            if bottom_component is not None:
                insight_text += f"""
            - The **lowest contributing component** is **{bottom_component['M0_AND_M1']}** with an amount of {bottom_component['Total_Amount']:,.2f}.
            """
            st.markdown(insight_text)
            add_insight(     st.session_state["all_insights"],     title= f"Component Ranking for {selected_ship}, {selected_product}", text = insight_text, chart = fig)
    else:
        st.info(f"No component data for selected ship '{selected_ship}' and product '{selected_product}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Ranking", text = "No data for selected entities.")

#-----------------------------------------------------------------------------------------------------------------------------------------------------
# Voyage Centric Flow-------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------------------
def bucketize(value, bucket_size=10):
    """
    Converts a numeric value into buckets:
    e.g. 12 → '10-20', 37 → '30-40'
    If already string like '10-20', returns as is.
    """
    if pd.isna(value):
        return None
    # If already in "X-Y" format
    if isinstance(value, str) and "-" in value:
        return value.strip()
 
    # Convert numeric → bucket
    try:
        value = float(value)
    except:
        return None
    lower = int((value // bucket_size) * bucket_size)
    upper = lower + bucket_size
    return f"{lower}-{upper}"

def cluster_voyages(df, target_voyage_id):
    """Cluster voyages into Top / Medium / Least Matches with bucketized GSS & LF."""
 
    # ----------------------
    # Bucketize input DF
    # ----------------------
    df = df.copy()
    df["GSS_BINNING"] = df["GSS"].apply(bucketize)
    df["LF_BINNING"] = (df["LF"] * 100).apply(bucketize)
    df['SEASON'] = df['ACCOUNTING_PERIOD'].astype(str)
    df['PORTCD_ACTIVITY']=df['PORTCD_ACTIVITY']
    # ----------------------
    # Extract target voyage
    # ----------------------
    voyage_row = df[df["VOYAGE_ID"] == target_voyage_id].iloc[0]
 
    target = {
        "RM_ROLLUP_PRODUCT_DESC": voyage_row["RM_ROLLUP_PRODUCT_DESC"],
        "SHIP_CLASS": voyage_row["SHIP_CLASS"],
        "GSS_BINNING": voyage_row["GSS_BINNING"],
        "LF_BINNING": voyage_row["LF_BINNING"],
        "SEASON": voyage_row["SEASON"],
        "PORTCD_ACTIVITY":voyage_row["PORTCD_ACTIVITY"]
    }
 
    # --------------------------
    # Tier 1 (Top Matches)
    # --------------------------
    tier1_filter = (
        (df["RM_ROLLUP_PRODUCT_DESC"] == target["RM_ROLLUP_PRODUCT_DESC"]) &
        (df["SHIP_CLASS"] == target["SHIP_CLASS"]) &
        (df["GSS_BINNING"] == target["GSS_BINNING"]) &
        (df["LF_BINNING"] == target["LF_BINNING"]) &
        (df["SEASON"] == target["SEASON"])&
        (df["PORTCD_ACTIVITY"]==target["PORTCD_ACTIVITY"])&
        (df["VOYAGE_ID"] != target_voyage_id)
    )
    tier1_df = df[tier1_filter]
    tier1_ids = set(tier1_df["VOYAGE_ID"].unique())
 
    # --------------------------
    # Tier 2 (Medium Matches)
    # --------------------------
    tier2_filter = (
        (df["RM_ROLLUP_PRODUCT_DESC"] == target["RM_ROLLUP_PRODUCT_DESC"]) &
        (df["SHIP_CLASS"] == target["SHIP_CLASS"]) &
        (df["LF_BINNING"] == target["LF_BINNING"]) &
        (df["PORTCD_ACTIVITY"]==target["PORTCD_ACTIVITY"])&
        (~df["VOYAGE_ID"].isin(tier1_ids)) &
        (df["VOYAGE_ID"] != target_voyage_id)
    )
    tier2_df = df[tier2_filter]
    tier2_ids = set(tier2_df["VOYAGE_ID"].unique())
 
    # --------------------------
    # Tier 3 (Least Matches)
    # --------------------------
    tier3_filter = (
        (df["RM_ROLLUP_PRODUCT_DESC"] == target["RM_ROLLUP_PRODUCT_DESC"]) &
        (df["SHIP_CLASS"] == target["SHIP_CLASS"]) &
        (df["PORTCD_ACTIVITY"]==target["PORTCD_ACTIVITY"])&
        (~df["VOYAGE_ID"].isin(tier1_ids)) &
        (~df["VOYAGE_ID"].isin(tier2_ids)) &
        (df["VOYAGE_ID"] != target_voyage_id)
    )
    tier3_df = df[tier3_filter]
 
    clusters = {
        "Top Matches": {"df": tier1_df, "cols": ["RM_ROLLUP_PRODUCT_DESC","SHIP_CLASS","GSS_BINNING","LF_BINNING","SEASON","PORTCD_ACTIVITY"]},
        "Medium Matches": {"df": tier2_df, "cols": ["RM_ROLLUP_PRODUCT_DESC","SHIP_CLASS","LF_BINNING","PORTCD_ACTIVITY"]},
        "Least Matches": {"df": tier3_df, "cols": ["RM_ROLLUP_PRODUCT_DESC","SHIP_CLASS","PORTCD_ACTIVITY"]}
    }
 
    return clusters, target, voyage_row.to_dict()

def format_with_unit(value):
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f} B"
    elif abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f} M"
    elif abs(value) >= 1_000:
        return f"{value / 1_000:.2f} K"
    else:
        return f"{value:.2f}"
def cluster_voyages_itinerary(df, selected_ship, selected_portcd):
    """Cluster all voyages matching a ship + itinerary route."""
    df = df.copy()
    df["GSS_BINNING"] = df["GSS"].apply(bucketize)
    df["LF_BINNING"] = (df["LF"]*100).apply(bucketize)
    df['SEASON'] = df['ACCOUNTING_PERIOD'].astype(str)
    
    # Filter for the selected ship + itinerary
    itinerary_df = df[(df['SHIP_CD'] == selected_ship) & (df['PORTCD_ACTIVITY'] == selected_portcd)]
    if itinerary_df.empty:
        return {}, {}, {}
    
    # Use first voyage as reference (optional)
    voyage_row = itinerary_df.iloc[0]
    target = {
        "RM_ROLLUP_PRODUCT_DESC": voyage_row["RM_ROLLUP_PRODUCT_DESC"],
        "SHIP_CLASS": voyage_row["SHIP_CLASS"],
        "GSS_BINNING": voyage_row["GSS_BINNING"],
        "LF_BINNING": voyage_row["LF_BINNING"],
        "SEASON": voyage_row["SEASON"],
        "PORTCD_ACTIVITY": voyage_row["PORTCD_ACTIVITY"]
    }
    
    # Looser filtering for itinerary mode
    tier1_df = itinerary_df[(itinerary_df["RM_ROLLUP_PRODUCT_DESC"] == target["RM_ROLLUP_PRODUCT_DESC"])]
    tier2_df = itinerary_df[(itinerary_df["RM_ROLLUP_PRODUCT_DESC"] == target["RM_ROLLUP_PRODUCT_DESC"])]
    tier3_df = itinerary_df.copy()
    
    clusters = {
        "Top Matches": {"df": tier1_df},
        "Medium Matches": {"df": tier2_df},
        "Least Matches": {"df": tier3_df}
    }
    
    return clusters, target, voyage_row.to_dict()


def plot_clustered_voyages_bar_chart(
    clustered_df, tier_name, base_row,
    primary_metric_col, selected_metric_display_name,
    key_suffix, order_column=None, order_type=None
):
    global all_insights

    if clustered_df.empty:
        st.info(f"No data for {tier_name}.")
        return

    clustered_df = clustered_df.copy()

    # Base row fields
    PRODUCT = base_row.get("RM_ROLLUP_PRODUCT_DESC", "N/A")
    SHIP_CLASS = base_row.get("SHIP_CLASS", "N/A")
    GSS = bucketize(base_row.get("GSS_BINNING"))
    LF = bucketize(base_row.get("LF_BINNING"))
    SEASON = base_row.get("SEASON", "N/A")
    PORTCD_ACTIVITY=base_row.get("PORTCD_ACTIVITY","N/A")

    # Show matched parameters
    show_params = st.checkbox("Show Matched Parameters", key=f"{tier_name}_params_{key_suffix}")
    if show_params:
        if tier_name == "Top Matches":
            params = {"Product": PRODUCT, "Ship Class": SHIP_CLASS, "GSS Binning": GSS, "LF Binning": LF, "Season": SEASON,"PORTCD_ACTIVITY":PORTCD_ACTIVITY}
        elif tier_name == "Medium Matches":
            params = {"Product": PRODUCT, "Ship Class": SHIP_CLASS, "LF Binning": LF,"PORTCD_ACTIVITY":PORTCD_ACTIVITY}
        else:
            params = {"Product": PRODUCT, "Ship Class": SHIP_CLASS,"PORTCD_ACTIVITY":PORTCD_ACTIVITY}
        st.table(pd.DataFrame(params.items(), columns=["Parameter", "Value"]))

    # Compute Metrics
    if selected_metric_display_name in ("Per Capacity Day (Margin PCD)", "Per Passenger Day (Margin PPD)"):
        metric_df = clustered_df.groupby("VOYAGE_ID").agg(
            Sum_Component_Amount=("COMPONENT_AMOUNT", "sum"),
            First_Primary_Metric=(primary_metric_col, "first"),
        ).assign(
            Total_Metric=lambda x: x["Sum_Component_Amount"] / x["First_Primary_Metric"]
        )
    elif selected_metric_display_name == "Passenger Days":
        metric_df = clustered_df.groupby("VOYAGE_ID").agg(Total_Metric=("NEW_PRTD_PAX_DAYS", "first"))
    elif selected_metric_display_name == "Capacity Days":
        metric_df = clustered_df.groupby("VOYAGE_ID").agg(Total_Metric=("NEW_PRTD_CAPS_DAYS", "first"))
    else:
        metric_df = clustered_df.groupby("VOYAGE_ID").agg(Total_Metric=(primary_metric_col, "sum"))

    # Tooltip info
    tooltip_df = clustered_df.groupby("VOYAGE_ID").agg(
        ITINERARY=("PORTCD_ACTIVITY", lambda x: ", ".join(sorted(set(x.dropna())))),
        CAPS_DAYS=("NEW_PRTD_CAPS_DAYS", "first"),
        PAX_DAYS=("NEW_PRTD_PAX_DAYS", "first"),
        LOAD_FACTOR=("LF", "first")
    )

    agg_df = metric_df.merge(tooltip_df, left_index=True, right_index=True).reset_index()

    if agg_df.empty:
        st.info(f"No voyages available for {tier_name}.")
        return

    # Sort values
    if order_column and order_type:
        agg_df = agg_df.sort_values(by="Total_Metric", ascending=(order_type.lower() == "asc"))
    else:
        agg_df = agg_df.sort_values(by="Total_Metric", ascending=False)

    # Plot chart
    fig = px.bar(
        agg_df,
        x="VOYAGE_ID",
        y="Total_Metric",
        color="VOYAGE_ID",
        title=f"{selected_metric_display_name} — {tier_name}",
        labels={"VOYAGE_ID": "Voyage ID", "Total_Metric": selected_metric_display_name},
        color_discrete_sequence=px.colors.qualitative.Alphabet,
        hover_data={"ITINERARY": True, "CAPS_DAYS": True, "PAX_DAYS": True, "LOAD_FACTOR": ":.2f"},
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True, key=f"clustered_voyages_{key_suffix}")

    # Insight Generation
    voyage_count = agg_df["VOYAGE_ID"].nunique()
    top = agg_df.iloc[0]
    bottom = agg_df.iloc[-1]
    avg = agg_df["Total_Metric"].mean()

    insight_text = (
        f"📊 **Cluster Insights — {tier_name}**\n"
        f"- Total voyages in this cluster: **{voyage_count}**\n"
        f"- **Highest** {selected_metric_display_name}: **{format_with_unit(top['Total_Metric'])}** (Voyage **{top['VOYAGE_ID']}**)\n"
        f"- **Lowest** {selected_metric_display_name}: **{format_with_unit(bottom['Total_Metric'])}** (Voyage **{bottom['VOYAGE_ID']}**)\n"
        f"- **Average** {selected_metric_display_name}: **{format_with_unit(avg)}**"
    )


    st.markdown(insight_text)

    # Save in global insights
    if "all_insights" in st.session_state:
        add_insight(
            st.session_state["all_insights"],
            title=f"{tier_name} Cluster Insights",
            text=insight_text,
            chart=fig
        )


#Itinerary flow ( voyage centric)
def portcd_activity_analysis_flow(df, filtered_df, primary_metric_col, selected_metric_display_name, order_column=None, order_type=None):
    """
    Itinerary-based Voyage Insights (PortCD Activity)
    Respects global filters and clusters voyages by relaxed itinerary logic.
    """
    st.header("🔎 Itinerary Based Voyage Insights")
    global all_insights

    # --- Step 0: Ensure filtered_df (global filters) is available ---
    if filtered_df is None or filtered_df.empty:
        st.warning("No voyages available after applying global filters.")
        st.stop()
    
    # --- Step 1: Filter by Product ---
    st.subheader("1. Filter by Product")
    
    if "selected_product" not in st.session_state:
        st.session_state.selected_product = None
    
    available_products = sorted(filtered_df['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist())
    
    query_prompt = st.session_state.get("user_query", "").lower()
    matched_index = None
    for i, prod in enumerate(available_products):
        if query_prompt and query_prompt in prod.lower():
            matched_index = i
            break
        elif st.session_state.selected_product == prod:
            matched_index = i
            break
    
    selected_product = st.selectbox(
        "Select Product:",
        available_products,
        index=matched_index if matched_index is not None else 0,
        key="product_filter"
    )
    st.session_state.selected_product = selected_product
    
    df_product = filtered_df[filtered_df['RM_ROLLUP_PRODUCT_DESC'] == selected_product]
    if df_product.empty:
        st.warning("No data available for the selected product after global filters.")
        st.stop()
    
    # --- Step 2: Filter by Itinerary Route (PortCD Activity) ---
    st.subheader("2. Filter by Itinerary Route")
    
    if "selected_portcd" not in st.session_state:
        st.session_state.selected_portcd = None
    
    available_activities = sorted(df_product['PORTCD_ACTIVITY'].dropna().unique().tolist())
    
    matched_index = 0
    for i, act in enumerate(available_activities):
        if query_prompt and query_prompt in act.lower():
            matched_index = i
            break
        elif st.session_state.selected_portcd == act:
            matched_index = i
            break
    
    selected_portcd = st.selectbox(
        "Select Itinerary Route:",
        available_activities,
        index=matched_index,
        key="portcd_filter"
    )
    st.session_state.selected_portcd = selected_portcd
    
    df_filtered = df_product[df_product['PORTCD_ACTIVITY'] == selected_portcd]
    if df_filtered.empty:
        st.warning("No voyages found for the selected itinerary route after global filters.")
        st.stop()
    
    # ----------------------
    # Step 3: Ship-wise Contribution
    # ----------------------
    st.subheader("3. Ships Using This Itinerary Route")
    
    # Always calculate voyage counts first
    voyage_counts = (
        df_filtered.groupby('SHIP_CD')['VOYAGE_ID']
        .nunique()
        .reset_index(name='Unique_Voyages')
    )
    
    # --- Aggregation Logic for Primary Metric ---
    if selected_metric_display_name in ('Per Capacity Day (Margin PCD)','Per Passenger Day (Margin PPD)'):
        df1 = df_filtered.groupby(['SHIP_CD'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df_filtered.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        ).agg(
            t1=(primary_metric_col,'first'),
            Ship_Caps=('NEW_PRTD_CAPS_DAYS', 'first'),
            Ship_Pax=('NEW_PRTD_PAX_DAYS', 'first')
        ).reset_index()
        df2 = df2.groupby(['SHIP_CD'], as_index=False)[['t1','Ship_Caps','Ship_Pax']].sum()
        ship_summary = df1.merge(df2, on=['SHIP_CD'], how='inner')
        ship_summary['Total_Amount'] = ship_summary['COMPONENT_AMOUNT'] / ship_summary['t1']
    elif selected_metric_display_name in ('Passenger Days', 'Capacity Days'):
        df2 = df_filtered.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        ).agg(
            Total_Amount=(primary_metric_col,'first'),
            Ship_Caps=('NEW_PRTD_CAPS_DAYS', 'first'),
            Ship_Pax=('NEW_PRTD_PAX_DAYS', 'first')
        ).reset_index()
    
        ship_summary = df2.groupby(['SHIP_CD'], as_index=False)[['Total_Amount','Ship_Caps','Ship_Pax']].sum()
    
    
    else:
        df2 = df_filtered.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        ).agg(
            Total_Amount=('COMPONENT_AMOUNT','sum'),
            Ship_Caps=('NEW_PRTD_CAPS_DAYS', 'first'),
            Ship_Pax=('NEW_PRTD_PAX_DAYS', 'first')
        ).reset_index()
    
        ship_summary = df2.groupby(['SHIP_CD'], as_index=False)[['Total_Amount','Ship_Caps','Ship_Pax']].sum()
    
    # Merge voyage counts
    ship_summary = ship_summary.merge(voyage_counts, on='SHIP_CD', how='left')

    # --- Aggregation and Sorting Logic for Order Column ---
    use_custom_order = order_column is not None and order_column.upper() != 'NONE'
    if use_custom_order:
        if order_column.upper() in ['GSS', 'CII']:
            order_col_for_agg = 'CII_SCORE' if order_column.upper() == 'CII' else 'GSS'
            temp_df = df_filtered.groupby(['VOYAGE_ID', 'SHIP_CD'], as_index=False)[order_col_for_agg].first()
            order_col_avg_df = temp_df.groupby('SHIP_CD', as_index=False)[order_col_for_agg].mean().rename(columns={order_col_for_agg: 'Order_Metric'})
            ship_summary = ship_summary.merge(order_col_avg_df, on='SHIP_CD', how='left')
        elif order_column.upper() in ['LOAD FACTOR', 'LF']:
            temp_df = df_filtered.groupby(['VOYAGE_ID', 'SHIP_CD'], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
            temp_df = temp_df.groupby('SHIP_CD', as_index=False).sum()
            temp_df['Order_Metric'] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
            ship_summary = ship_summary.merge(temp_df[['SHIP_CD', 'Order_Metric']], on='SHIP_CD', how='left')
    
    # Sort the dataframe
    if use_custom_order and 'Order_Metric' in ship_summary.columns:
        is_ascending = (order_type.lower() == 'asc')
        ship_summary = ship_summary.sort_values(by='Order_Metric', ascending=is_ascending)
        sorting_info_text = f"by average '{order_column}' in {order_type} order."
    else:
        ship_summary = ship_summary.sort_values(by='Total_Amount', ascending=False)
        sorting_info_text = f"by '{selected_metric_display_name}' in descending order (default)."

    # --- Plotting with dual axis if applicable ---
    if use_custom_order and 'Order_Metric' in ship_summary.columns:
        ship_summary['Total_Amount_Formatted'] = ship_summary['Total_Amount'].round(2).astype(str)
        if order_column.upper() in ['LOAD FACTOR', 'LF']:
            ship_summary['Order_Metric_Formatted'] = f"{order_column}: " + (ship_summary['Order_Metric'] * 100).round(2).astype(str) + '%'
            yaxis2_title = f"{order_column}"
        elif order_column.upper() in ['GSS', 'CII']:
            ship_summary['Order_Metric_Formatted'] = f"{order_column} (Average): " + ship_summary['Order_Metric'].round(2).astype(str)
            yaxis2_title = f"{order_column} (Average)"
        else:
            ship_summary['Order_Metric_Formatted'] = f"{order_column}: " + ship_summary['Order_Metric'].round(2).astype(str)
            yaxis2_title = f"{order_column}"
        
        fig1 = make_subplots(specs=[[{"secondary_y": True}]])
        fig1.add_trace(go.Bar(x=ship_summary['SHIP_CD'], y=ship_summary['Total_Amount'], name=selected_metric_display_name, customdata=ship_summary[['Total_Amount_Formatted', 'Order_Metric_Formatted']], hovertemplate=f"<b>Ship:</b> %{{x}}<br><b>{selected_metric_display_name}:</b> %{{customdata[0]}}<br>%{{customdata[1]}}<extra></extra>"), secondary_y=False)
        fig1.add_trace(go.Scatter(x=ship_summary['SHIP_CD'], y=ship_summary['Order_Metric'], mode='lines+markers', name=yaxis2_title, line=dict(color='red', width=2), marker=dict(size=8, color='red'), customdata=ship_summary[['Total_Amount_Formatted', 'Order_Metric_Formatted']], hovertemplate=f"<b>Ship:</b> %{{x}}<br><b>{selected_metric_display_name}:</b> %{{customdata[0]}}<br>%{{customdata[1]}}<extra></extra>"), secondary_y=True)
        fig1.update_layout(title_text=f"{selected_metric_display_name} by Ship for Selected Itinerary Route (Sorted {sorting_info_text})", xaxis_title="Ship", yaxis_title=selected_metric_display_name, yaxis2_title=yaxis2_title, legend=dict(x=1.05, y=1))
    
    else:
        # Simple bar chart
        fig1 = px.bar(
            ship_summary,
            x='SHIP_CD',
            y='Total_Amount',
            hover_data={'Ship_Caps': True, 'Ship_Pax': True, 'Unique_Voyages': True},
            labels={'Total_Amount': selected_metric_display_name, 'SHIP_CD': 'Ship', 'Unique_Voyages': 'Voyage Count'},
            title=f"{selected_metric_display_name} by Ship for Selected Itinerary Route",
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
    
    st.plotly_chart(fig1, use_container_width=True)
    
    if not ship_summary.empty:
        top_ship = ship_summary.iloc[0]
        insight_text = f"**Findings of Itinerary Performance Analysis for {selected_portcd}:**\n\n"
        insight_text += f"""- The **top performing ship** on this itinerary is **{top_ship['SHIP_CD']}**
        with a total {selected_metric_display_name.lower()} of **{top_ship['Total_Amount']:,.2f}**
        across {top_ship['Unique_Voyages']} voyage(s).
        *The ranking is {sorting_info_text}*
        """
        st.markdown(insight_text)
        add_insight(st.session_state["all_insights"],     title= f"Top Ship for {selected_portcd}", text = insight_text, chart = fig1)
    
    # ------------------------------------
    # Step 4: Overall Component Contribution
    # ------------------------------------

    st.subheader("4. Overall Component Contribution")
    
    if selected_metric_display_name in ('Passenger Days', 'Capacity Days'):
        st.info("Passenger Days and Capacity Days are identical across all components, so this metric isn’t meaningful for component-level breakdowns.")
        component_contribution = pd.DataFrame()  # Prevents plotting and insights
    
    else:
        if selected_metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
            df1 = df_filtered.groupby(['M0_AND_M1'], as_index=False)['COMPONENT_AMOUNT'].sum()
            df2 = df_filtered.groupby(
                ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID','M0_AND_M1']
            )[primary_metric_col].first().reset_index(name='t1')
            df2 = df2.groupby(['M0_AND_M1'], as_index=False)['t1'].sum()
            component_contribution = df1.merge(df2, on=['M0_AND_M1'], how='inner')
            component_contribution['Total_Amount'] = component_contribution['COMPONENT_AMOUNT'] / component_contribution['t1']
        else:
            component_contribution = (
                df_filtered.groupby('M0_AND_M1')['COMPONENT_AMOUNT']
                .sum().reset_index(name='Total_Amount')
            )
        component_contribution['Total_Amount1'] = component_contribution['Total_Amount'].abs()
        component_contribution = component_contribution[component_contribution['Total_Amount'] != 0]
    
        if not component_contribution.empty:
            fig2 = px.pie(
                component_contribution,
                names='M0_AND_M1',
                values='Total_Amount1',
                title="Component Distribution for Selected PortCD Activity",
                labels={'M0_AND_M1': 'Component', 'Total_Amount': selected_metric_display_name},
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            st.plotly_chart(fig2, use_container_width=True)
    
            top_component = component_contribution.sort_values('Total_Amount', ascending=False).iloc[0]
            insight_text = f"**Component Distribution for Itinerary {selected_portcd}:**\n\n"
            insight_text += f""" - The **most significant component** is **{top_component['M0_AND_M1']}**, contributing **{top_component['Total_Amount']:,.2f}** in {selected_metric_display_name.lower()}. """
            st.markdown(insight_text)
            add_insight(     st.session_state["all_insights"],     title= f"Top Component for {selected_portcd}", text = insight_text, chart = fig2)



    # Step 5: Filter by Ship and Component Breakdown
    st.subheader("5.Drill Down to Specific Ship")
    ship_options = df_filtered['SHIP_CD'].dropna().unique().tolist()
    selected_ship = st.selectbox("Select Ship:", sorted(ship_options), key="ship_filter")

    df_ship = df_filtered[df_filtered['SHIP_CD'] == selected_ship]
    if df_ship.empty:
        st.warning("No data for selected ship.")
        return

    st.subheader(f"6. Component Contribution for {selected_ship}")

    if selected_metric_display_name in ('Passenger Days', 'Capacity Days'):
        st.info("Passenger Days and Capacity Days are identical across all components, so this metric isn’t meaningful for component-level breakdowns.")
        ship_component = pd.DataFrame()  # Prevents plotting and insights
    
    else:
        if selected_metric_display_name in ('Per Capacity Day (Margin PCD)','Per Passenger Day (Margin PPD)'):
            df1 = df_ship.groupby(['M0_AND_M1'], as_index=False)['COMPONENT_AMOUNT'].sum()
            df2 = df_ship.groupby(
                ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID','M0_AND_M1']
            )[primary_metric_col].first().reset_index(name='t1')
            df2 = df2.groupby(['M0_AND_M1'], as_index=False)['t1'].sum()
            ship_component = df1.merge(df2, on=['M0_AND_M1'], how='inner')
            ship_component['Total_Amount'] = ship_component['COMPONENT_AMOUNT'] / ship_component['t1']
        else:
            ship_component = df_ship.groupby(['SHIP_CLASS', 'M0_AND_M1']).agg(
                Total_Amount=('COMPONENT_AMOUNT', 'sum')
            ).reset_index()
        ship_component['ColorLabel'] = ship_component['Total_Amount'].apply(
            lambda x: 'Revenue' if x >= 0 else 'Cost'
        )

        if not ship_component.empty:
            fig3 = px.bar(
                ship_component,
                x='M0_AND_M1',
                y='Total_Amount',
                color='ColorLabel',
                title=f"Component Contribution for Ship: {selected_ship}",
                labels={
                    'M0_AND_M1': 'M0_AND_M1',
                    'Total_Amount': selected_metric_display_name,
                    'ColorLabel': 'M0&M1'
                },
                barmode='group',
                color_discrete_map={'Revenue': 'green', 'Cost': 'red'}
            )
            st.plotly_chart(fig3, use_container_width=True)
    
            dominant_component = ship_component.sort_values('Total_Amount', ascending=False).iloc[0]
            insight_text = f"**Component Distribution for Itinerary {selected_portcd} on {selected_ship}:**\n\n"
            insight_text += f"""- The ship {selected_ship} operating on the {selected_portcd} itinerary yields a total of ${ship_component['Total_Amount'].sum():,.2f} \n\n- The top contributing component is {dominant_component['M0_AND_M1']}, accounting for ${dominant_component['Total_Amount']:,.2f}. """
            st.markdown(insight_text)
            add_insight(     st.session_state["all_insights"],     title= f"Component Performance on {selected_ship}", text = insight_text, chart = fig3)
    
    
    
        st.markdown("---")
        st.subheader("7. Like-for-Like Voyages on Selected Itinerary Route (Clustered by Key Attributes)")
        
    # --- Get representative voyage for clustering ---
        representative_voyages = (
            df[(df['SHIP_CD'] == selected_ship) & (df['PORTCD_ACTIVITY'] == selected_portcd)]
            ['VOYAGE_ID'].dropna().unique()
        )
        
        if len(representative_voyages) == 0:
            st.warning(f"No voyages found for clustering using {selected_ship} and {selected_portcd}.")
            return
        
        # Use first voyage for clustering (as per requirement)
        voyage_for_clustering = representative_voyages[0]
        
        # Call clustering function
        clustered_results, target_voyage_dict, target_voyage_row = cluster_voyages_itinerary(
            df_filtered, selected_ship, selected_portcd
        )

        
        # --- Tier mapping ---
        tier_mapping = {
            "Top Matches": "Top Match",
            "Medium Matches": "Medium Matches",
            "Least Matches": "Least Matches"
        }
        
        shown_voyages = set()  # Track already shown voyages
        
        # --- Iterate across tiers ---
        for tier_code in ["Top Matches", "Medium Matches", "Least Matches"]:
            data = clustered_results.get(tier_code, {})
            tier_df = data.get('df', pd.DataFrame())
            tier_label = tier_mapping.get(tier_code, tier_code)
        
            # Exclude already shown voyages
            tier_df = tier_df[~tier_df['VOYAGE_ID'].isin(shown_voyages)]
        
            if not tier_df.empty:
                unique_voyages = tier_df['VOYAGE_ID'].nunique()
                if unique_voyages == 0:
                    st.info(f"No unique voyages found in {tier_label} for selected itinerary.")
                    continue
        
                shown_voyages.update(tier_df['VOYAGE_ID'])
                with st.expander(f"{tier_label} for {selected_portcd}", expanded=False):
                    plot_clustered_voyages_bar_chart(
                    clustered_df=tier_df,
                    tier_name=tier_code,
                    base_row=target_voyage_dict,
                    primary_metric_col=primary_metric_col,
                    selected_metric_display_name=selected_metric_display_name,
                    key_suffix=f"{tier_code}_{selected_ship}",
                    order_column=order_column,
                    order_type=order_type
                    )
            # else:
            #     st.info("No voyages found in Least Matches tier or fallback.")


# component breakdown( voyage centric)
def plot_voyage_component_contribution(df, target_voyage_id, metric_col, metric_display_name):
    """Shows the contribution of a specific voyage across each component."""
    global all_insights
    st.subheader(f"📊 Component Contribution for Voyage: {target_voyage_id}")
    
    voyage_df = df[df['VOYAGE_ID'] == target_voyage_id].copy()
    if voyage_df.empty:
        st.warning(f"No data found for Voyage ID '{target_voyage_id}'.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Contribution for Voyage {target_voyage_id}", 
            text = f"No data found for Voyage ID '{target_voyage_id}'."
        )
        return
    
    # --- Skip Passenger Days & Capacity Days ---
    if metric_display_name in ('Passenger Days', 'Capacity Days'):
        st.info("Passenger Days and Capacity Days are identical across all components, so this metric isn’t meaningful for component-level breakdowns.")
        add_insight(     st.session_state["all_insights"],     title= f"Component Contribution for Voyage {target_voyage_id}", 
            text = f"{metric_display_name} is not applicable for component-level breakdowns."
        )
        return

    # --- PCD / PPD case ---
    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = voyage_df.groupby(['M0_AND_M1'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = voyage_df.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID','M0_AND_M1']
        )[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['M0_AND_M1'], as_index=False)['t1'].sum()
        component_contribution = df1.merge(df2, on=['M0_AND_M1'], how='inner')
        component_contribution['Total_Amount'] = component_contribution['COMPONENT_AMOUNT'] / component_contribution['t1']

    # --- Margin $ and other metrics ---
    else:
        component_contribution = voyage_df.groupby('M0_AND_M1').agg(
            Total_Amount=('COMPONENT_AMOUNT', 'sum')
        ).reset_index().sort_values(by="Total_Amount", ascending=False)
    component_contribution['Total_Amount1'] = component_contribution['Total_Amount'].abs()
    # Filter out components with zero total amount for better visualization in pie chart
    component_contribution = component_contribution[component_contribution['Total_Amount1'] != 0]

    # --- Plot only if data exists ---
    if not component_contribution.empty:
        if metric_display_name == 'Per Capacity Day (Margin PCD)':
            hover_metric = 'PCD'
        elif metric_display_name == 'Per Passenger Day (Margin PPD)':
            hover_metric = 'PPD'
        elif metric_display_name == "Passenger Days":   
            hover_metric = 'NEW_PRTD_PAX_DAYS'
        elif metric_display_name == "Capacity Days":   
            hover_metric = 'NEW_PRTD_CAPS_DAYS'
        else:
            hover_metric = 'Margin $'  # No normalized value to show
        fig = px.pie(
            component_contribution, names='M0_AND_M1', values='Total_Amount1',
            title=f"Component Distribution for Voyage {target_voyage_id}",
            labels={'M0_AND_M1': 'Component', 'Total_Amount': hover_metric},
            color_discrete_sequence=px.colors.qualitative.Plotly

        )
        st.plotly_chart(fig, use_container_width=True, key=f"voyage_comp_pie_{target_voyage_id}")

        if len(component_contribution) > 0:
        
            # Compute summary statistics
            component_count = component_contribution["M0_AND_M1"].nunique()
            top = component_contribution.iloc[0]
            bottom = component_contribution.iloc[-1]
            total_amount = component_contribution["Total_Amount"].sum()
        
            insight_text = (
                f"📊 **Component Contribution Insights — Voyage {target_voyage_id}**\n"
                f"- **Total {metric_display_name.lower()}** for this voyage: **{total_amount:,.2f}**\n"
                f"- **Highest contributing component:** **{top['M0_AND_M1']}** "
                f"with **{top['Total_Amount']:,.2f}**\n"
                f"- **Lowest contributing component:** **{bottom['M0_AND_M1']}** "
                f"with **{bottom['Total_Amount']:,.2f}**\n"
            )
        
            st.markdown(insight_text)
        
            add_insight(
                st.session_state["all_insights"],
                title=f"Component Contribution — Voyage {target_voyage_id}",
                text=insight_text,
                chart=fig
            )

        else:
            insight_text = (
                f"📊 **Component Contribution Insights — Voyage {target_voyage_id}**\n"
                f"No non-zero component data found."
            )
            st.markdown(insight_text)
            
            add_insight(
                st.session_state["all_insights"],
                title=f"Component Contribution — Voyage {target_voyage_id}",
                text=insight_text
            )
   
#----------------------------------------------------------------------------------------------------------------------------------------------------
# Outlier Analysis Flow-----------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------------------

# outlier (outlier analysis)
def calculate_zscore_series(x_series):
    """
    Calculates the Z-score for a pandas Series using scipy.stats.zscore,
    handling edge cases for UDF compatibility.
    """
    if len(x_series) > 1:
        # Using population standard deviation (ddof=0) for consistency with common z-score definitions
        return pd.Series(zscore(x_series, ddof=0), index=x_series.index)
    else:
        return pd.Series(np.nan, index=x_series.index)

# outlier analysis flow
def outlier_analysis_flow(df, filtered_df, corrected_query, primary_metric_col, selected_metric_display_name):
    st.header("Outlier Analysis")
    st.write("Identify outlier performance patterns—such as unusually high costs or low revenues—for products or ships based on selected components.")

    st.warning("##### 📢 Please note:\n Outlier analysis is limited to **2023 and 2024**. Due to incomplete data, **results for 2025 may not be reliable**.")

    analysis_type = st.radio(
        "Choose Outlier Analysis Type:",
        ["Product Outliers (across Ships)", "Ship Outliers (across Products)"],
        key="outlier_analysis_type"
    )

    if 'M0_AND_M1' not in filtered_df.columns:
        st.error("Expected column 'M0_AND_M1' not found in filtered data.")
        return

    if selected_metric_display_name == "Passenger Days":
        st.info(f'The Selected Metric **{selected_metric_display_name}** is static across the M0 & M1 Components')
    elif selected_metric_display_name == "Capacity Days":
        st.info(f'The Selected Metric **{selected_metric_display_name}** is static across the M0 & M1 Components')
    else:
        " "

    all_components = sorted(filtered_df['M0_AND_M1'].dropna().unique().tolist())
    selected_component = st.selectbox(
        "Select Component for Outlier Analysis:",
        options=all_components,
        key="outlier_component_select"
    )
    if not selected_component:
        st.warning("Please select a component to proceed with outlier analysis.")
        return

    component_df = filtered_df[filtered_df['M0_AND_M1'] == selected_component].copy()
    if component_df.empty:
        st.warning(f"No data available for the selected component '{selected_component}' with current filters.")
        return

    def zscore_series(s: pd.Series) -> pd.Series:
        s = s.astype(float)
        if s.count() <= 1:
            return pd.Series(0.0, index=s.index)
        std = s.std(ddof=0)
        if std == 0 or np.isclose(std, 0):
            return pd.Series(0.0, index=s.index)
        return (s - s.mean()) / std

    def iqr_flags(s: pd.Series) -> pd.Series:
        s = s.astype(float)
        if s.count() <= 1:
            return pd.Series(False, index=s.index)
        q1 = s.quantile(0.25)
        q3 = s.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        return s < lower

    # ---------------- PRODUCT OUTLIERS ----------------
    if analysis_type == "Product Outliers (across Ships)":
        st.subheader(f"Product Outliers for {selected_component} (across Ships)")

        selected_product = st.selectbox(
            "Select Product to Analyze:",
            options=sorted(component_df['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist()),
            key="outlier_product_select"
        )
        if not selected_product:
            return

        df_filtered = component_df[component_df['RM_ROLLUP_PRODUCT_DESC'] == selected_product].copy()
        if df_filtered.empty:
            st.info("No rows for the selected product under current filters.")
            return

        df_filtered = (
            df_filtered
            .groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'M0_AND_M1'])[
                ['COMPONENT_AMOUNT', 'NEW_PRTD_CAPS_DAYS', 'NEW_PRTD_PAX_DAYS']
            ]
            .sum()
            .reset_index()
        )

        df_filtered['MONTHLY_PCD'] = df_filtered['COMPONENT_AMOUNT'] / df_filtered['NEW_PRTD_CAPS_DAYS']
        df_filtered['MONTHLY_PPD'] = df_filtered['COMPONENT_AMOUNT'] / df_filtered['NEW_PRTD_PAX_DAYS']

        grouped_data = df_filtered.copy()

        # Select the correct metric column
        if selected_metric_display_name == "Margin $":
            primary_metric_col = 'COMPONENT_AMOUNT'
        elif selected_metric_display_name == "Per Capacity Day (Margin PCD)":
            primary_metric_col = 'MONTHLY_PCD'
        elif selected_metric_display_name == "Per Passenger Day (Margin PPD)":
            primary_metric_col = 'MONTHLY_PPD'
        elif selected_metric_display_name == "Passenger Days":
            primary_metric_col = 'NEW_PRTD_PAX_DAYS'
        elif selected_metric_display_name == "Capacity Days":
            primary_metric_col = 'NEW_PRTD_CAPS_DAYS'
        else:
            st.error("Unsupported metric selected for trend plot.")
            return

        # Compute per-ship stats
        ship_stats = (
            grouped_data.groupby('SHIP_CD')[primary_metric_col]
            .agg(
                ship_mean='mean',
                ship_median='median',
                ship_q1=lambda x: x.quantile(0.25),
                ship_q3=lambda x: x.quantile(0.75),
                ship_min='min',
                ship_max='max'
            )
            .reset_index()
        )

        # Merge back into grouped_data
        grouped_data = grouped_data.merge(ship_stats, on='SHIP_CD', how='left')

        grouped_data['NORMALIZED_METRIC'] = (
            grouped_data.groupby('SHIP_CD')[primary_metric_col].transform(zscore_series)
        )
        grouped_data['IS_OUTLIER'] = (
            grouped_data.groupby('SHIP_CD')[primary_metric_col].transform(iqr_flags)
        )

        grouped_data['customdata'] = grouped_data.apply(
            lambda row: [
                row[primary_metric_col],
                row['FISCAL_YEAR'],
                row['ACCOUNTING_PERIOD'],
                row['ship_mean'], row['ship_median'],
                row['ship_q1'], row['ship_q3'],
                row['ship_min'], row['ship_max']
            ],
            axis=1
        )

        fig = go.Figure()

        fig.add_trace(go.Box(
            y=grouped_data['NORMALIZED_METRIC'],
            x=grouped_data['SHIP_CD'],
            boxpoints='all',
            jitter=0.5,
            pointpos=-1.8,
            marker=dict(color='aqua', size=6, opacity=0.7),
            line=dict(color='darkcyan'),
            name='',
            customdata=grouped_data['customdata'].tolist(),
            hovertemplate=(
                f"<b>Component: {selected_component}</b><br>"
                f"{selected_metric_display_name}: %{{customdata[0]:.2f}}<br>"
                "Fiscal Year: %{customdata[1]}<br>"
                "Month: %{customdata[2]}<br>"
                "<br>--- Component Stats ---<br>"
                f"Mean ({selected_metric_display_name}): %{{customdata[3]:.2f}}<br>"
                f"Median ({selected_metric_display_name}): %{{customdata[4]:.2f}}<br>"
                f"Q1 ({selected_metric_display_name}): %{{customdata[5]:.2f}}<br>"
                f"Q3 ({selected_metric_display_name}): %{{customdata[6]:.2f}}<br>"
                f"Min ({selected_metric_display_name}): %{{customdata[7]:.2f}}<br>"
                f"Max ({selected_metric_display_name}): %{{customdata[8]:.2f}}<br>"
                "<extra></extra>"
            ),
            showlegend=False
        ))

        outliers_df = grouped_data[grouped_data['IS_OUTLIER']].copy()
        outliers_df['RM_ROLLUP_PRODUCT_DESC'] = selected_product
        outliers_df['M0_AND_M1'] = selected_component

        if not outliers_df.empty:
            # ✅ removed redundant merge here
            outliers_df['customdata'] = outliers_df.apply(
                lambda row: [
                    row[primary_metric_col],
                    row['FISCAL_YEAR'],
                    row['ACCOUNTING_PERIOD'],
                    row['ship_mean'], row['ship_median'],
                    row['ship_q1'], row['ship_q3'],
                    row['ship_min'], row['ship_max']
                ],
                axis=1
            )

            fig.add_trace(
                go.Scatter(
                    x=outliers_df['SHIP_CD'],
                    y=outliers_df['NORMALIZED_METRIC'],
                    mode='markers',
                    marker=dict(color='red', size=10, symbol='circle'),
                    name='IQR Outlier',
                    customdata=outliers_df['customdata'].tolist(),
                    hovertemplate=(
                        f"<b>Component: {selected_component}</b><br>"
                        f"{selected_metric_display_name}: %{{customdata[0]:.2f}}<br>"
                        "Fiscal Year: %{customdata[1]}<br>"
                        "Month: %{customdata[2]}<br>"
                        "<br>--- Component Stats ---<br>"
                        f"Mean ({selected_metric_display_name}): %{{customdata[3]:.2f}}<br>"
                        f"Median ({selected_metric_display_name}): %{{customdata[4]:.2f}}<br>"
                        f"Q1 ({selected_metric_display_name}): %{{customdata[5]:.2f}}<br>"
                        f"Q3 ({selected_metric_display_name}): %{{customdata[6]:.2f}}<br>"
                        f"Min ({selected_metric_display_name}): %{{customdata[7]:.2f}}<br>"
                        f"Max ({selected_metric_display_name}): %{{customdata[8]:.2f}}<br>"
                        "<extra></extra>"
                    ),
                    showlegend=False
                )
            )

        st.plotly_chart(fig, use_container_width=True)

        if not outliers_df.empty:
            with st.expander("🔴 View Outliers"):
                st.dataframe(
                    outliers_df[
                        ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'RM_ROLLUP_PRODUCT_DESC', 'SHIP_CD', 'M0_AND_M1', primary_metric_col]
                    ]
                )
                csv = outliers_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📂 Download Outlier Data",
                    csv,
                    file_name=f"outliers_{selected_product}_{selected_component}.csv"
                )


        else:
            st.info("No significant outliers detected.")
        st.markdown("---")
        if st.button(
            f"Want More Insights? Explore Product-Centric Analysis for {selected_product}",
            key="go_to_product_from_outlier"
        ):
            st.session_state.selected_product_jump = selected_product
            st.session_state.selected_component_jump= selected_component
            
            st.session_state.initial_flow_choice = "Product-Centric"
            st.rerun()


    # ---------------- SHIP OUTLIERS ----------------
    elif analysis_type == "Ship Outliers (across Products)":
        st.subheader(f"Ship Outliers for {selected_component} (across Products)")
    
        selected_ship = st.selectbox(
            "Select Ship to Analyze:",
            options=sorted(component_df['SHIP_CD'].dropna().unique().tolist()),
            key="outlier_ship_select"
        )
        if not selected_ship:
            return
    
        df_filtered = component_df[component_df['SHIP_CD'] == selected_ship].copy()
        if df_filtered.empty:
            st.info("No rows for the selected ship under current filters.")
            return
    
        df_filtered = (
            df_filtered
            .groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 'RM_ROLLUP_PRODUCT_DESC', 'M0_AND_M1'])[
                ['COMPONENT_AMOUNT', 'NEW_PRTD_CAPS_DAYS', 'NEW_PRTD_PAX_DAYS']
            ]
            .sum()
            .reset_index()
        )
    
        df_filtered['MONTHLY_PCD'] = df_filtered['COMPONENT_AMOUNT'] / df_filtered['NEW_PRTD_CAPS_DAYS']
        df_filtered['MONTHLY_PPD'] = df_filtered['COMPONENT_AMOUNT'] / df_filtered['NEW_PRTD_PAX_DAYS']
    
        grouped_data = df_filtered.copy()
    
        # Select the correct metric column
        if selected_metric_display_name == "Margin $":
            primary_metric_col = 'COMPONENT_AMOUNT'
        elif selected_metric_display_name == "Per Capacity Day (Margin PCD)":
            primary_metric_col = 'MONTHLY_PCD'
        elif selected_metric_display_name == "Per Passenger Day (Margin PPD)":
            primary_metric_col = 'MONTHLY_PPD'
        elif selected_metric_display_name == "Passenger Days":
            primary_metric_col = 'NEW_PRTD_PAX_DAYS'
        elif selected_metric_display_name == "Capacity Days":
            primary_metric_col = 'NEW_PRTD_CAPS_DAYS'
        else:
            st.error("Unsupported metric selected for trend plot.")
            return
    
        grouped_data['PRODUCT_LABEL'] = grouped_data['RM_ROLLUP_PRODUCT_DESC']
    
        # Compute per-product stats for the selected ship
        product_stats = (
            grouped_data.groupby('PRODUCT_LABEL')[primary_metric_col]
            .agg(
                fleet_mean='mean',
                fleet_median='median',
                fleet_q1=lambda x: x.quantile(0.25),
                fleet_q3=lambda x: x.quantile(0.75),
                fleet_min='min',
                fleet_max='max'
            )
            .reset_index()
        )
    
        # Merge back into grouped_data
        grouped_data = grouped_data.merge(product_stats, on='PRODUCT_LABEL', how='left')
    
        # Normalize & detect outliers
        grouped_data['NORMALIZED_METRIC'] = grouped_data.groupby('PRODUCT_LABEL')[primary_metric_col].transform(zscore_series)
        grouped_data['IS_OUTLIER'] = grouped_data.groupby('PRODUCT_LABEL')[primary_metric_col].transform(iqr_flags)
    
        # Build row-specific hover data
        grouped_data['customdata'] = grouped_data.apply(
            lambda row: [
                row[primary_metric_col],
                row['FISCAL_YEAR'],
                row['ACCOUNTING_PERIOD'],
                row['fleet_mean'], row['fleet_median'],
                row['fleet_q1'], row['fleet_q3'],
                row['fleet_min'], row['fleet_max']
            ],
            axis=1
        )
    
        fig = go.Figure()
    
        fig.add_trace(go.Box(
            y=grouped_data['NORMALIZED_METRIC'],
            x=grouped_data['PRODUCT_LABEL'],
            boxpoints='all',
            jitter=0.5,
            pointpos=-1.8,
            marker=dict(color='aqua', size=6, opacity=0.6),
            line=dict(color='darkcyan'),
            name='',
            customdata=grouped_data['customdata'].tolist(),
            hovertemplate=(
                f"<b>Component: {selected_component}</b><br>"
                f"{selected_metric_display_name}: %{{customdata[0]:.2f}}<br>"
                "Fiscal Year: %{customdata[1]}<br>"
                "Month: %{customdata[2]}<br>"
                "<br>--- Product Stats ---<br>"
                f"Mean: %{{customdata[3]:.2f}}<br>"
                f"Median: %{{customdata[4]:.2f}}<br>"
                f"Q1: %{{customdata[5]:.2f}}<br>"
                f"Q3: %{{customdata[6]:.2f}}<br>"
                f"Min: %{{customdata[7]:.2f}}<br>"
                f"Max: %{{customdata[8]:.2f}}<br>"
                "<extra></extra>"
            )
        ))
    
        # Red outliers
        outliers_df = grouped_data[grouped_data['IS_OUTLIER']].copy()
        outliers_df['SHIP_CD'] = selected_ship
        outliers_df['M0_AND_M1'] = selected_component
    
        if not outliers_df.empty:
            outliers_df['customdata'] = outliers_df.apply(
                lambda row: [
                    row[primary_metric_col],
                    row['FISCAL_YEAR'],
                    row['ACCOUNTING_PERIOD'],
                    row['fleet_mean'], row['fleet_median'],
                    row['fleet_q1'], row['fleet_q3'],
                    row['fleet_min'], row['fleet_max']
                ],
                axis=1
            )
    
            fig.add_trace(
                go.Scatter(
                    x=outliers_df['PRODUCT_LABEL'],
                    y=outliers_df['NORMALIZED_METRIC'],
                    mode='markers',
                    marker=dict(color='red', size=10, symbol='circle'),
                    name='IQR Outlier',
                    customdata=outliers_df['customdata'].tolist(),
                    hovertemplate=(
                        f"<b>Component: {selected_component}</b><br>"
                        f"{selected_metric_display_name}: %{{customdata[0]:.2f}}<br>"
                        "Fiscal Year: %{customdata[1]}<br>"
                        "Month: %{customdata[2]}<br>"
                        "<br>--- Product Stats ---<br>"
                        f"Mean: %{{customdata[3]:.2f}}<br>"
                        f"Median: %{{customdata[4]:.2f}}<br>"
                        f"Q1: %{{customdata[5]:.2f}}<br>"
                        f"Q3: %{{customdata[6]:.2f}}<br>"
                        f"Min: %{{customdata[7]:.2f}}<br>"
                        f"Max: %{{customdata[8]:.2f}}<br>"
                        "<extra></extra>"
                    ),
                    showlegend=False
                )
            )
    
        fig.update_layout(
            title=f'{selected_metric_display_name} for {selected_ship} by Product',
            xaxis_title='Product',
            yaxis_title='Normalized Metric (Z-score)',
            showlegend=False
        )
    
        st.plotly_chart(fig, use_container_width=True)
    
        if not outliers_df.empty:
            with st.expander("🔴 View Outliers"):
                st.dataframe(
                    outliers_df[
                        ['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'RM_ROLLUP_PRODUCT_DESC', 'SHIP_CD', 'M0_AND_M1', primary_metric_col]
                    ]
                )
                csv = outliers_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📂 Download Outlier Data",
                    csv,
                    file_name=f"outliers_{selected_ship}_{selected_component}.csv"
                )
        else:
            st.info("No significant outliers detected.")
    
        st.markdown("---")
        if st.button(
            f"Want More Insights? Explore Ship-Centric for {selected_ship}",
            key="go_to_ship_from_outlier"
        ):
            st.session_state.selected_ship_jump= selected_ship
            st.session_state.selected_component_jump= selected_component
            st.session_state.initial_flow_choice = "Ship-Centric"
            st.rerun()


# ---------------------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------------
# General Overview Functions
#---------------------------------------------------------------------------------------------------------------------------------------------------
# 1--- KPI Display Styling (general overview) ---
def display_kpis(df_filtered):
    global all_insights
    # --- KPI Calculations ---
    if not df_filtered.empty:
        voyage_count = df_filtered['VOYAGE_ID'].nunique()
        ship_count = df_filtered['SHIP_CD'].nunique()
        
        # Ensure that NEW_PRTD_PAX_DAYS and NEW_PRTD_CAPS_DAYS are aggregated correctly
        # Summing the unique first values per (FISCAL_YEAR, ACCOUNTING_PERIOD, VOYAGE_ID)
        grouped_df_kpi = df_filtered.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'VOYAGE_ID']).agg(
            unique_pax_days=('NEW_PRTD_PAX_DAYS', 'first'),
            unique_caps_days=('NEW_PRTD_CAPS_DAYS', 'first')
        ).reset_index()

        total_pax_days = grouped_df_kpi['unique_pax_days'].sum()
        total_caps_days = grouped_df_kpi['unique_caps_days'].sum()
        
        Load_Factor = (total_pax_days / total_caps_days) * 100 if total_caps_days > 0 else 0

        # M0/M1 and Margin calculations
        m0_amount = df_filtered[df_filtered['M0_AND_M1'].isin(all_Revenue_components)]['COMPONENT_AMOUNT'].sum()
        m1_amount = df_filtered[~df_filtered['M0_AND_M1'].isin(all_Revenue_components)]['COMPONENT_AMOUNT'].sum()
        Margin= m0_amount + m1_amount # Assuming Margin is total Margin across all types as per your structure
                                        # Or if Margin is specifically (Revenue - Cost):
                                        # Margin = df_filtered['REVENUE'].sum() - df_filtered['COST'].sum()

        amn = df_filtered['COMPONENT_AMOUNT'].sum()
        pcd = amn / total_caps_days if total_caps_days > 0 else 0
        ppd = amn / total_pax_days if total_pax_days > 0 else 0
    else:
        voyage_count = ship_count = total_pax_days = total_caps_days = m0_amount = m1_amount = Load_Factor = Margin= pcd = ppd = 0

    # --- Styling for KPIs with uniform length, white text, and spacing ---
    kpi_style = """
        <div style="
            background-color: #003366;
            border-radius: 12px;
            padding: 5px;
            text-align: center;
            box-shadow: 0 44px 8px rgba(0,0,0,0.3);
            font-size: 18px;
            font-weight: 500;
            color: white;
            margin-right: 20px;
            margin-bottom: 20px;
            min-width: 90px;
        ">
            <div style="font-size: 28px; margin-bottom: 8px;">{value}</div>
            <div style="font-size: 16px;">{label}</div>
        </div>
    """
    def format_thousands(number):
        return f"{number / 1000:.1f}K" if abs(number) >= 1000 else f"{number:,.0f}"

    st.subheader("📊 Key Performance Indicators")
    col1, col2 = st.columns(2)
    col1.markdown(kpi_style.format(value=f"{total_pax_days:,.0f} / {total_caps_days:,.0f}",
                                 label="🧑‍🤝‍🧑 Pax Days / ⚓ Capacity Days"), unsafe_allow_html=True)
    col2.markdown(kpi_style.format(value=f"${format_thousands(m0_amount)} : (${format_thousands(m1_amount)})",
                                    label="💰 M0: (M1) "), unsafe_allow_html=True)
    
    col3, col4 = st.columns(2)
    col3.markdown(kpi_style.format(value=f"{Load_Factor:,.2f} %",
                                 label="⚓ Load Factor"), unsafe_allow_html=True)
    col4.markdown(kpi_style.format(value=f"${format_thousands(Margin)}",
                                    label="💰 Margin"), unsafe_allow_html=True)

    col5, col6 = st.columns(2)
    col5.markdown(kpi_style.format(value=f"{voyage_count:,}", label="🛳️ Voyages"), unsafe_allow_html=True)
    col6.markdown(kpi_style.format(value=f"{ship_count:,}", label="🚢 Ships"), unsafe_allow_html=True)

    col7, col8 = st.columns(2)
    col7.markdown(kpi_style.format(value=f"{pcd:,.2f}", label="PCD"), unsafe_allow_html=True)
    col8.markdown(kpi_style.format(value=f"{ppd:,.2f}", label="PPD"), unsafe_allow_html=True)
    
    add_insight(     st.session_state["all_insights"],     title= "Key Performance Indicators", text = f"Voyages: {voyage_count}, Ships: {ship_count}, Pax Days: {total_pax_days:,.0f}, Capacity Days: {total_caps_days:,.0f}, Load Factor: {Load_Factor:,.2f}%, M0: ${m0_amount:,.2f}, M1: (${m1_amount:,.2f}), Margin: ${Margin:,.2f}, PCD: {pcd:,.2f}, PPD: {ppd:,.2f}")

# 2.General Overview Charts - product ranking
def plot_product_ranking(df, metric_col, selected_metric_display_name, order_column=None, order_type=None):
    """
    Plots product ranking by a selected metric, with an optional secondary metric for sorting.
    """
    global all_insights
    st.subheader(f"📊 Product Ranking by {selected_metric_display_name}")

    # --- Handle None for order_column and order_type ---
    use_custom_order = (order_column is not None) and (order_type is not None)
    
    # Map the order_column name to the actual DataFrame column name
    if use_custom_order:
        order_column = order_column.upper()
        if order_column in ['CII', 'CII_SCORE']:
            order_col_for_agg = 'CII_SCORE'
        elif order_column in ['GSS']:
            order_col_for_agg = 'GSS'
        elif order_column in ['LOAD FACTOR', 'LF']:
            order_col_for_agg = 'PAX_DAYS' # Use PAX_DAYS for the complex LF calculation
        order_column_avg_name = f"{order_column}_Average"
        st.write(f"Sorting by: '{order_column}' in '{order_type}' order")
    else:
        order_column_avg_name = None
        st.write(f"Order column or type not specified. Defaulting to sorting by '{selected_metric_display_name}' in descending order.")

    # --- Aggregation Logic for Primary Metric ---
    if selected_metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = df.groupby(['RM_ROLLUP_PRODUCT_DESC'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['RM_ROLLUP_PRODUCT_DESC'], as_index=False)['t1'].sum()
        product_ranking_df = df1.merge(df2, on='RM_ROLLUP_PRODUCT_DESC', how='inner')
        product_ranking_df['Total_Metric'] = product_ranking_df['COMPONENT_AMOUNT'] / product_ranking_df['t1']
    elif selected_metric_display_name =='Passenger Days':
        df21 = df.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_PAX_DAYS'].first().reset_index(name='t1')
        
        product_ranking_df = df21.groupby('RM_ROLLUP_PRODUCT_DESC').agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()
    elif selected_metric_display_name =='Capacity Days':
        df22 = df.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_CAPS_DAYS'].first().reset_index(name='t1')
        
        product_ranking_df = df22.groupby('RM_ROLLUP_PRODUCT_DESC').agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()

    else:
        product_ranking_df = df.groupby('RM_ROLLUP_PRODUCT_DESC').agg(
            Total_Metric=(metric_col, 'sum')
        ).reset_index()

    # --- Aggregation Logic for Order Column ---
    if use_custom_order:
        if order_column in ['GSS', 'CII']:
            order_col_for_agg = 'CII_SCORE' if order_column == 'CII' else 'GSS'
            temp_df = df.groupby(['VOYAGE_ID', 'RM_ROLLUP_PRODUCT_DESC'], as_index=False)[order_col_for_agg].first()
            order_col_avg_df = temp_df.groupby('RM_ROLLUP_PRODUCT_DESC', as_index=False)[order_col_for_agg].mean().rename(columns={order_col_for_agg: order_column_avg_name})
            product_ranking_df = product_ranking_df.merge(order_col_avg_df, on='RM_ROLLUP_PRODUCT_DESC', how='left')
        elif order_column in ['LOAD FACTOR', 'LF']:
            temp_df = df.groupby(['VOYAGE_ID', 'RM_ROLLUP_PRODUCT_DESC'], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
            temp_df = temp_df.groupby('RM_ROLLUP_PRODUCT_DESC', as_index=False).sum()
            temp_df[order_column_avg_name] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
            product_ranking_df = product_ranking_df.merge(temp_df[['RM_ROLLUP_PRODUCT_DESC', order_column_avg_name]], on='RM_ROLLUP_PRODUCT_DESC', how='left')

    # --- Sorting Logic ---
    if use_custom_order and order_column_avg_name in product_ranking_df.columns:
        is_ascending = (order_type.lower() == 'asc')
        product_ranking_df = product_ranking_df.sort_values(by=order_column_avg_name, ascending=is_ascending)
        sorting_info_text = f"by average '{order_column}' in {order_type} order."
    else:
        product_ranking_df = product_ranking_df.sort_values(by='Total_Metric', ascending=False)
        sorting_info_text = f"by '{selected_metric_display_name}' in descending order (default)."

    if not product_ranking_df.empty:
        # --- Plotting with dual axis ---
        if use_custom_order and order_column_avg_name in product_ranking_df.columns:
            # Create formatted strings for the tooltip
            if order_column in ['LOAD FACTOR', 'LF']:
                product_ranking_df['hover_text_line'] = f"{order_column}: " + (product_ranking_df[order_column_avg_name] * 100).round(2).astype(str) + '%'
                yaxis2_title = f"{order_column}"
            elif order_column in ['GSS', 'CII']:
                product_ranking_df['hover_text_line'] = f"{order_column} (Average): " + product_ranking_df[order_column_avg_name].round(2).astype(str)
                yaxis2_title = f"{order_column} (Average)"
            else:
                product_ranking_df['hover_text_line'] = f"{order_column_avg_name}: " + product_ranking_df[order_column_avg_name].round(2).astype(str)
                yaxis2_title = f"{order_column_avg_name}"
            
            product_ranking_df['Total_Metric_Formatted'] = product_ranking_df['Total_Metric'].round(2).astype(str)
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            palette = qualitative.Plotly
            num_colors = len(palette)
            bar_colors = [palette[i % num_colors] for i in range(len(product_ranking_df))]
            
            # Add bar trace
            fig.add_trace(
                go.Bar(
                    x=product_ranking_df['RM_ROLLUP_PRODUCT_DESC'],
                    y=product_ranking_df['Total_Metric'],
                    name=f"{selected_metric_display_name}",
                    hovertemplate=f"<b>Ship:</b> %{{x}}<br><b>{selected_metric_display_name}:</b> %{{y:,.2f}}<extra></extra>",
                    marker=dict(color=bar_colors),
                    customdata=product_ranking_df[['Total_Metric_Formatted', 'hover_text_line']]
                ),
                secondary_y=False,
            )
            # Add line trace for the order column
            fig.add_trace(
                go.Scatter(
                    x=product_ranking_df['RM_ROLLUP_PRODUCT_DESC'],
                    y=product_ranking_df[order_column_avg_name],
                    mode='lines+markers',
                    name=f"{yaxis2_title}",
                    line=dict(color='red', width=2),
                    marker=dict(size=8, color='red'),
                    hovertemplate=f"</b> %{{customdata[1]}}<extra></extra>",
                    customdata=product_ranking_df[['Total_Metric_Formatted', 'hover_text_line']]
                ),
                secondary_y=True,
            )
            # Update layout
            fig.update_layout(
                title_text=f"Product Ranking by {selected_metric_display_name}",
                xaxis_title="Product",
                yaxis_title=selected_metric_display_name,
                yaxis2_title=yaxis2_title,
                yaxis2=dict(overlaying='y', side='right'),
                legend=dict(x=1.05, y=1, xanchor='left', yanchor='top'),
                barmode='group',
                hovermode="x unified"
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True, key="product_ranking_general")

        else:
            # Fallback to single-axis bar chart
            fig = px.bar(
                product_ranking_df, x='RM_ROLLUP_PRODUCT_DESC', y='Total_Metric',
                title=f"Product Ranking by {selected_metric_display_name}",
                labels={'RM_ROLLUP_PRODUCT_DESC': 'Product', 'Total_Metric': selected_metric_display_name},
                color='RM_ROLLUP_PRODUCT_DESC',
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True, key="product_ranking_general")

        # Generate insights
        if len(product_ranking_df) > 0:
            top_product = product_ranking_df.iloc[0]
            insight_text = f"The top performing product is **{top_product['RM_ROLLUP_PRODUCT_DESC']}** with a total {selected_metric_display_name} of **{top_product['Total_Metric']:,.2f}**."
            if use_custom_order:
                insight_text += f"\n\n*The ranking is based on sorting by average '{order_column}' in {order_type} order.*"
            else:
                insight_text += f"\n\n*The ranking is based on sorting by '{selected_metric_display_name}' in descending order (default).*"

            st.markdown(insight_text)
            add_insight(     st.session_state["all_insights"],     title= "Product Ranking", text = insight_text, chart = fig)
    else:
        st.info("No data available for product ranking.")
        add_insight(     st.session_state["all_insights"],     title= "Product Ranking", text = "No data available.")

# 3.General Overview Charts - ship class ranking
def plot_shipclass_ranking(df, metric_col, selected_metric_display_name, order_column=None, order_type=None):

    global all_insights
    st.subheader(f"🚢 Ship Class Ranking by {selected_metric_display_name}")

    # --- Handle None for order_column and order_type ---
    use_custom_order = (order_column is not None) and (order_type is not None)
    
    # Map the order_column name to the actual DataFrame column name
    if use_custom_order:
        order_column = order_column.upper()
        if order_column in ['CII', 'CII_SCORE']:
            order_col_for_agg = 'CII_SCORE'
        elif order_column in ['GSS']:
            order_col_for_agg = 'GSS'
        elif order_column in ['LOAD FACTOR', 'LF']:
            order_col_for_agg = 'PAX_DAYS' # Use PAX_DAYS for the complex LF calculation
        order_column_avg_name = f"{order_column}_Average"
        st.write(f"Sorting by: '{order_column}' in '{order_type}' order")
    else:
        order_column_avg_name = None
        st.write(f"Order column or type not specified. Defaulting to sorting by '{selected_metric_display_name}' in descending order.")

    # --- Aggregation Logic for Primary Metric ---
    if selected_metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = df.groupby(['SHIP_CLASS'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['SHIP_CLASS'], as_index=False)['t1'].sum()
        ship_class_ranking_df = df1.merge(df2, on='SHIP_CLASS', how='inner')
        ship_class_ranking_df['Total_Metric'] = ship_class_ranking_df['COMPONENT_AMOUNT'] / ship_class_ranking_df['t1'] 

    elif selected_metric_display_name =='Passenger Days':
        df21 = df.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_PAX_DAYS'].first().reset_index(name='t1')
        
        ship_class_ranking_df= df21.groupby('SHIP_CLASS').agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()

    elif selected_metric_display_name =='Capacity Days':
        df22 = df.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_CAPS_DAYS'].first().reset_index(name='t1')
        
        ship_class_ranking_df = df22.groupby('SHIP_CLASS').agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()

    else:
        ship_class_ranking_df = df.groupby('SHIP_CLASS').agg(
            Total_Metric=(metric_col, 'sum')
        ).reset_index()

    # --- Aggregation Logic for Order Column ---
    if use_custom_order:
        if order_column in ['GSS', 'CII']:
            order_col_for_agg = 'CII_SCORE' if order_column == 'CII' else 'GSS'
            temp_df = df.groupby(['VOYAGE_ID', 'SHIP_CLASS'], as_index=False)[order_col_for_agg].first()
            order_col_avg_df = temp_df.groupby('SHIP_CLASS', as_index=False)[order_col_for_agg].mean().rename(columns={order_col_for_agg: order_column_avg_name})
            ship_class_ranking_df = ship_class_ranking_df.merge(order_col_avg_df, on='SHIP_CLASS', how='left')
        elif order_column in ['LOAD FACTOR', 'LF']:
            temp_df = df.groupby(['VOYAGE_ID', 'SHIP_CLASS'], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
            temp_df = temp_df.groupby('SHIP_CLASS', as_index=False).sum()
            temp_df[order_column_avg_name] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
            ship_class_ranking_df = ship_class_ranking_df.merge(temp_df[['SHIP_CLASS', order_column_avg_name]], on='SHIP_CLASS', how='left')

    # --- Sorting Logic ---
    if use_custom_order and order_column_avg_name in ship_class_ranking_df.columns:
        is_ascending = (order_type.lower() == 'asc')
        ship_class_ranking_df = ship_class_ranking_df.sort_values(by=order_column_avg_name, ascending=is_ascending)
        sorting_info_text = f"by average '{order_column}' in {order_type} order."
    else:
        ship_class_ranking_df = ship_class_ranking_df.sort_values(by='Total_Metric', ascending=False)
        sorting_info_text = f"by '{selected_metric_display_name}' in descending order (default)."

    if not ship_class_ranking_df.empty:
        # --- Plotting with dual axis ---
        if use_custom_order and order_column_avg_name in ship_class_ranking_df.columns:
            # Create formatted strings for the tooltip
            if order_column in ['LOAD FACTOR', 'LF']:
                ship_class_ranking_df['hover_text_line'] = f"{order_column}: " + (ship_class_ranking_df[order_column_avg_name] * 100).round(2).astype(str) + '%'
                yaxis2_title = f"{order_column}"
            elif order_column in ['GSS', 'CII']:
                ship_class_ranking_df['hover_text_line'] = f"{order_column} (Average): " + ship_class_ranking_df[order_column_avg_name].round(2).astype(str)
                yaxis2_title = f"{order_column} (Average)"
            else:
                ship_class_ranking_df['hover_text_line'] = f"{order_column_avg_name}: " + ship_class_ranking_df[order_column_avg_name].round(2).astype(str)
                yaxis2_title = f"{order_column_avg_name}"
            
            ship_class_ranking_df['Total_Metric_Formatted'] = ship_class_ranking_df['Total_Metric'].round(2).astype(str)
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            palette = (
                qualitative.Plotly
            )
            bar_colors = [palette[i % len(palette)] for i in range(len(ship_class_ranking_df))]
            
            # Add bar trace
            fig.add_trace(
                go.Bar(
                    x=ship_class_ranking_df['SHIP_CLASS'],
                    y=ship_class_ranking_df['Total_Metric'],
                    name=f"{selected_metric_display_name}",
                    hovertemplate=f"<b>Ship:</b> %{{x}}<br><b>{selected_metric_display_name}:</b> %{{y:,.2f}}<extra></extra>",
                    marker=dict(color=bar_colors),
                    customdata=ship_class_ranking_df[['Total_Metric_Formatted', 'hover_text_line']]
                ),
                secondary_y=False,
            )
            # Add line trace for the order column
            fig.add_trace(
                go.Scatter(
                    x=ship_class_ranking_df['SHIP_CLASS'],
                    y=ship_class_ranking_df[order_column_avg_name],
                    mode='lines+markers',
                    name=f"{yaxis2_title}",
                    line=dict(color='red', width=2),
                    marker=dict(size=8, color='red'),
                    hovertemplate=f"</b> %{{customdata[1]}}<extra></extra>",
                    customdata=ship_class_ranking_df[['Total_Metric_Formatted', 'hover_text_line']]
                ),
                secondary_y=True,
            )
            # Update layout
            fig.update_layout(
                title_text=f"Ship Class Ranking by {selected_metric_display_name}",
                xaxis_title="Ship Class",
                yaxis_title=selected_metric_display_name,
                yaxis2_title=yaxis2_title,
                yaxis2=dict(overlaying='y', side='right'),
                legend=dict(x=1.05, y=1, xanchor='left', yanchor='top'),
                barmode='group',
                hovermode="x unified"
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True, key="shipclass_ranking_general")
        
        else:
            # Fallback to single-axis bar chart
            fig = px.bar(
                ship_class_ranking_df, x='SHIP_CLASS', y='Total_Metric',
                title=f"Ship Class Ranking by {selected_metric_display_name}",
                labels={'SHIP_CLASS': 'Ship Class', 'Total_Metric': selected_metric_display_name},
                color='SHIP_CLASS',
                color_discrete_sequence=px.colors.qualitative.Plotly

            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True, key="shipclass_ranking_general")

        # Generate insights
        if len(ship_class_ranking_df) > 0:
            top_ship = ship_class_ranking_df.iloc[0]
            insight_text = f"The top performing ship class is **{top_ship['SHIP_CLASS']}** with a total {selected_metric_display_name} of **{top_ship['Total_Metric']:,.2f}**."
            if use_custom_order:
                insight_text += f"\n\n*The ranking is based on sorting by average '{order_column}' in {order_type} order.*"
            else:
                insight_text += f"\n\n*The ranking is based on sorting by '{selected_metric_display_name}' in descending order (default).*"
            
            st.markdown(insight_text)
            add_insight(     st.session_state["all_insights"],     title= "Ship class Ranking", text = insight_text, chart = fig)
    else:
        st.info("No data available for ship class ranking.")
        add_insight(     st.session_state["all_insights"],     title= "Ship class Ranking", text = "No data available.")

# 4.General Overview Charts - ship ranking

def plot_ship_ranking(df, metric_col, selected_metric_display_name, order_column=None, order_type=None):
    """
    Plots ship ranking by a selected metric, with an optional secondary metric for sorting.
    """
    global all_insights
    st.subheader(f"🚢 Ship Ranking by {selected_metric_display_name}")

    # --- Handle None for order_column and order_type ---
    use_custom_order = (order_column is not None) and (order_type is not None)
    
    # Map the order_column name to the actual DataFrame column name
    if use_custom_order:
        order_column = order_column.upper()
        if order_column in ['CII', 'CII_SCORE']:
            order_col_for_agg = 'CII_SCORE'
        elif order_column in ['GSS']:
            order_col_for_agg = 'GSS'
        elif order_column in ['LOAD FACTOR', 'LF']:
            order_col_for_agg = 'PAX_DAYS' # Use PAX_DAYS for the complex LF calculation
        order_column_avg_name = f"{order_column}_Average"
        st.write(f"Sorting by: '{order_column}' in '{order_type}' order.")
    else:
        order_column_avg_name = None
        st.write(f"Order column or type not specified. Defaulting to sorting by '{selected_metric_display_name}' in descending order.")
    
    # --- Aggregation Logic for Primary Metric ---
    if selected_metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = df.groupby(['SHIP_CD'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID'])[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['SHIP_CD'], as_index=False)['t1'].sum()
        ship_ranking_df = df1.merge(df2, on='SHIP_CD', how='inner')
        ship_ranking_df['Total_Metric'] = ship_ranking_df['COMPONENT_AMOUNT'] / ship_ranking_df['t1']
    elif selected_metric_display_name =='Passenger Days':
        df21 = df.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_PAX_DAYS'].first().reset_index(name='t1')
        
        ship_ranking_df= df21.groupby('SHIP_CD').agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()

    elif selected_metric_display_name =='Capacity Days':
        df22 = df.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_CAPS_DAYS'].first().reset_index(name='t1')
        
        ship_ranking_df = df22.groupby('SHIP_CD').agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()

    else:
        ship_ranking_df = df.groupby('SHIP_CD').agg(
            Total_Metric=(metric_col, 'sum')
        ).reset_index()

    # --- Aggregation Logic for Order Column ---
    if use_custom_order:
        if order_column in ['GSS', 'CII']:
            order_col_for_agg = 'CII_SCORE' if order_column == 'CII' else 'GSS'
            temp_df = df.groupby(['VOYAGE_ID', 'SHIP_CD'], as_index=False)[order_col_for_agg].first()
            order_col_avg_df = temp_df.groupby('SHIP_CD', as_index=False)[order_col_for_agg].mean().rename(columns={order_col_for_agg: order_column_avg_name})
            ship_ranking_df = ship_ranking_df.merge(order_col_avg_df, on='SHIP_CD', how='left')
        elif order_column in ['LOAD FACTOR', 'LF']:
            temp_df = df.groupby(['VOYAGE_ID', 'SHIP_CD'], as_index=False)[['PAX_DAYS', 'DO_CAP_DAYS']].first()
            temp_df = temp_df.groupby('SHIP_CD', as_index=False).sum()
            temp_df[order_column_avg_name] = temp_df['PAX_DAYS'] / temp_df['DO_CAP_DAYS']
            ship_ranking_df = ship_ranking_df.merge(temp_df[['SHIP_CD', order_column_avg_name]], on='SHIP_CD', how='left')

    # --- Sorting Logic ---
    if use_custom_order and order_column_avg_name in ship_ranking_df.columns:
        is_ascending = (order_type.lower() == 'asc')
        ship_ranking_df = ship_ranking_df.sort_values(by=order_column_avg_name, ascending=is_ascending)
        sorting_info_text = f"by average '{order_column}' in {order_type} order."
    else:
        ship_ranking_df = ship_ranking_df.sort_values(by='Total_Metric', ascending=False)
        sorting_info_text = f"by '{selected_metric_display_name}' in descending order (default)."

    if not ship_ranking_df.empty:
        # --- Plotting with dual axis ---
        if use_custom_order and order_column_avg_name in ship_ranking_df.columns:
            # Create formatted strings for the tooltip
            if order_column in ['LOAD FACTOR', 'LF']:
                ship_ranking_df['hover_text_line'] = f"{order_column}: " + (ship_ranking_df[order_column_avg_name] * 100).round(2).astype(str) + '%'
                yaxis2_title = f"{order_column}"
            elif order_column in ['GSS', 'CII']:
                ship_ranking_df['hover_text_line'] = f"{order_column} (Average): " + ship_ranking_df[order_column_avg_name].round(2).astype(str)
                yaxis2_title = f"{order_column} (Average)"
            else:
                ship_ranking_df['hover_text_line'] = f"{order_column_avg_name}: " + ship_ranking_df[order_column_avg_name].round(2).astype(str)
                yaxis2_title = f"{order_column_avg_name}"
            
            ship_ranking_df['Total_Metric_Formatted'] = ship_ranking_df['Total_Metric'].round(2).astype(str)


            # Combine multiple palettes to get 50+ unique colors
            palette = (
                qualitative.Plotly
            )
            bar_colors = [palette[i % len(palette)] for i in range(len(ship_ranking_df))]

            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add bar trace
            fig.add_trace(
                go.Bar(
                    x=ship_ranking_df['SHIP_CD'],
                    y=ship_ranking_df['Total_Metric'],
                    name=f"{selected_metric_display_name}",
                    hovertemplate=f"<b>Ship:</b> %{{x}}<br><b>{selected_metric_display_name}:</b> %{{y:,.2f}}<extra></extra>",
                    marker=dict(color=bar_colors),
                    customdata=ship_ranking_df[['Total_Metric_Formatted', 'hover_text_line']]
                ),
                secondary_y=False,
            )
            # Add line trace for the order column
            fig.add_trace(
                go.Scatter(
                    x=ship_ranking_df['SHIP_CD'],
                    y=ship_ranking_df[order_column_avg_name],
                    mode='lines+markers',
                    name=f"{yaxis2_title}",
                    line=dict(color='red', width=2),
                    marker=dict(size=8, color='red'),
                    hovertemplate=f"</b> %{{customdata[1]}}<extra></extra>",
                    customdata=ship_ranking_df[['Total_Metric_Formatted', 'hover_text_line']]
                ),
                secondary_y=True,
            )
            # Update layout
            fig.update_layout(
                title_text=f"Ship Ranking by {selected_metric_display_name}",
                xaxis_title="Ship",
                yaxis_title=selected_metric_display_name,
                yaxis2_title=yaxis2_title,
                yaxis2=dict(overlaying='y', side='right'),
                legend=dict(x=1.05, y=1, xanchor='left', yanchor='top'),
                barmode='group',
                hovermode="x unified"
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True, key="ship_ranking_general")
        
        else:
            # Fallback to single-axis bar chart
            fig = px.bar(
                ship_ranking_df, x='SHIP_CD', y='Total_Metric',
                title=f"Ship Ranking by {selected_metric_display_name}",
                labels={'SHIP_CD': 'Ship', 'Total_Metric': selected_metric_display_name},
                color='SHIP_CD',
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True, key="ship_ranking_general")

        # Generate insights
        if len(ship_ranking_df) > 0:
            top_ship = ship_ranking_df.iloc[0]
            insight_text = f"The top performing ship is **{top_ship['SHIP_CD']}** with a total {selected_metric_display_name} of **{top_ship['Total_Metric']:,.2f}**."
            if use_custom_order:
                insight_text += f"\n\n*The ranking is based on sorting by average '{order_column}' in {order_type} order.*"
            else:
                insight_text += f"\n\n*The ranking is based on sorting by '{selected_metric_display_name}' in descending order (default).*"
            
            st.markdown(insight_text)
            add_insight(     st.session_state["all_insights"],     title= "Ship Ranking", text = insight_text, chart = fig)
    else:
        st.info("No data available for ship ranking.")
        add_insight(     st.session_state["all_insights"],     title= "Ship Ranking", text = "No data available.")
#5. --- Trend Over Years Chart ---
def display_trend_chart(filtered_df, primary_metric_col, selected_metric_display_name, key=None):
    global all_insights

    metric_col = metric_display_to_col.get(selected_metric_display_name)
    if not metric_col:
        st.warning(f"Trend chart: Metric column for '{selected_metric_display_name}' not found.")
        return

    df_temp = filtered_df.copy()
    df_temp = df_temp[(df_temp["FISCAL_YEAR"] != 0) & (df_temp["ACCOUNTING_PERIOD"] != 0)].copy()
    df_temp["FISCAL_YEAR"] = df_temp["FISCAL_YEAR"].astype(str)

    if selected_metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        # Step 1: Sum COMPONENT_AMOUNT
        df1 = df_temp.groupby(["FISCAL_YEAR", "ACCOUNTING_PERIOD"], as_index=False)["COMPONENT_AMOUNT"].sum()

        # Step 2: First denominator value by voyage
        df2 = df_temp.groupby(
            ["FISCAL_YEAR", "ACCOUNTING_PERIOD", "SHIP_CD", "RM_ROLLUP_PRODUCT_DESC", "VOYAGE_ID"]
        )[primary_metric_col].first().reset_index(name="t1")

        # Step 3: Sum denominators by fiscal period
        df2 = df2.groupby(["FISCAL_YEAR", "ACCOUNTING_PERIOD"], as_index=False)["t1"].sum()

        # Step 4: Merge numerator and denominator
        trend_df = df1.merge(df2, on=["FISCAL_YEAR", "ACCOUNTING_PERIOD"], how="inner")
        trend_df["Total_Metric"] = trend_df["COMPONENT_AMOUNT"] / trend_df["t1"]
        
    elif selected_metric_display_name =='Passenger Days':
        df21 = df_temp.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_PAX_DAYS'].first().reset_index(name='t1')
        
        trend_df = df21.groupby(["FISCAL_YEAR", "ACCOUNTING_PERIOD"]).agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()

    elif selected_metric_display_name =='Capacity Days':
        df22 = df_temp.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CLASS','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']
        )['NEW_PRTD_CAPS_DAYS'].first().reset_index(name='t1')
        
        trend_df = df22.groupby(["FISCAL_YEAR", "ACCOUNTING_PERIOD"]).agg(
            Total_Metric=('t1', 'sum')
        ).reset_index()
        
    else:
        trend_df = df_temp.groupby(["FISCAL_YEAR", "ACCOUNTING_PERIOD"], as_index=False)[metric_col].sum()
        trend_df = trend_df.rename(columns={metric_col: "Total_Metric"})

    # Add sorting key for chronological order
    trend_df["SORT_KEY"] = trend_df["FISCAL_YEAR"] + trend_df["ACCOUNTING_PERIOD"].astype(str).str.zfill(2)
    trend_df = trend_df.sort_values(by="SORT_KEY").drop(columns="SORT_KEY")

    if trend_df.empty:
        st.warning(f"No data available for '{selected_metric_display_name}' to plot trend.")
        return

    # Plot the trend chart
    fig = px.line(
        trend_df,
        x="ACCOUNTING_PERIOD",
        y="Total_Metric",
        color="FISCAL_YEAR",
        markers=True,
        title=f"{selected_metric_display_name} Trend by Accounting Period and Fiscal Year",
        labels={"Total_Metric": selected_metric_display_name, "ACCOUNTING_PERIOD": "Accounting Period"}
    )

    fig.update_layout(
        xaxis_title="Accounting Period",
        yaxis_title=selected_metric_display_name,
        hovermode="x unified"
    )

    st.plotly_chart(fig, use_container_width=True, key=key)


#6. General Overview Charts - component breakdown

def plot_components_breakdown(df, metric_col, selected_metric_display_name):
    global all_insights
    st.subheader(f"📊 Components Breakdown by {selected_metric_display_name}")

    # 🚫 Skip component breakdown for Passenger Days / Capacity Days
    if selected_metric_display_name in ('Passenger Days', 'Capacity Days'):
        st.info("Passenger Days and Capacity Days are identical across all components, so this metric isn’t meaningful for component-level breakdowns.")
        add_insight(     st.session_state["all_insights"],     title= "Components Breakdown",
            text = "Passenger Days and Capacity Days are identical across all components, so this metric isn’t meaningful for component-level breakdowns."
        )
        return

    # ✅ Continue with breakdown for other metrics
    specific_components = ['NTR', 'OBR Accounts', 'Crew', 'Food', 'Fuel', 'OSO', 'Port', 'R&M']
    df_filtered = df[df['M0_AND_M1'].isin(specific_components)]

    if selected_metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = df_filtered.groupby(['M0_AND_M1'], as_index=False)['COMPONENT_AMOUNT'].sum()
        df2 = df_filtered.groupby(
            ['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID','M0_AND_M1']
        )[metric_col].first().reset_index(name='t1')
        df2 = df2.groupby(['M0_AND_M1'], as_index=False)['t1'].sum()
        component_contribution = df1.merge(df2, on=['M0_AND_M1'], how='inner')
        component_contribution['Total_Metric'] = component_contribution['COMPONENT_AMOUNT'] / component_contribution['t1']
    else:
        component_contribution = df_filtered.groupby('M0_AND_M1').agg(
            Total_Metric=('COMPONENT_AMOUNT', 'sum')
        ).reset_index()

    if component_contribution.empty:
        st.info("No data available for specified components breakdown.")
        add_insight(     st.session_state["all_insights"],     title= "Components Breakdown",
            text = "No data available for specified components."
        )
        return

    total_metric_sum = component_contribution['Total_Metric'].sum()
    total_row = pd.DataFrame([{'M0_AND_M1': 'Total', 'Total_Metric': total_metric_sum}])
    final_breakdown_df = pd.concat([component_contribution, total_row], ignore_index=True)

    colors = ['green' if val >= 0 else 'red' for val in final_breakdown_df['Total_Metric']]

    fig = px.bar(
        final_breakdown_df,
        x='M0_AND_M1',
        y='Total_Metric',
        title=f"Components Breakdown by {selected_metric_display_name}",
        labels={'M0_AND_M1': 'Component', 'Total_Metric': selected_metric_display_name},
        color=colors,
        color_discrete_map={'green': 'green', 'red': 'red'}
    )
    fig.update_traces(
        hovertemplate="<b>Component:</b> %{x}<br>"
                      f"<b>{selected_metric_display_name}:</b> %{{y}}<br>"
                      "<extra></extra>"
    )
    fig.update_layout(xaxis_title="Component", yaxis_title=selected_metric_display_name, showlegend=False)
    fig.update_xaxes(tickangle=45)

    st.plotly_chart(fig, use_container_width=True, key="components_breakdown_general")

    components_only_df = final_breakdown_df[final_breakdown_df['M0_AND_M1'] != 'Total']
    if not components_only_df.empty:
        top_component = components_only_df.loc[components_only_df['Total_Metric'].abs().idxmax()]
        insight_text = f"The total {selected_metric_display_name} across the selected components is **{total_metric_sum:,.2f}**. "
        insight_text += f"The component with the largest absolute impact is **{top_component['M0_AND_M1']}** with **{top_component['Total_Metric']:,.2f}**."
    else:
        insight_text = f"The total {selected_metric_display_name} across the selected components is **{total_metric_sum:,.2f}**. No individual component data available for ranking."

    st.markdown(insight_text)
    add_insight(     st.session_state["all_insights"],     title= "Components Breakdown", text = insight_text, chart = fig)

# development 
def process_query_and_navigate(query, df_data):
    st.session_state.current_query = query
    st.session_state.page = 'query_results_page'
    

    corrected_query = correct_query(query)
    query_type_raw = detect_query_type(corrected_query)
    query_type_nlp = detect_query_type(corrected_query).upper() if query_type_raw else "UNKNOWN"
    ships_q, products_q, years_q, months_q, voyages_q, SHIP_CLASSs_q,component = extract_filters_from_query(corrected_query, df_data)

    # Determine initial metric based on NLP
    if query_type_nlp in metric_groups and metric_groups[query_type_nlp]:
        default_metric = metric_groups[query_type_nlp][0]
        default_metric = ("Per Capacity Day (Margin PCD)" if default_metric == "PCD"
                         else "Per Passenger Day (Margin PPD)" if default_metric == "PPD"
                         else "Passenger Days" if query_type_nlp == "PASSENGER DAYS"
                         else "Capacity Days" if query_type_nlp == "CAPACITY DAYS"
                         else default_metric)
        if default_metric in metric_display_to_col:
             st.session_state.selected_primary_metric_sidebar = default_metric
        else:
            st.session_state.selected_primary_metric_sidebar = "Margin $" # Fallback
    else:
        st.session_state.selected_primary_metric_sidebar = "Margin $" # Fallback for unknown query types

    # Determine initial flow based on query keywords
    query_lower = corrected_query.lower()
    if any(p_keyword in query_lower for p_keyword in PRODUCT_KEYWORDS):
        st.session_state.initial_flow_choice = "Product-Centric"
    elif any(s_keyword in query_lower for s_keyword in SHIP_KEYWORDS):
        st.session_state.initial_flow_choice = "Ship-Centric"
    elif any(v_keyword in query_lower for v_keyword in VOYAGE_KEYWORDS):
        st.session_state.initial_flow_choice = "Voyage-Centric"
    elif "outlier" in query_lower: # Direct to Outlier Analysis
        st.session_state.initial_flow_choice = "Outlier Analysis"
    elif any(d_keyword in query_lower for d_keyword in DEPLOYMENT_KEYWORDS):
        st.session_state.initial_flow_choice = "Deployment Analysis"
    else:
        st.session_state.initial_flow_choice = "General Overview"

    # Pre-populate sidebar filters based on query
    st.session_state.current_filters['Year'] = [int(y) for y in years_q if y.isdigit()] if years_q else []
    st.session_state.current_filters['Month'] = [int(m) for m in months_q if m.isdigit()] if months_q else []
    st.session_state.current_filters['RM_ROLLUP_PRODUCT_DESC'] = products_q
    st.session_state.current_filters['Ship Class'] = SHIP_CLASSs_q
    st.session_state.current_filters['Ship'] = ships_q
    st.session_state.current_filters['Voyage'] = voyages_q
    st.session_state.current_filters['M0_AND_M1'] = component

    st.rerun() 
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# --- GUIDED FLOW DISPATCHERS ---
# -----------------------------------------------------------------------------------------------------------------

def product_analysis_flow(df, filtered_df, query, primary_metric_col, selected_metric_display_name,order_column=None,order_type=None):
    st.header("Product-Centric Analysis")

    # 1. Overall Product Performance Plot (NEW - aggregated across all filtered years)
    plot_overall_entity_performance(filtered_df, 'RM_ROLLUP_PRODUCT_DESC', primary_metric_col, selected_metric_display_name, "Product", "product_overall_perf",order_column,order_type)
    
    # 2. Individual bar plots with consistent legend color for each year for products (moved to expander)
    with st.expander("View Product Performance Year-over-Year"):
        plot_entity_performance_comparison(filtered_df, 'RM_ROLLUP_PRODUCT_DESC', primary_metric_col, selected_metric_display_name, "Product", "product_yearly_perf")

    # User selects a product
    unique_products = sorted(filtered_df['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist())
    if unique_products:
        default_product = st.session_state.get("selected_product_jump", 'All')
        
        selected_product = st.selectbox(
        "Select a Product to drill down:",
        options=['All'] + unique_products,
        index=(['All'] + unique_products).index(default_product) if default_product in unique_products or default_product=='All' else 0,
        key="selected_product_drilldown"
        )
        
        if selected_product != 'All':
            st.session_state.selected_product = selected_product # Store for further drill down
            st.markdown(f"---")
            st.subheader(f"Analyzing: **{selected_product}**")


            plot_product_ship_class_summary(filtered_df, selected_product, primary_metric_col, selected_metric_display_name,order_column,order_type)
    
            # User selects a ship class
            product_filtered_ship_class_df = filtered_df[filtered_df['RM_ROLLUP_PRODUCT_DESC'] == selected_product].copy()
            unique_ship_classes = sorted(product_filtered_ship_class_df['SHIP_CLASS'].dropna().unique().tolist())
            if unique_ship_classes:
                selected_ship_class = st.selectbox(
                    "Select a Ship Class to further analyze:",
                    options=['All'] + unique_ship_classes,
                    key="selected_ship_class_drilldown"
                )

                if selected_ship_class != 'All':
                    st.session_state.selected_ship_class = selected_ship_class # Store for further drill down
                    st.markdown(f"---")
                    st.subheader(f"Analyzing: **{selected_product}** > **{selected_ship_class}**")

                    # 4. Show trend to compare ships of same class across each year separate plot for each year
                    plot_ship_class_yearly_comparison(product_filtered_ship_class_df, selected_ship_class, primary_metric_col, selected_metric_display_name,order_column,order_type)

                    # 5. Show component breakdown for each class of ship
                    plot_component_breakdown_by_ship_class(product_filtered_ship_class_df[product_filtered_ship_class_df['SHIP_CLASS'] == selected_ship_class],primary_metric_col, selected_metric_display_name)

                    # User selects a ship
                    ship_class_filtered_ship_df = product_filtered_ship_class_df[product_filtered_ship_class_df['SHIP_CLASS'] == selected_ship_class].copy()
                    unique_ships_in_class = sorted(ship_class_filtered_ship_df['SHIP_CD'].dropna().unique().tolist())
                    
                    if unique_ships_in_class:
                        selected_ship = st.selectbox(
                            "Select a Ship to inspect components:",
                            options=['All'] + unique_ships_in_class,
                            key="selected_ship_drilldown"
                        )
                        
                        if selected_ship != 'All':
                            st.session_state.selected_ship = selected_ship # Store for further drill down
                            st.markdown(f"---")
                            st.subheader(f"Analyzing: **{selected_product}** > **{selected_ship_class}** > **{selected_ship}**")
                            
                            st.markdown("#### 💰/💸 Please select the Component Type (Non - Fuel)") # Added emoji
    
                            type_option = st.radio(
                                "Select Component Type (Cost/Revenue):",
                                options=["Cost", "Revenue"],
                                horizontal=True,
                                key="type_option_toggle_main" # Changed key to avoid conflict if old one was used in sidebar
                            )

                            selected_m0_m1_components = all_Revenue_components if type_option == "Revenue" else Cost_components


                                                        # 6. Components box plot using z-score method
                            outliers_df_detail, selected_m0m1_for_plot2 = plot_component_boxplot_zscore(
                                filtered_df,
                                selected_product,
                                selected_ship,
                                primary_metric_col,
                                selected_metric_display_name,
                                type_option,
                                selected_m0_m1_components
                            )
                            
                            # 🔒 Guarantee M0_AND_M1 column exists
                            if "M0_AND_M1" not in outliers_df_detail.columns:
                                if {"M0", "M1"}.issubset(outliers_df_detail.columns):
                                    outliers_df_detail["M0_AND_M1"] = (
                                        outliers_df_detail["M0"].astype(str) + "_" + outliers_df_detail["M1"].astype(str)
                                    )
                                elif "COMPONENT_DESC" in outliers_df_detail.columns:
                                    outliers_df_detail["M0_AND_M1"] = outliers_df_detail["COMPONENT_DESC"].astype(str)
                                else:
                                    # last-resort fallback
                                    outliers_df_detail["M0_AND_M1"] = "-- Select Component --"
                             # ✅ INSERT THIS BLOCK **RIGHT HERE**
                            # --- Auto-select component if user came from Outlier View ---
                            default_component = st.session_state.get("selected_component_jump", "-- Select Component --")
                            
                            if selected_m0m1_for_plot2 == "-- Select Component --" and default_component != "-- Select Component --":
                                selected_m0m1_for_plot2 = default_component
                                
                            # 🚦 Only filter if user has made a valid selection
                            if selected_m0m1_for_plot2 != "-- Select Component --":
                                outliers_df_detail = outliers_df_detail[outliers_df_detail["M0_AND_M1"] == selected_m0m1_for_plot2]
                            else:
                                st.warning("Please select a valid component to continue.")


                            if not outliers_df_detail.empty and selected_m0m1_for_plot2 != '-- Select Component --':
        
                                outliers_df_detail['SHIP_CD'] = selected_ship
                                
                                # Get unique Year
                                available_year = outliers_df_detail['FISCAL_YEAR'].unique()
                                
                                available_year = ["-- Select Year --"] + sorted(available_year)

                                
                                # 🖱️ User selects the outlier month to explore
                                selected_outlier_year = st.selectbox("Select Outlier Year to Analyze  from 2023-2024", available_year)

                                if selected_outlier_year != "-- Select Year --":
                                
                                    # Filter to selected outlier month only
                                    outliers_df_detail = outliers_df_detail[outliers_df_detail['FISCAL_YEAR'] == selected_outlier_year]
                                    
                                    # 🔽 Get unique months
                                    available_outlier_months = outliers_df_detail['ACCOUNTING_PERIOD'].unique()
                            
                                    # 🧠 Optional: sort months numerically
                                    available_months = ["-- Select Month --"] + sorted(available_outlier_months)
                                    
                                    st.info(f"{len(available_months) - 1} outlier months found for {selected_m0m1_for_plot2} on ship {selected_ship} for the year {selected_outlier_year}")
                                    
                            
                                    # 🖱️ User selects the outlier month to explore
                                    selected_outlier_month = st.selectbox("Select Outlier Month to Analyze from 2023 - 2024", available_months)

                                    st.info("Select an outlier month to continue  from 2023-2024")
                                    
                                    if selected_outlier_year != "-- Select Month --":
                                    # Filter to selected outlier month only
                                        outliers_df_detail = outliers_df_detail[outliers_df_detail['ACCOUNTING_PERIOD'] == selected_outlier_month]                                      

                                        df_accnt = load_accnt_data()

                                        df_accnt['NEW_PRTD_CAPS_DAYS'] = pd.to_numeric(df_accnt['NEW_PRTD_CAPS_DAYS'], errors='coerce')
                                        df_accnt['NEW_PRTD_PAX_DAYS'] = pd.to_numeric(df_accnt['NEW_PRTD_PAX_DAYS'], errors='coerce')
                                        
                                        df_voy = outlier_voyage_analysis(df_accnt, outliers_df_detail, selected_m0m1_for_plot2, selected_ship, selected_metric_display_name, selected_product, available_months, selected_outlier_month, selected_outlier_year)

                                        st.info("No Outlier Voyage (Non Straddle) found")
                                        selected_outlier_option = st.selectbox("Select YES to Analyze on a Month-Account Level from 2023-2024", ['--Select if you want to continue--','YES', 'NO'])
                                        
                                        if selected_outlier_option == "YES":
                                            account_list_1= plot_non_outlier_voyage_mon_lvl_delta_accnts(df_accnt, selected_outlier_year, selected_outlier_month, selected_metric_display_name, selected_m0m1_for_plot2, selected_product, selected_ship, available_outlier_months)
                                            account_list = ['--Select an Account--'] + account_list_1
                                            if account_list_1 and len(account_list_1) > 0:
                                                st.info("Select a delta Account to analyse the trend")
                                                selected_account = st.selectbox("Select a delta Account to Analyze", account_list)
    
                                                if selected_account != '--Select an Account':
    
                                                    selected_account_id = selected_account.split('-')[0]
    
                                                    plot_account_time_series(selected_account, selected_account_id, df_accnt, selected_product, selected_ship, selected_m0m1_for_plot2, type_option, selected_metric_display_name, selected_outlier_year, selected_outlier_month)
                                            
                                        else:
                                            st.warning("Please select YES to continue on Account level drill down")
                                            

                                        #outlier_account_analysis(outliers_df_detail, selected_m0m1_for_plot2, selected_ship, selected_metric_display_name, selected_product, available_months, selected_outlier_month, selected_outlier_year )
                                    else:
                                        st.info("Select an outlier Month to continue from 2023-2024")
                                else:
                                    st.info("Select an outlier Year to continue from 2023-2024")
                            else:
                                st.info("No outlier data available for the selected component/ship .") 
                        else:
                            st.info("Select a ship to inspect its components.")
                    else:
                        st.info("No ships available in this ship class for drilldown.")
                else:
                    st.info("Select a ship class to view detailed ship trends and rankings.")
            else:
                st.info("No ship classes available for this product for drilldown.")
        else:
            st.info("Select a product to drill down into ship class performance.")
    else:
        st.info("No products available in the filtered data for detailed analysis.")


def ship_analysis_flow(df, filtered_df, query, primary_metric_col, selected_metric_display_name,order_column=None,order_type=None):
    st.header("Ship-Centric Analysis")
    

    # 1. Overall Ship Performance Plot (NEW - aggregated across all filtered years)
    plot_overall_entity_performance(filtered_df, 'SHIP_CD', primary_metric_col, selected_metric_display_name, "Ship", "ship_overall_perf",order_column,order_type)

    # 2. Individual bar plots with consistent legend color for each year for ships (moved to expander)
    with st.expander("View Ship Performance Year-over-Year"):
        plot_entity_performance_comparison(filtered_df, 'SHIP_CD', primary_metric_col, selected_metric_display_name, "Ship", "ship_yearly_perf")

    unique_ships = sorted(filtered_df['SHIP_CD'].dropna().unique().tolist())
    if unique_ships:
        default_ships = st.session_state.get("selected_ship_jump", 'All')
        
        selected_ship = st.selectbox(
            "Select a Ship to drill down:",
            options=['All'] + unique_ships,
            index=(['All'] + unique_ships).index(default_ships) if default_ships in unique_ships or default_ships =='All' else 0,
            key="selected_ship_drilldown")
        if selected_ship != 'All':
            st.session_state.selected_ship = selected_ship # Store for further drill down
            st.markdown(f"---")
            st.subheader(f"Analyzing: **{selected_ship}**")
            
            # 3. Plots of product going to that ship across each year
            plot_product_yearly_for_ship(filtered_df, selected_ship, primary_metric_col, selected_metric_display_name,order_column,order_type)
        
            # User selects a product
            ship_filtered_product_df = filtered_df[filtered_df['SHIP_CD'] == selected_ship].copy()
            unique_products_for_ship = sorted(ship_filtered_product_df['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist())
            if unique_products_for_ship:
                selected_product_for_ship_flow = st.selectbox(
                    "Select a Product to further analyze for this ship:",
                    options=['All'] + unique_products_for_ship,
                    key="selected_product_drilldown_ship_flow"
                )

                if selected_product_for_ship_flow != 'All':
                    st.session_state.selected_product_for_ship_flow = selected_product_for_ship_flow
                    st.markdown(f"---")
                    st.subheader(f"Analyzing: **{selected_ship}** > **{selected_product_for_ship_flow}**")

                    # 4. Trend across months of the year for the selected product on this ship
                    plot_product_monthly_trend_for_ship(filtered_df, selected_ship, selected_product_for_ship_flow, primary_metric_col, selected_metric_display_name)

                    # 5. Ranks the components across for this ship and product
                    rank_components_by_ship_product(filtered_df, selected_ship, selected_product_for_ship_flow, primary_metric_col, selected_metric_display_name)
                    
                    type_option = st.radio(
                        "Select Component Type (Cost/Revenue):",
                        options=["Cost", "Revenue"],
                        horizontal=True,
                        key="type_option_toggle_main" # Changed key to avoid conflict if old one was used in sidebar
                    )

                    selected_m0_m1_components = all_Revenue_components if type_option == "Revenue" else Cost_components

                    # 6. Components box plot using z-score method

                    outliers_df_detail, selected_m0m1_for_plot2 = plot_component_boxplot_zscore(filtered_df, selected_product_for_ship_flow, selected_ship, primary_metric_col, selected_metric_display_name, type_option, selected_m0_m1_components)

                    if "M0_AND_M1" not in outliers_df_detail.columns:
                        if {"M0", "M1"}.issubset(outliers_df_detail.columns):
                            outliers_df_detail["M0_AND_M1"] = (
                                outliers_df_detail["M0"].astype(str) + "_" + outliers_df_detail["M1"].astype(str)
                            )
                        elif "COMPONENT_DESC" in outliers_df_detail.columns:
                            outliers_df_detail["M0_AND_M1"] = outliers_df_detail["COMPONENT_DESC"].astype(str)
                        else:
                            # last-resort fallback
                            outliers_df_detail["M0_AND_M1"] = "-- Select Component --"
                    # --- Auto-select component if user came from Outlier View ---
                    default_component = st.session_state.get("selected_component_jump", "-- Select Component --")
                    
                    if selected_m0m1_for_plot2 == "-- Select Component --" and default_component != "-- Select Component --":
                        selected_m0m1_for_plot2 = default_component
                    
                    # 🚦 Only filter if user has made a valid selection
                    if selected_m0m1_for_plot2 != "-- Select Component --":
                        outliers_df_detail = outliers_df_detail[outliers_df_detail["M0_AND_M1"] == selected_m0m1_for_plot2]
                    else:
                        st.warning("Please select a valid component to continue.")

                    if not outliers_df_detail.empty and selected_m0m1_for_plot2 != '-- Select Component --':
        
                        outliers_df_detail['SHIP_CD'] = selected_ship
                        
                        # Get unique Year
                        available_year = outliers_df_detail['FISCAL_YEAR'].unique()
                        
                        available_year = ["-- Select Year --"] + sorted(available_year)

                        
                        # 🖱️ User selects the outlier month to explore
                        selected_outlier_year = st.selectbox("Select Outlier Year to Analyze from 2023-2024", available_year)

                        if selected_outlier_year != "-- Select Year --":
                        
                            # Filter to selected outlier month only
                            outliers_df_detail = outliers_df_detail[outliers_df_detail['FISCAL_YEAR'] == selected_outlier_year]
                            
                            # 🔽 Get unique months
                            available_outlier_months = outliers_df_detail['ACCOUNTING_PERIOD'].unique()
                    
                            # 🧠 Optional: sort months numerically
                            available_months = ["-- Select Month --"] + sorted(available_outlier_months)
                            
                            st.info(f"{len(available_months) - 1} outlier months found for {selected_m0m1_for_plot2} on ship {selected_ship} for the year {selected_outlier_year}")
                            
                    
                            # 🖱️ User selects the outlier month to explore
                            selected_outlier_month = st.selectbox("Select Outlier Month to Analyze from 2023-2024", available_months)

                            st.info("Select an outlier month to continue")
                            
                            if selected_outlier_year != "-- Select Month --":
                            # Filter to selected outlier month only
                                outliers_df_detail = outliers_df_detail[outliers_df_detail['ACCOUNTING_PERIOD'] == selected_outlier_month]

                                df_accnt = load_accnt_data()

                                df_accnt['NEW_PRTD_CAPS_DAYS'] = pd.to_numeric(df_accnt['NEW_PRTD_CAPS_DAYS'], errors='coerce')
                                df_accnt['NEW_PRTD_PAX_DAYS'] = pd.to_numeric(df_accnt['NEW_PRTD_PAX_DAYS'], errors='coerce')
                                
                                df_voy = outlier_voyage_analysis(df_accnt, outliers_df_detail, selected_m0m1_for_plot2, selected_ship, selected_metric_display_name, selected_product_for_ship_flow, available_months, selected_outlier_month, selected_outlier_year)

                                st.info("No Outlier Voyage (Non Straddle) found")
                                selected_outlier_option = st.selectbox("Select YES to Analyze on a Month-Account Level from 2023-2024", ['--Select if you want to continue--','YES', 'NO'])
                                
                                if selected_outlier_option == "YES":
                                    account_list_1 = plot_non_outlier_voyage_mon_lvl_delta_accnts(df_accnt, selected_outlier_year, selected_outlier_month, selected_metric_display_name, selected_m0m1_for_plot2, selected_product_for_ship_flow, selected_ship, available_outlier_months)
                                    account_list = ['--Select an Account--'] + account_list_1

                                    if account_list_1 and len(account_list_1) > 0:
                                        st.info("Select a delta Account to analyse the trend from 2023-2024")
                                        selected_account = st.selectbox("Select a delta Account to Analyze", account_list)
    
                                        if selected_account and selected_account != '--Select an Account':
    
                                            selected_account_id = selected_account.split('-')[0]
    
                                            plot_account_time_series(selected_account, selected_account_id, df_accnt, selected_product_for_ship_flow, selected_ship, selected_m0m1_for_plot2, type_option, selected_metric_display_name, selected_outlier_year, selected_outlier_month)
                                    
                                else:
                                    st.warning("Please select YES to continue on Account level drill down")
                                    

                                #outlier_account_analysis(outliers_df_detail, selected_m0m1_for_plot2, selected_ship, selected_metric_display_name, selected_product, available_months, selected_outlier_month, selected_outlier_year )
                            else:
                                st.info("Select an outlier Month to continue")
                        else:
                            st.info("Select an outlier Year to continue")
                    else:
                        st.info("No outlier data available for the selected component/ship.") 
                else:
                    st.info("Select a product to view detailed component trends and rankings for this ship.")
            else:
                st.info("No products available for this ship for drilldown.")
        else:
            st.info("Select a ship to drill down into product performance.")
    else:
        st.info("No ships available in the filtered data for detailed analysis.")


def voyage_analysis_flow(df, filtered_df, query, primary_metric_col, selected_metric_display_name, order_column=None,order_type=None,all_voyage_ids=None):

    st.header("Voyage-Centric Analysis")
    global all_insights

    selected_voyage_id = None

    # Choose mode
    mode = st.radio("Choose analysis mode:", ["By Voyage ID", "By Itinerary Route"], key="analysis_mode")

    if all_voyage_ids is None:
        all_voyage_ids = sorted(df['VOYAGE_ID'].dropna().unique().tolist())

    if mode == "By Voyage ID":
    
        # --- Extract Voyage IDs from SQL-like query ---
        _, _, _, _, target_voyage_ids_from_query, _, _ = extract_filters_from_query(query, df)
        target_voyage_ids_from_query = [v for v in target_voyage_ids_from_query if v and v != 'None']
    
        # Respect global filters
        filtered_voyages = filtered_df['VOYAGE_ID'].dropna().unique().tolist()
        if filtered_voyages:
            all_voyage_ids = sorted(filtered_voyages)
    
        # Merge query voyages + filtered voyages (no duplicates)
        all_voyage_options = list(dict.fromkeys(
            target_voyage_ids_from_query + [v for v in all_voyage_ids if v not in target_voyage_ids_from_query]
        ))
    
        st.info("💡 For a clearer view, use the Global Filters (Year, Month, Ship) to refine the voyage results.")
    
        # ===========================
        #       START METRICS
        # ===========================
        metric_map = {
            "Margin $": "COMPONENT_AMOUNT",
            "Passenger Days": "NEW_PRTD_PAX_DAYS",
            "Capacity Days": "NEW_PRTD_CAPS_DAYS",
            "Per Capacity Day (Margin PCD)": None,
            "Per Passenger Day (Margin PPD)": None
        }

        if not filtered_df.empty:
        
            # ---------------------------
            # BASIC AGGREGATION
            # ---------------------------
            if selected_metric_display_name in ("Passenger Days", "Capacity Days"):
                metric_col = metric_map[selected_metric_display_name]
                final_metric_col = metric_col
            
                voyage_counts = (
                    filtered_df
                    .groupby(["FISCAL_YEAR","ACCOUNTING_PERIOD","VOYAGE_ID"], as_index=False)
                    .agg(
                        **{
                            metric_col: (metric_col, "first"),
                            "PORTCD_ACTIVITY": ("PORTCD_ACTIVITY", "first")
                        }
                    )
                )
                voyage_counts = (
                    voyage_counts
                    .groupby("VOYAGE_ID", as_index=False)
                    .agg(
                        **{
                            metric_col: (metric_col, "sum"),
                            "PORTCD_ACTIVITY": ("PORTCD_ACTIVITY", "first")
                        }
                    )
                )

            elif selected_metric_display_name in (
                "Per Capacity Day (Margin PCD)",
                "Per Passenger Day (Margin PPD)"
            ):
            
                # ---- Numerator
                df_amount = (
                    filtered_df
                    .groupby("VOYAGE_ID", as_index=False)
                    .agg(
                        COMPONENT_AMOUNT=("COMPONENT_AMOUNT", "sum"),
                        PORTCD_ACTIVITY=("PORTCD_ACTIVITY", "first")
                    )
                )

                # ---- Denominator 
                #st.write(filtered_df)
                denom_col = ( "NEW_PRTD_CAPS_DAYS" if selected_metric_display_name == "Per Capacity Day (Margin PCD)" else "NEW_PRTD_PAX_DAYS" )
                df_denom = (
                    filtered_df
                    .groupby(["FISCAL_YEAR","ACCOUNTING_PERIOD","VOYAGE_ID"], as_index=False)[denom_col]
                    .first()
                )
                #st.write(df_denom)
                df_denom = (
                    df_denom
                    .groupby("VOYAGE_ID", as_index=False)[denom_col]
                    .sum()
                )
                #st.write(df_denom)
                voyage_counts = df_amount.merge(df_denom, on="VOYAGE_ID", how="inner")
                #st.write(voyage_counts)
                final_metric_col = "PCD" if denom_col == "NEW_PRTD_CAPS_DAYS" else "PPD"
                
                voyage_counts[final_metric_col] = (
                    voyage_counts["COMPONENT_AMOUNT"] / voyage_counts[denom_col]
                )

            
            else:
                metric_col = metric_map[selected_metric_display_name]
                final_metric_col = metric_col
            
                voyage_counts = (
                    filtered_df
                    .groupby("VOYAGE_ID", as_index=False)
                    .agg(
                        COMPONENT_AMOUNT=("COMPONENT_AMOUNT", "sum"),
                        PORTCD_ACTIVITY=("PORTCD_ACTIVITY", "first")
                    )
                )
                
            # Sail Day QTY Aggregation and join with main data
            #df_sail_m = df[df['M0_AND_M1']!='OBR Accounts']
            if (filtered_df["M0_AND_M1"].nunique() == 1 and filtered_df["M0_AND_M1"].iloc[0] == "OBR Accounts"):
                df_tmp = filtered_df.copy()

                # Ensure datetime
                df_tmp["SAIL_DATE"] = pd.to_datetime(df_tmp["SAIL_DATE"])
            
                # Extract total sail days from VOYAGE_CD (e.g. GWY-20250126-07-MSY-MSY)
                df_tmp["TOTAL_SAIL_DAYS"] = (
                    df_tmp["VOYAGE_CD"]
                    .str.split("-")
                    .str[2]
                    .astype(int)
                )
            
                def calculate_month_sail_days(row):
                    sail_start = row["SAIL_DATE"]
                    total_days = row["TOTAL_SAIL_DAYS"]
            
                    sail_end = sail_start + pd.Timedelta(days=total_days - 1)
            
                    # Accounting month window
                    month_start = pd.Timestamp(row["FISCAL_YEAR"], row["ACCOUNTING_PERIOD"], 1)
                    month_end = month_start + pd.offsets.MonthEnd(1)
            
                    overlap_start = max(sail_start, month_start)
                    overlap_end = min(sail_end, month_end)
            
                    if overlap_start > overlap_end:
                        return 0
            
                    return (overlap_end - overlap_start).days + 1
            
                df_tmp["CONVERTED_SAIL_DAY_QTY"] = df_tmp.apply(
                    calculate_month_sail_days, axis=1
                )
                sail_days_by_period = (
                    df_tmp
                    .groupby(["FISCAL_YEAR", "ACCOUNTING_PERIOD","VOYAGE_ID"], as_index=False)["CONVERTED_SAIL_DAY_QTY"]
                    .first()
                )
                sail_days_by_period = sail_days_by_period.groupby("VOYAGE_ID", as_index=False)["CONVERTED_SAIL_DAY_QTY"].sum()
                #sail_days_by_period["CONVERTED_SAIL_DAY_QTY"] = sail_days_by_period["CONVERTED_SAIL_DAY_QTY"]/2
            else:
                filtered_df1 = filtered_df[filtered_df['M0_AND_M1']!='OBR Accounts']
                #st.write(filtered_df1)
                sail_days_by_period = (
                    filtered_df1
                    .drop_duplicates(
                        subset=["FISCAL_YEAR", "ACCOUNTING_PERIOD", "VOYAGE_ID"]
                    )
                    .groupby("VOYAGE_ID", as_index=False)["CONVERTED_SAIL_DAY_QTY"]
                    .sum()
                )
            #st.write(df_tmp)
            voyage_counts = voyage_counts.merge(
                sail_days_by_period,
                on="VOYAGE_ID",
                how="left"
            )  
        
            # =====================================================
            #   CLEAN SORTING + LINE CHART LOGIC (MATCH FUNC #1)
            # =====================================================
    
            order_requested = order_column is not None and order_column.upper() != "NONE"
    
            # Normalize order column
            if order_requested:
                oc = order_column.upper()
    
                if oc == "CII":
                    sort_by_column = "CII_SCORE"
                elif oc in ["LOAD FACTOR", "LF"]:
                    sort_by_column = "LF"
                else:
                    sort_by_column = oc
            else:
                sort_by_column = None
    
            # Add LF if needed
            if sort_by_column == "LF":
                temp = filtered_df.groupby("VOYAGE_ID")[["PAX_DAYS", "DO_CAP_DAYS"]].first().reset_index()
                temp["LF"] = temp["PAX_DAYS"] / temp["DO_CAP_DAYS"]
                voyage_counts = voyage_counts.merge(temp[["VOYAGE_ID", "LF"]], on="VOYAGE_ID", how="left")
    
            # Add GSS / CII if needed
            elif sort_by_column in ["CII_SCORE", "GSS"]:
                temp = filtered_df.groupby("VOYAGE_ID")[sort_by_column].first().reset_index()
                voyage_counts = voyage_counts.merge(temp, on="VOYAGE_ID", how="left")
    
            # ---------------------------
            # APPLY SORTING
            # ---------------------------
            if sort_by_column:
                ascending = (order_type.lower() == "asc")
                voyage_counts = voyage_counts.sort_values(by=sort_by_column, ascending=ascending)
            else:
                # Default Option C: sort by metric descending
                voyage_counts = voyage_counts.sort_values(by=final_metric_col, ascending=False)

    
            sorted_voyage_ids = voyage_counts["VOYAGE_ID"].tolist()
 
    
            # =====================================================
            #      COMBINED BAR + LINE CHART
            # =====================================================
            fig = go.Figure()
    
            # BAR TRACE (your original color)
            fig.add_trace(go.Bar(
                x=voyage_counts["VOYAGE_ID"],
                y=voyage_counts[final_metric_col],
                name=selected_metric_display_name,
                marker_color="skyblue",
                marker_line_width=0,
            
                # Pass extra fields to hover
                customdata=np.stack([
                    voyage_counts["CONVERTED_SAIL_DAY_QTY"],
                    voyage_counts["PORTCD_ACTIVITY"]
                ], axis=-1),
            
                hovertemplate=(
                    "Value: %{y:,.2f}<br>"
                    "Sail Day Qty: %{customdata[0]}<br>"
                    "Itinerary: %{customdata[1]}<extra></extra>"
                )
            ))
            #st.write(voyage_counts)
    
            # OPTIONAL LINE TRACE (only if sorting requested)
            if sort_by_column:
                fig.add_trace(go.Scatter(
                    x=voyage_counts["VOYAGE_ID"],
                    y=voyage_counts[sort_by_column],
                    mode="lines+markers",
                    name=sort_by_column,
                    yaxis="y2",
                    line=dict(color="#00bfa5", width=2),
                    marker=dict(color="#00bfa5"),
                    hovertemplate="%{y:,.2f}<extra></extra>"
                ))
    
            # LAYOUT
            fig.update_layout(
                title=f"Voyage Profitability by {selected_metric_display_name}",
                xaxis=dict(
                    title="Voyage ID",
                    categoryorder="array",
                    categoryarray=sorted_voyage_ids,
                ),
                yaxis=dict(title=selected_metric_display_name),
                yaxis2=dict(
                    title=sort_by_column if sort_by_column else "",
                    overlaying="y",
                    side="right"
                ),
                hovermode="x unified",
                template="plotly_white",
                height=420,
            )
    
            st.plotly_chart(fig, use_container_width=True)
    
        # ===========================
        #   VOYAGE SELECTION UI
        # ===========================
        if "selected_voyage" in st.session_state and st.session_state.selected_voyage in all_voyage_options:
            default_voyage = st.session_state.selected_voyage
        elif target_voyage_ids_from_query:
            default_voyage = target_voyage_ids_from_query[0]
        else:
            default_voyage = all_voyage_options[0] if all_voyage_options else None
    
        if default_voyage:
            selected_voyage_id = st.selectbox(
                "Select a Voyage ID for detailed analysis:",
                options=all_voyage_options,
                index=all_voyage_options.index(default_voyage),
                key="voyage_select_direct"
            )
        else:
            st.warning("No voyage IDs available.")
            return


    elif mode == "By Itinerary Route":
        portcd_activity_analysis_flow(df,filtered_df ,primary_metric_col, selected_metric_display_name,order_column,order_type)
        return

    if selected_voyage_id and selected_voyage_id != 'None':
        st.session_state.selected_voyage = selected_voyage_id
        st.markdown("---")
        st.subheader(f"Analyzing Voyage: **{selected_voyage_id}**")

        # Component breakdown
        plot_voyage_component_contribution(df, selected_voyage_id, primary_metric_col, selected_metric_display_name)

        # Clustering
        st.markdown("---")
        st.subheader("Like for Like Voyages (Clustering Based on Key Attributes)")
        clustered_results, target_voyage_dict, target_voyage_row = cluster_voyages(df, selected_voyage_id)

        tier_mapping = {
            "Tier 1": "Top Matches",
            "Tier 2": "Medium Matches",
            "Tier 3": "Least Matches"
        }

        for tier_code, data in clustered_results.items():
            tier_label = tier_mapping.get(tier_code, tier_code)
            tier_df = data['df']

            if not tier_df.empty:
                with st.expander(f"{tier_label} for {selected_voyage_id}", expanded=False):

                    plot_clustered_voyages_bar_chart(
                        clustered_df=tier_df,
                        tier_name=tier_label,
                        primary_metric_col=primary_metric_col,
                        selected_metric_display_name=selected_metric_display_name,
                        key_suffix=tier_code,
                        base_row=target_voyage_dict,
                        order_column=order_column, 
                        order_type=order_type
                        )

            elif tier_code == "Least Matches":
                st.info(f"No direct Least Matches found. Showing fallback voyages with matching product and port activity.")


# -------------------------------------------------------------------------------------------------
#Deployment Analysis Flow
# -------------------------------------------------------------------------------------------------
def deployment_analysis_flow(df, filtered_df, query, primary_metric_col, selected_metric_display_name, order_column=None,order_type=None,filtered_df_un=None):
    
    #world map graph
    deployment_world_map(filtered_df_un,primary_metric_col,selected_metric_display_name)
    #st.write(filtered_df_un.head())
    
    
        #st.write(future_df.head())
    # KPI chart
    deployment_KPI_chart(filtered_df_un,primary_metric_col,selected_metric_display_name)
    # deployment product activity
    deployment_product_activity(filtered_df_un,primary_metric_col,selected_metric_display_name)
    # flow option selection
   # --- NEW: Centralized flow control widget ---
    st.markdown("---") # Add a separator for clarity
    flow_selection = st.radio(
        "Select Analysis Flow",
        options=["Product-flow", "Ship-flow"],
        horizontal=True,
        key="deployment_flow_toggle",
        help="This changes the primary dimension for the drill-down charts below."
    )
    # Filter selection
    final_filtered_df = pd.DataFrame() # Initialize an empty dataframe
    selected_primary_filter = None

    if flow_selection == 'Product-flow':
        product_list = sorted(filtered_df['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist())
        if product_list:
            selected_primary_filter = st.selectbox(
                "Select a Product to Analyze", 
                product_list, 
                key="primary_product_filter"
            )
            final_filtered_df = filtered_df[filtered_df["RM_ROLLUP_PRODUCT_DESC"] == selected_primary_filter].copy()

    else: # Ship-flow
        ship_list = sorted(filtered_df['SHIP_CD'].dropna().unique().tolist())
        if ship_list:
            selected_primary_filter = st.selectbox(
                "Select a Ship to Analyze", 
                ship_list, 
                key="primary_ship_filter"
            )
            final_filtered_df = filtered_df[filtered_df["SHIP_CD"] == selected_primary_filter].copy()

    # --- UPDATED: Calling the simplified heatmap function ---
    # We now pass the more granular dataframe and the selected filter value.
    if not final_filtered_df.empty:
        deployment_dynamic_heatmap(
            final_filtered_df, # Pass the final, granularly filtered data
            primary_metric_col,
            selected_metric_display_name,
            flow_selection,
            selected_primary_filter # Pass the name for the title
        )
    else:
        st.warning("Please make a selection to view the heatmap.")

    st.markdown("---")
    st.markdown("### 🔬 Refine View for Deep Dive Analysis")

    if flow_selection == 'Product-flow':
        # Get list of ships for the selected product
        ship_options = ['All'] + sorted(final_filtered_df['SHIP_CD'].dropna().unique().tolist())
        
        final_ship_filter = st.selectbox(
            f"Select a Ship for deeper analysis of '{selected_primary_filter}'",
            options=ship_options
        )
        secondary_filtered_df = final_filtered_df
        if final_ship_filter !='All':
            secondary_filtered_df = final_filtered_df[final_filtered_df['SHIP_CD']==final_ship_filter].copy()

    else: # Ship-flow
        # Get list of ships for the selected product
        product_options = ['All'] + sorted(final_filtered_df['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist())
        
        final_product_filter = st.selectbox(
            f"Select a Product for deeper analysis of '{selected_primary_filter}'",
            options=product_options
        )
        secondary_filtered_df = final_filtered_df
        if final_product_filter !='All':
            secondary_filtered_df = final_filtered_df[final_filtered_df['RM_ROLLUP_PRODUCT_DESC']==final_product_filter].copy()

    if not secondary_filtered_df.empty:
        deployment_voyage_performance_charts(secondary_filtered_df,primary_metric_col,
            selected_metric_display_name,flow_selection,order_column, order_type) # Pass flow_type for context
    else:
        st.warning("No data available for the selected deep-dive criteria.")
    

# ----------------------------------------------------------------------------------------------------------
# Deployment Graph functions
# -------------------------------------------------------------------------------------------------------
# world map graph
def deployment_world_map(df, metric_col, metric_display_name):

    st.header("🗺️ Product Deployment Map")

    # Updated coordinates to avoid overlap
    product_locations = {
        "Hawaii": (21.27, -157.82),
        "Bahamas 3 & 4 nights": (25.03, -78.04),
        "Caribbean - Other": (16.27, -61.55),
        "Caribbean - Miami": (25.76, -80.19),
        "Mexican Riviera": (23.22, -106.42),
        "Alaska": (58.30, -134.42),
        "Panama Canal-Miami": (9.0, -79.6),
        "Repositions": (10.0, -30.0),        # ✔ moved away from overlap
        "Caribbean - Gulf": (21.0, -86.8),
        "Europe": (41.9, 12.5),
        "Other Products": (5.0, 20.0),       # ✔ moved
        "Canada & New England": (45.5, -63.6),
        "Bermuda": (32.3078, -64.7505),
        "Caribbean - Tampa": (27.95, -82.46),
        "Caribbean": (18.0, -66.0),
        "Caribbean - New York": (40.71, -74.00),
        "AFRICA-SOUTH AFRICA": (-33.92, 18.42),
        "Asia": (13.41, 103.87),
        "Panama Canal-Panama City": (8.98, -79.52),
        "Panama Canal-Tampa": (27.95, -82.46),
        "Australia": (-33.86, 151.20),
        "South America": (-12.05, -77.04),
        "South Pacific": (-17.68, 177.42),
        "Undefined": (-10.0, 60.0),          # ✔ moved
        "Caribbean-Philadelphia": (39.95, -75.17),
    }

    # ============================== AGG LOGIC (unchanged) ==============================
    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        df1 = df.groupby(["RM_ROLLUP_PRODUCT_DESC"], as_index=False)["COMPONENT_AMOUNT"].sum()
        df2 = df.groupby(["RM_ROLLUP_PRODUCT_DESC", "VOYAGE_ID"])[metric_col].first().reset_index()
        df2 = df2.groupby(["RM_ROLLUP_PRODUCT_DESC"], as_index=False)[metric_col].sum()
        agg_df = df1.merge(df2, on=["RM_ROLLUP_PRODUCT_DESC"], how="inner")
        agg_df["Metric_Value"] = agg_df["COMPONENT_AMOUNT"] / agg_df[metric_col]

    elif metric_display_name in ('Passenger Days', 'Capacity Days'):
        df_voy = df.groupby(['RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'])[metric_col].first().reset_index()
        agg_df = df_voy.groupby(['RM_ROLLUP_PRODUCT_DESC']).agg(
            Metric_Value=(metric_col, 'sum')).reset_index()
    else:
        agg_df = df.groupby(['RM_ROLLUP_PRODUCT_DESC']).agg(
            Metric_Value=('COMPONENT_AMOUNT', 'sum')).reset_index()

    # ============================== PLOTTING PREP (unchanged) ==============================
    agg_df['lat'] = agg_df['RM_ROLLUP_PRODUCT_DESC'].apply(lambda x: product_locations.get(x, (0, 0))[0])
    agg_df['lon'] = agg_df['RM_ROLLUP_PRODUCT_DESC'].apply(lambda x: product_locations.get(x, (0, 0))[1])

    agg_df['Metric_Display'] = (
        agg_df['Metric_Value'] / 1_000_000 if agg_df['Metric_Value'].max() > 10000 else agg_df['Metric_Value']
    )
    hover_suffix = "M" if agg_df['Metric_Value'].max() > 10000 else ""

    if agg_df.empty:
        st.warning("❌ No data to plot for the selected filters.")
        return

    # ============================== PYDECK MAP ==============================

    # View
    view_state = pdk.ViewState(
        latitude=20,
        longitude=0,
        zoom=1.0,
        pitch=0
    )

    # Turbo color
    vmin = agg_df["Metric_Display"].min()
    vmax = agg_df["Metric_Display"].max()

    def turbo_color(value):
        if vmax == vmin:
            norm = 0
        else:
            norm = (value - vmin) / (vmax - vmin)
        r, g, b, _ = cm.turbo(norm)
        return [int(r * 255), int(g * 255), int(b * 255)]

    agg_df["color"] = agg_df["Metric_Display"].apply(turbo_color)

    # FIXED bubble radius (constant)
    CONSTANT_RADIUS = 220000   # tweak if needed

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=agg_df,
        get_position=["lon", "lat"],
        get_radius=CONSTANT_RADIUS,     # ✔ constant size
        get_fill_color="color",         # ✔ only color varies
        pickable=True,
        stroke=False
    )

    # Tooltip
    tooltip = {
        "html": (
            "<b>{RM_ROLLUP_PRODUCT_DESC}</b><br>"
            f"{metric_display_name}: " + "{Metric_Display}" + hover_suffix
        ),
        "style": {"backgroundColor": "white", "color": "black"}
    }

    # Dark map as you requested
    deck = pdk.Deck(
        layers=[scatter_layer],
        initial_view_state=view_state,
        map_style="dark",
        tooltip=tooltip,
        height=600,
    )

    st.pydeck_chart(deck)




# deployment KPI chart
def deployment_KPI_chart(df,metric_col,metric_display_name):
    """
    Displays yearly KPI cards. The first two cards (Capacity, Passengers) are static,
    while the third card dynamically shows the selected metric ($ Value, PCD, or PPD).
    
    Args:
        df (pd.DataFrame): The filtered dataframe for the calculations.
        metric_display_name (str): The name of the metric to display in the third card.
    """

    # --- 1. AGGREGATION ---
    # First, aggregate to the voyage level to get unique denominators
    voyage_level_agg = df.groupby(['FISCAL_YEAR','ACCOUNTING_PERIOD','SHIP_CD','RM_ROLLUP_PRODUCT_DESC','VOYAGE_ID']).agg(
        Capacity_Days_Voyage=('NEW_PRTD_CAPS_DAYS', 'first'),
        Passenger_Days_Voyage=('NEW_PRTD_PAX_DAYS', 'first'),
        Dollar_Value_Voyage=('COMPONENT_AMOUNT', 'sum')
    ).reset_index()

    # Now, aggregate by year to get final totals
    agg_df = voyage_level_agg.groupby('FISCAL_YEAR').agg(
        Capacity_Days=('Capacity_Days_Voyage', 'sum'),
        Passenger_Days=('Passenger_Days_Voyage', 'sum'),
        Dollar_Value=('Dollar_Value_Voyage', 'sum')
    ).reset_index()
    
    # --- 2. CALCULATE ALL POTENTIAL METRICS ---
    # Calculate derived metrics (PCD and PPD) safely, handling division by zero
    agg_df['PCD'] = np.divide(agg_df['Dollar_Value'], agg_df['Capacity_Days'], 
                              out=np.zeros_like(agg_df['Dollar_Value'], dtype=float), 
                              where=agg_df['Capacity_Days']!=0)
    
    agg_df['PPD'] = np.divide(agg_df['Dollar_Value'], agg_df['Passenger_Days'],
                              out=np.zeros_like(agg_df['Dollar_Value'], dtype=float),
                              where=agg_df['Passenger_Days']!=0)

    # --- 3. DYNAMICALLY SELECT THE METRIC FOR THE 3RD CARD ---
    if 'PCD' in metric_display_name:
        metric_col_to_show = 'PCD'
        metric_title = "💰 Margin PCD"
    elif 'PPD' in metric_display_name:
        metric_col_to_show = 'PPD'
        metric_title = "💰 Margin PPD"
    else: # Default to Dollar Value
        metric_col_to_show = 'Dollar_Value'
        metric_title = "💰 Dollar Value"

    # --- 4. CALCULATE PERCENTAGE CHANGES ---
    agg_df["Cap_Change"] = agg_df["Capacity_Days"].pct_change() * 100
    agg_df["Pax_Change"] = agg_df["Passenger_Days"].pct_change() * 100
    agg_df["Metric_Change"] = agg_df[metric_col_to_show].pct_change() * 100
    
    # Round all change columns at once
    change_cols = ["Cap_Change", "Pax_Change", "Metric_Change"]
    agg_df[change_cols] = agg_df[change_cols].round(1)

    # --- 5. RENDER UI ---
    def get_delta(val):
        if pd.isna(val): return "<span style='font-size:0.9em;'>&nbsp;</span>"
        symbol = "▲" if val > 0 else "▼"
        color = "green" if val > 0 else "red"
        return f"<span style='color:{color}; font-size:0.9em'>{symbol} {abs(val)}%</span>"
    
    st.markdown("### 📊 Yearly KPIs")
    
    if agg_df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # --- Map fiscal years to display headers ---
    if st.session_state.get("use_future_data", False):
        year_header_map = {
            2023: "2023",
            2024: "2024",
            2025: "2025",
            2026: "2026 (12F)",
            2027: "2027 (12F)"
        }
    else:
        # Default: show normal fiscal year
        year_header_map = {y: str(y) for y in agg_df['FISCAL_YEAR'].unique()}
    
    columns = st.columns(len(agg_df))
    for i, row in agg_df.iterrows():
        fiscal_year = int(row['FISCAL_YEAR'])
        header_label = year_header_map.get(fiscal_year, str(fiscal_year))
    
        with columns[i]:
            st.markdown(f"<h4 style='text-align:center'>{header_label}</h4>", unsafe_allow_html=True)
    
            # Card 1: Capacity Days (Static)
            st.markdown(f"""
                <div style='background:#f0f9ff;padding:10px 15px;border-radius:12px;margin-bottom:10px;text-align:center'>
                    <div style='font-size:1em;'>🚢 <strong>Capacity Days</strong></div>
                    <div style='font-size:1.2em; font-weight:bold;'>{int(row['Capacity_Days']):,}</div>
                    {get_delta(row['Cap_Change'])}
                </div>
            """, unsafe_allow_html=True)
    
            # Card 2: Passenger Days (Static)
            st.markdown(f"""
                <div style='background:#fff9e6;padding:10px 15px;border-radius:12px;margin-bottom:10px;text-align:center'>
                    <div style='font-size:1em;'>👥 <strong>Passenger Days</strong></div>
                    <div style='font-size:1.2em; font-weight:bold;'>{int(row['Passenger_Days']):,}</div>
                    {get_delta(row['Pax_Change'])}
                </div>
            """, unsafe_allow_html=True)
    
            # Card 3: DYNAMIC METRIC
            if metric_col_to_show == 'Dollar_Value':
                display_value = f"${row[metric_col_to_show]/1_000_000:.1f}M"
            else:
                display_value = f"${row[metric_col_to_show]:,.2f}"
    
            st.markdown(f"""
                <div style='background:#e6fff3;padding:10px 15px;border-radius:12px;margin-bottom:10px;text-align:center'>
                    <div style='font-size:1em;'><strong>{metric_title}</strong></div>
                    <div style='font-size:1.2em; font-weight:bold;'>{display_value}</div>
                    {get_delta(row['Metric_Change'])}
                </div>
            """, unsafe_allow_html=True)

# deployment product activity
def deployment_product_activity(df,metric_col,metric_display_name):
    st.header("📅 Interactive Product Activity Grid")

    # 1. We only care about activity, so we drop duplicates.
    grid_df = df[['FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID']].drop_duplicates()

    if grid_df.empty:
        st.warning("❌ No data available to display the activity grid.")
    else:
        # 2. Prepare data for plotting and filtering
        grid_df['Year_Product'] = grid_df['FISCAL_YEAR'].astype(str) + ' - ' + grid_df['RM_ROLLUP_PRODUCT_DESC']
        month_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 
                     7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        grid_df['Month'] = grid_df['ACCOUNTING_PERIOD'].map(month_map)
        #st.write(grid_df)
        all_products = sorted(grid_df['RM_ROLLUP_PRODUCT_DESC'].unique())
        grid_df['Year_Product'] = grid_df['FISCAL_YEAR'].astype(str) + ' - ' + grid_df['RM_ROLLUP_PRODUCT_DESC']
        grid_df['Month'] = grid_df['ACCOUNTING_PERIOD'].map(month_map)
        
        all_y_labels = sorted(grid_df['Year_Product'].unique())
        all_x_labels = list(month_map.values())
        
        # --- Compute distinct voyages for all Year_Product x Month at once ---
        voyages_pivot = (
            grid_df.groupby(['Year_Product', 'Month'])['VOYAGE_ID']
            .nunique()
            .unstack(fill_value=0)
            .reindex(index=all_y_labels, columns=all_x_labels, fill_value=0)
        )
        # --- CHANGE: Create an "All" option and integrate it into the multiselect ---
        st.markdown("Use the dropdown below to select which products to display on the chart.")
        
        # Add the "(Select All)" option to the list of products
        multiselect_options = ["(Select All)"] + all_products
        
        # Use a new variable to hold the user's direct selection
        selection = st.multiselect(
            'Select Products:',
            options=multiselect_options,
            default=["(Select All)"] # Set the default to "(Select All)"
        )

        # Determine the final list of products to display
        if "(Select All)" in selection:
            selected_products = all_products
        else:
            selected_products = selection

        # Stop and show a message if no products are selected
        if not selected_products:
            st.warning("Please select at least one product to display the chart.")
            st.stop()

        # --- The rest of the code remains the same ---
        filtered_grid_df = grid_df[grid_df['RM_ROLLUP_PRODUCT_DESC'].isin(selected_products)]
        
        product_color_mapping = {
            "Hawaii": "rgb(0, 204, 150)",             # Teal
            "Bahamas 3 & 4 nights": "rgb(255, 161, 90)",  # Bright Orange
            "Caribbean - Other": "rgb(171, 99, 250)", # Purple
            "Caribbean - Miami": "rgb(99, 110, 250)", # Blue
            "Mexican Riviera": "rgb(0, 153, 255)",    # Sky Blue
            "Alaska": "rgb(0, 102, 204)",             # Deep Blue
            "Panama Canal-Miami": "rgb(239, 85, 59)", # Red-Orange
            "Repositions": "rgb(128, 128, 128)",      # Neutral Grey
            "Caribbean - Gulf": "rgb(0, 128, 128)",   # Dark Teal
            "Europe": "rgb(255, 217, 102)",           # Gold-Yellow
            "Other Products": "rgb(182, 232, 128)",   # Light Green
            "Canada & New England": "rgb(153, 102, 51)",  # Brown
            "Bermuda": "rgb(255, 102, 146)",          # Pink
            "Caribbean - Tampa": "rgb(255, 140, 0)",  # Orange-Red
            "Caribbean": "rgb(0, 204, 255)",          # Cyan
            "Caribbean - New York": "rgb(204, 0, 102)", # Magenta
            "AFRICA-SOUTH AFRICA": "rgb(102, 0, 51)", # Deep Maroon
            "Asia": "rgb(240, 122, 255)",             # Violet
            "Panama Canal-Panama City": "rgb(255, 99, 71)", # Tomato Red
            "Panama Canal-Tampa": "rgb(255, 178, 102)",     # Soft Orange
            "Australia": "rgb(0, 128, 255)",          # Ocean Blue
            "South America": "rgb(204, 85, 0)",       # Burnt Orange
            "South Pacific": "rgb(51, 204, 204)",     # Aqua
            "Undefined": "rgb(160, 160, 160)",        # Neutral Grey
            "Caribbean-Philadelphia": "rgb(0, 204, 102)" # Emerald Green
        }


        all_y_labels = sorted(filtered_grid_df['Year_Product'].unique())
        all_x_labels = list(month_map.values())

        master_hover_pivot = filtered_grid_df.pivot_table(
            index='Year_Product', columns='Month', values='FISCAL_YEAR', aggfunc='first'
        ).reindex(index=all_y_labels, columns=all_x_labels)
        master_hover_text = master_hover_pivot.applymap(lambda x: str(int(x)) if pd.notna(x) else '')

        fig_grid = go.Figure()

        for product in selected_products:
            product_df = grid_df[grid_df['RM_ROLLUP_PRODUCT_DESC'] == product].copy()

            # Mask: 1 where there is data, 0 where not
            z_data = product_df.pivot_table(
                index='Year_Product', columns='Month', values='FISCAL_YEAR', aggfunc='count'
            ).reindex(index=all_y_labels, columns=all_x_labels, fill_value=0)
        
            # Convert to binary mask (1 if >0, else 0)
            z_mask = (z_data > 0).astype(int)
        
            # --- Customdata for tooltip (with voyage counts) ---
            customdata = []
            for y in all_y_labels:
                _, prod = y.split(" - ", 1)
                row = []
                for m in all_x_labels:
                    voyages_val = int(voyages_pivot.loc[y, m]) if (y in voyages_pivot.index and m in voyages_pivot.columns) else 0
                    row.append([prod, y.split(" - ", 1)[0], voyages_val])
                customdata.append(row)
            customdata = np.array(customdata)
        
            product_color = product_color_mapping.get(product, 'rgb(128,128,128)')
        
            fig_grid.add_trace(go.Heatmap(
                x=all_x_labels,
                y=all_y_labels,
                z=z_mask,  # only binary mask for coloring
                name=product,
                colorscale=[[0, 'rgba(0,0,0,0)'], [1, product_color]],  # fixed product color
                showscale=False,
                customdata=customdata,
                hovertemplate=(
                    "<b>Product:</b> %{customdata[0]}<br>"
                    "<b>Year:</b> %{customdata[1]}<br>"
                    "<b>Month:</b> %{x}<br>"
                    "<b>No. of Voyages:</b> %{customdata[2]:,.0f}<extra></extra>"
                ),
                xgap=2, ygap=2
            ))

        num_products_total = len(all_y_labels)
        visible_rows = 15
        if num_products_total > visible_rows:
            # Many products → show only last `visible_rows` with scroller
            yaxis_range = [num_products_total - visible_rows - 0.5, num_products_total - 0.5]
        else:
            # Few products → fit tightly around data
            yaxis_range = [-0.5, num_products_total - 0.5]

        fig_grid.update_layout(
            template="plotly_white",
            height=700,
            title_text="Product Activity Dashboard",
            xaxis=dict(tickmode='array', tickvals=all_x_labels, showgrid=False, title_text="Month"),
            yaxis=dict(range=yaxis_range, title_text="Product by Year", showgrid=False),
            showlegend=False,
            barmode='stack',
            margin=dict(t=80, b=50)
        )
        
        st.plotly_chart(fig_grid, use_container_width=True)
        df_filtered = df[df["RM_ROLLUP_PRODUCT_DESC"].isin(selected_products)] if selected_products else df.copy()

        if df_filtered.empty:
            add_insight(
                st.session_state["all_insights"],
                "product_activity_no_data",
                "⚠️ No activity data available for the selected product(s)."
            )
        else:
        
            # ===============================================================
            # 1️⃣ Executive Summary – Top vs Bottom Product Activity
            # ===============================================================
            st.markdown("### 🧭 Executive Insights")

            # Compute voyages per product
            product_voyage_counts = (
                df_filtered.groupby("RM_ROLLUP_PRODUCT_DESC")["VOYAGE_ID"]
                .nunique()
                .reset_index(name="Distinct_Voyages")
            )
            
            # Compute voyages per month
            month_voyage_counts = (
                df_filtered.groupby("ACCOUNTING_PERIOD")["VOYAGE_ID"]
                .nunique()
                .reset_index(name="Distinct_Voyages")
            )
            
            # Compute YOY by product
            yoy = (
                df_filtered.groupby(["RM_ROLLUP_PRODUCT_DESC", "FISCAL_YEAR"])["VOYAGE_ID"]
                .nunique()
                .reset_index(name="Distinct_Voyages")
            )
            
            # ---- 1. Top & Bottom Products ----
            if not product_voyage_counts.empty:
                top_product = product_voyage_counts.loc[
                    product_voyage_counts["Distinct_Voyages"].idxmax()
                ]
                bottom_product = product_voyage_counts.loc[
                    product_voyage_counts["Distinct_Voyages"].idxmin()
                ]
            else:
                top_product = bottom_product = None
            
            insight_top_bottom = f"""
            ### 🔹 Product Performance Leaders & Laggards
            - **Top Performing Product:** **{top_product['RM_ROLLUP_PRODUCT_DESC']}** with **{top_product['Distinct_Voyages']}** voyages.
            - **Lowest Performing Product:** **{bottom_product['RM_ROLLUP_PRODUCT_DESC']}** with **{bottom_product['Distinct_Voyages']}** voyages.
            - This spread highlights variation in monthly deployment intensity and product demand.
            """
            
            add_insight(
                st.session_state["all_insights"],
                "deployment_product_activity_top_bottom",
                insight_top_bottom
            )
            #st.markdown(insight_top_bottom)
            
            
            # ---- 2. Seasonality Insights ----
            # ---- 2. Seasonality Insights (YEAR–MONTH level) ----
            df_filtered["YEAR_MONTH"] = df_filtered["FISCAL_YEAR"].astype(str) + "-" + df_filtered["ACCOUNTING_PERIOD"].astype(str)
            
            year_month_voyage_counts = (
                df_filtered.groupby("YEAR_MONTH")["VOYAGE_ID"]
                .nunique()
                .reset_index(name="Distinct_Voyages")
            )
            
            if not year_month_voyage_counts.empty:
                max_ym = year_month_voyage_counts.loc[
                    year_month_voyage_counts["Distinct_Voyages"].idxmax()
                ]
                min_ym = year_month_voyage_counts.loc[
                    year_month_voyage_counts["Distinct_Voyages"].idxmin()
                ]
            else:
                max_ym = min_ym = None
            
            insight_seasonality = f"""
            ### 🔹 Seasonality & Deployment Trends (Year–Month)
            - **Peak Activity (Year–Month):** **{max_ym['YEAR_MONTH']}** with **{max_ym['Distinct_Voyages']}** voyages.
            - **Lowest Activity (Year–Month):** **{min_ym['YEAR_MONTH']}** with **{min_ym['Distinct_Voyages']}** voyages.
            - Highlights clear temporal patterns in voyage deployment.
            """
            
            add_insight(
                st.session_state["all_insights"],
                "deployment_product_activity_seasonality",
                insight_seasonality
            )
            
            st.markdown(insight_seasonality)

            
            
            # ---- 3. Year-over-Year Growth/Decline ----
            yoy_summary = []
            for product in yoy["RM_ROLLUP_PRODUCT_DESC"].unique():
                product_data = yoy[yoy["RM_ROLLUP_PRODUCT_DESC"] == product].sort_values("FISCAL_YEAR")
                if len(product_data) >= 2:
                    first = product_data.iloc[0]
                    last = product_data.iloc[-1]
                    change = last["Distinct_Voyages"] - first["Distinct_Voyages"]
                    yoy_summary.append((product, first["FISCAL_YEAR"], last["FISCAL_YEAR"], change))
            
            yoy_text = ""
            for p, y1, y2, c in yoy_summary:
                direction = "increased" if c > 0 else "decreased"
                yoy_text += f"- **{p}** {direction} by **{abs(c)} voyages** from **{y1} → {y2}**.\n"
            
            st.markdown("### 🔹 Year-over-Year Product Trends")

            insight_yoy = (
                yoy_text if yoy_text else "Insufficient data to calculate YOY trends."
            ).strip()
            
            add_insight(
                st.session_state["all_insights"],
                "deployment_product_activity_yoy",
                insight_yoy
            )
            
            with st.expander("📘 View Detailed YOY Explanation"):
                st.markdown(insight_yoy)        
            

            # -------------------------------------------------------------


# format values for heatmap
def format_value(val, metric_display_name):
    if pd.isna(val) or val == 0: return "" # Return empty for zero to declutter heatmap
    
    # Determine prefix and formatting based on the metric's name
    prefix = "$" if "Dollar" in metric_display_name or "PCD" in metric_display_name or "PPD" in metric_display_name else ""
    
    if "PCD" in metric_display_name or "PPD" in metric_display_name:
        return f"{prefix}{val:,.2f}" # Show two decimal places for rates
        
    if abs(val) >= 1_000_000: return f"{prefix}{val/1_000_000:.1f}M"
    if abs(val) >= 1_000: return f"{prefix}{val/1_000:.1f}K"
    return f"{prefix}{val:,.0f}"

# deployment heatmap graph
def deployment_dynamic_heatmap(df, metric_col, metric_display_name, flow_type, selected_primary_item):
    
    st.markdown("### 📊 Drill-Down Heatmap")
    st.subheader(f"Heatmap: {selected_primary_item} by Year ({metric_display_name})")

    # Determine the Y-axis label based on the flow
    if flow_type == 'Product-flow':
        df["Y_LABEL"] = df["FISCAL_YEAR"].astype(str) + " | " + df["SHIP_CD"]
        yaxis_title = "Year | Ship"
    else:  # Ship-flow
        df["Y_LABEL"] = df["FISCAL_YEAR"].astype(str) + " | " + df["RM_ROLLUP_PRODUCT_DESC"]
        yaxis_title = "Year | Product"

    # --- Advanced aggregation logic (unchanged) ---
    grouping_keys = ['Y_LABEL', 'ACCOUNTING_PERIOD']

    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        voyage_level_denominators = df.drop_duplicates(subset=grouping_keys + ['VOYAGE_ID'])
        final_denominators = voyage_level_denominators.groupby(grouping_keys)[metric_col].sum().reset_index()
        final_numerators = df.groupby(grouping_keys)['COMPONENT_AMOUNT'].sum().reset_index()
        merged_agg = pd.merge(final_numerators, final_denominators, on=grouping_keys, how='left')
        merged_agg[metric_col] = np.divide(merged_agg['COMPONENT_AMOUNT'], merged_agg[metric_col])
        grouped = merged_agg.fillna(0)
    elif metric_display_name in ("Capacity Days", "Passenger Days"):
        voyage_level_values = df.drop_duplicates(subset=grouping_keys + ['VOYAGE_ID'])
        grouped = voyage_level_values.groupby(grouping_keys)[metric_col].sum().reset_index()
    else:
        grouped = df.groupby(grouping_keys)[metric_col].sum().reset_index()

    # --- New addition: percentage share per (Fiscal_Year + Accounting_Period) block ---
    grouped["FISCAL_YEAR"] = grouped["Y_LABEL"].str.split(" | ").str[0]
    total_per_year_period = grouped.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD'])[metric_col].transform('sum')
    grouped["PERCENTAGE"] = np.where(total_per_year_period != 0,
                                     (grouped[metric_col] / total_per_year_period) * 100,
                                     0)

    # --- Continue with your existing layout and sort ---
    if grouped.empty:
        st.warning("No data to display in the heatmap for this selection.")
        return

    unique_labels = sorted(df['Y_LABEL'].dropna().unique().tolist())
    all_periods = [str(p).zfill(2) for p in range(1, 13)]

    grouped['ACCOUNTING_PERIOD'] = grouped['ACCOUNTING_PERIOD'].astype(str).str.zfill(2)

    pivot = grouped.pivot_table(
        index='Y_LABEL', columns='ACCOUNTING_PERIOD', values=metric_col, fill_value=0
    ).reindex(index=unique_labels, columns=all_periods, fill_value=0).sort_index()

    # --- Align percentages for hover display ---
    percent_pivot = grouped.pivot_table(
        index='Y_LABEL', columns='ACCOUNTING_PERIOD', values='PERCENTAGE', fill_value=0
    ).reindex(index=unique_labels, columns=all_periods, fill_value=0).sort_index()

    text_labels = np.vectorize(lambda x: format_value(x, metric_display_name))(pivot.values)

    BASE_HEIGHT = 150
    ROW_HEIGHT_PIXELS = 80
    calculated_height = max(400, BASE_HEIGHT + (len(pivot.index) * ROW_HEIGHT_PIXELS))

    # --- Updated hovertemplate: added percentage info ---
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        text=text_labels,
        customdata=percent_pivot.values[..., None],
        texttemplate="%{text}",
        hovertemplate=(
            f"<b>Period:</b> %{{x}}<br>"
            f"<b>Label:</b> %{{y}}<br>"
            f"<b>{metric_display_name}:</b> %{{z:,.2f}}<br>"
            f"<b>Share (within Year-Month):</b> %{{customdata[0]:.2f}}%"
            "<extra></extra>"
        ),
        colorscale="Blues",
        showscale=True
    ))

    fig.update_layout(
        title=f"📆 Breakdown for {selected_primary_item}",
        xaxis_title="Accounting Period",
        yaxis_title=yaxis_title,
        yaxis=dict(automargin=True, type='category', autorange="reversed"),
        height=calculated_height,
        dragmode="pan",
        margin=dict(t=50, b=50, l=50, r=50)
    )

    fig.update_xaxes(
        tickmode="array",
        tickvals=pivot.columns,
        ticktext=pivot.columns,
        range=[0.5, 10.5]
    )

    st.plotly_chart(fig, use_container_width=True)

    # --------------------------- IMPROVED INSIGHT SECTION ---------------------------
    
    st.markdown("### 🔍 Key Insights from Heatmap")
    
    # Determine axis context
    if flow_type == "Product-flow":
        y_entity = "Ship"
        row_desc = "Year–Ship"
    else:
        y_entity = "Product"
        row_desc = "Year–Product"
    
    # --- Rebuild pivot for calculations ---
    pivot_values = pivot.copy()
    
    row_totals = pivot_values.sum(axis=1)
    col_totals = pivot_values.sum(axis=0)
    
    # --- Strongest / weakest rows ---
    strongest_row = row_totals.idxmax() if not row_totals.empty else None
    strongest_row_val = row_totals.max() if not row_totals.empty else 0
    
    weakest_row = row_totals.idxmin() if not row_totals.empty else None
    weakest_row_val = row_totals.min() if not row_totals.empty else 0
    
    # --- Strongest / weakest periods ---
    strongest_period = col_totals.idxmax() if not col_totals.empty else None
    strongest_period_val = col_totals.max() if not col_totals.empty else 0
    
    weakest_period = col_totals.idxmin() if not col_totals.empty else None
    weakest_period_val = col_totals.min() if not col_totals.empty else 0
    
    # --- Skew logic (corrected) ---
    col_values_nonzero = col_totals[col_totals > 0]
    try:
        month_index_ints = col_totals.index.astype(int)
    except Exception:
        # fallback if index are like '01' strings with possible leading zeros
        month_index_ints = pd.Index([int(str(m).lstrip("0") or "0") for m in col_totals.index])
    
    col_totals_indexed = pd.Series(col_totals.values, index=month_index_ints)
    
    # Define buckets
    early_months = [1,2,3,4]    # Jan-Apr
    mid_months = [5,6,7,8]      # May-Aug
    late_months = [9,10,11,12]  # Sep-Dec
    
    early_sum = col_totals_indexed.loc[col_totals_indexed.index.isin(early_months)].sum()
    mid_sum = col_totals_indexed.loc[col_totals_indexed.index.isin(mid_months)].sum()
    late_sum = col_totals_indexed.loc[col_totals_indexed.index.isin(late_months)].sum()
    
    total_nonzero = early_sum + mid_sum + late_sum
    
    if total_nonzero > 0:
        early_pct = (early_sum / total_nonzero) * 100
        mid_pct = (mid_sum / total_nonzero) * 100
        late_pct = (late_sum / total_nonzero) * 100
    
        # find the dominant bucket
        bucket_shares = {"Early (Jan–Apr)": early_pct, "Mid (May–Aug)": mid_pct, "Late (Sep–Dec)": late_pct}
        dominant_bucket = max(bucket_shares, key=bucket_shares.get)
        dominant_pct = bucket_shares[dominant_bucket]
    
        # Interpretive sentence with the actual percent
        if dominant_pct >= 50:
            skew_comment = (
                f"Activity is heavily concentrated in **{dominant_bucket}**, "
                f"accounting for around **{dominant_pct:.0f}%** of the yearly total."
            )
        elif dominant_pct >= 33:
            skew_comment = (
                f"Activity shows a noticeable tilt towards **{dominant_bucket}**, "
                f"contributing about **{dominant_pct:.0f}%** of annual activity."
            )
        else:
            skew_comment = (
                f"Activity is fairly balanced across the year, with a slight leaning "
                f"towards **{dominant_bucket}** (~**{dominant_pct:.0f}%**)."
            )

    else:
        skew_comment = "No meaningful skew detected — activity is minimal or zero."
    # ------------------------------------------------------------------  
    # --- Clean insight markdown (no white block) ---
    insight_heatmap = f"""
    **Summary of Observations ({metric_display_name}):**
    
    - **Strongest {row_desc}:** {strongest_row} with **{strongest_row_val:,.2f}**
    - **Weakest {row_desc}:** {weakest_row} with **{weakest_row_val:,.2f}**
    - **Peak Accounting Period:** {strongest_period} with **{strongest_period_val:,.2f}**
    - **Weakest Accounting Period:** {weakest_period} with **{weakest_period_val:,.2f}**
    - **Overall Monthly Skew:** {skew_comment}
    """
    
    add_insight(
        st.session_state["all_insights"],
        f"dynamic_heatmap_{flow_type}",
        insight_heatmap
    )
    
    with st.expander("📘 View Detailed Heatmap Interpretation"):
        st.markdown(insight_heatmap)


# voyage performance
def deployment_voyage_performance_charts(product_df,metric_col,metric_display_name, flow_type,
     order_column = None, order_type = None):

    if order_column == "CII":
        order_column = 'CII_SCORE'

    # State Management for Analysis Toggles

    if 'show_changes_analysis' not in st.session_state:
        st.session_state.show_changes_analysis = False
    if 'show_gaps_analysis' not in st.session_state:
        st.session_state.show_gaps_analysis = False
    
    # UI Section
    st.subheader(f"Voyage Performance by Year")
    
    # --- Data Processing ---
    # (The aggregation logic remains the same)
    product_df['SAIL_DATE'] = pd.to_datetime(product_df['SAIL_DATE'])
    if product_df.empty:
        st.warning(f"No valid data available for this selection.")
        return

    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        granular_keys = [
            'FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 
            'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'
        ]
        denominator_df = product_df[granular_keys + [metric_col]].drop_duplicates()
        voyage_agg_df = product_df.groupby(['VOYAGE_ID', 'FISCAL_YEAR']).agg(
            Total_Component_Amount=('COMPONENT_AMOUNT', 'sum'),
            SAIL_DATE=('SAIL_DATE', 'first'),
            GSS=('GSS', 'first'),
            CII_SCORE=('CII_SCORE', 'first'),
            PAX_DAYS=('PAX_DAYS', 'first'),
            CAPACITY_DAYS=('DO_CAP_DAYS', 'first')
        ).reset_index()


        # --- Step 3: Sum the de-duplicated denominator to the final level ---
        # This gives us the correct total denominator for each voyage-year.
        final_denominators = denominator_df.groupby(
            ['VOYAGE_ID', 'FISCAL_YEAR']
        )[metric_col].sum().reset_index()
        final_denominators.rename(columns={metric_col: 'Denominator_Value'}, inplace=True)
        
        
        # --- Step 4: Merge and perform the final calculation ---
        # Join the correct numerator and denominator together.
        voyage_agg_df = pd.merge(
            voyage_agg_df,
            final_denominators,
            on=['VOYAGE_ID', 'FISCAL_YEAR'],
            how='left'
        )
        
        # Now, safely perform the division to get the final metric.
        voyage_agg_df['Metric_Value'] = np.divide(
            voyage_agg_df['Total_Component_Amount'], voyage_agg_df['Denominator_Value']
        )
    elif metric_display_name in ('Passenger Days', 'Capacity Days'):
        granular_keys = [
            'FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 
            'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'
        ]
        
        # Step 1: De-duplicate using drop_duplicates()
        deduplicated_df = product_df.drop_duplicates(subset=granular_keys)
        
        # Step 2: Group the de-duplicated data and aggregate to the final level
        voyage_agg_df = deduplicated_df.groupby(['VOYAGE_ID', 'FISCAL_YEAR']).agg(
            Metric_Value=(metric_col, 'sum'), # Sum the de-duplicated metric
            SAIL_DATE=('SAIL_DATE', 'first'),
            GSS=('GSS', 'first'),
            CII_SCORE=('CII_SCORE', 'first'),
            PAX_DAYS=('PAX_DAYS', 'first'),
            CAPACITY_DAYS=('DO_CAP_DAYS', 'first')
        ).reset_index()
    else:
        voyage_agg_df = product_df.groupby(['VOYAGE_ID', 'FISCAL_YEAR']).agg(
            Metric_Value=(metric_col, 'sum'),
            SAIL_DATE=('SAIL_DATE', 'first'),
            GSS=('GSS', 'first'),
            CII_SCORE=('CII_SCORE', 'first'),
            PAX_DAYS=('PAX_DAYS', 'first'),
            CAPACITY_DAYS=('DO_CAP_DAYS', 'first')
        ).reset_index()

    voyage_agg_df['LF'] = np.divide(voyage_agg_df['PAX_DAYS'], voyage_agg_df['CAPACITY_DAYS'])
    voyage_agg_df.fillna(0, inplace=True)
    
    if voyage_agg_df.empty:
        st.warning("No data found for the selected fit criteria.")
        return
        
    # --- Charting Loop ---
    fiscal_years = sorted(voyage_agg_df['FISCAL_YEAR'].unique())
    
    for year in fiscal_years:
        st.markdown("---")
        st.markdown(f"#### {year}")
    
        yearly_voyage_df = voyage_agg_df[voyage_agg_df['FISCAL_YEAR'] == year].copy()
        
        if not yearly_voyage_df.empty:
            
            # <-- CHANGED: Sorting is now controlled by function arguments
            sort_by_column = order_column
            sort_ascending = (order_type == 'asc')
            
            if sort_by_column is None:
                yearly_voyage_df.sort_values(by='SAIL_DATE', ascending=True, inplace=True)
            else:
                yearly_voyage_df.sort_values(by=sort_by_column, ascending=sort_ascending, inplace=True)

            sorted_voyage_ids = yearly_voyage_df['VOYAGE_ID'].tolist()
                
            fig = go.Figure()
            bar_color = '#1f4e79'
            line_color = '#00bfa5'
            
            hover_df = yearly_voyage_df.copy()
            hover_df['custom_data'] = list(zip(hover_df['Metric_Value'], 
                                               hover_df['GSS'],
                                               hover_df['CII_SCORE'], 
                                               hover_df['LF']))

            fig.add_trace(go.Bar(
                x=hover_df['VOYAGE_ID'],
                y=hover_df['Metric_Value'],
                customdata=hover_df['custom_data'],
                visible=False,
                hovertemplate=""
            ))

            fig.add_trace(go.Bar(
                x=yearly_voyage_df['VOYAGE_ID'],
                y=yearly_voyage_df['Metric_Value'],
                name='Revenue',
                marker_color=bar_color,
                hovertemplate=""
            ))

            if sort_by_column and sort_by_column != 'COMPONENT_AMOUNT':
                fig.add_trace(go.Scatter(
                    x=yearly_voyage_df['VOYAGE_ID'],
                    y=yearly_voyage_df[sort_by_column],
                    mode='lines+markers',
                    name=sort_by_column,
                    yaxis='y2',
                    marker=dict(color=line_color),
                    line=dict(color=line_color, width=2),
                    hovertemplate=""
                ))
            
            fig.update_layout(
                title=f'Revenue by Voyage ID ({year})',
                xaxis={'categoryorder': 'array', 'categoryarray': sorted_voyage_ids},
                yaxis=dict(
                    title='Total Revenue ($)',
                    title_font=dict(color=bar_color),
                    tickfont=dict(color=bar_color)
                ),
                yaxis2=dict(
                    title=f'{sort_by_column}' if sort_by_column else '',
                    overlaying='y',
                    side='right',
                    title_font=dict(color=line_color),
                    tickfont=dict(color=line_color)
                ),
                height=400,
                margin=dict(t=50, b=10, l=10, r=10),
                legend=dict(x=0.01, y=0.99),
                hovermode='x unified',
                hoverlabel=dict(
                    bgcolor="rgba(255, 255, 255, 0.8)",
                    bordercolor="rgba(0, 0, 0, 0.5)",
                    font=dict(color="black")
                ),
                template="plotly_white"
            )

            if len(yearly_voyage_df['VOYAGE_ID']) > 10:
                fig.update_xaxes(
                    rangeslider_visible=False,
                    range=[-0.5, 9.5]
                )
                
            # (The rest of the plotting logic remains the same...)
            # It will now use the sorted `yearly_voyage_df` correctly.
            #fig = go.Figure()
            # ...
            # The line `if sort_by_column and sort_by_column != metric_col:` will correctly
            # add the secondary axis line chart based on the passed `order_column`.
            # ...
            st.plotly_chart(fig, use_container_width=True, key=f"voyage_performance_chart_{year}") # Placeholder for your chart code
        else:
            st.info(f"No voyage data available for {year} for this selection.")

    st.markdown("#### Options:")
    
    # <-- CHANGED: A simpler layout for the remaining analysis buttons
    col1, col2 = st.columns(2)
    
    # <-- REMOVED: Sorting buttons for GSS, CII, LF, and Revenue are gone.

    # Analysis Buttons (These remain)
    with col1:
        if st.button("Show Changes Analysis 🔎", key="show_changes_btn", use_container_width=True):
            st.session_state.show_changes_analysis = not st.session_state.show_changes_analysis
            st.rerun() # Use rerun to show/hide the analysis section immediately

    with col2:
        if st.button("Show Voyage Gap Analysis ⏳", key="show_gaps_btn", use_container_width=True):
            st.session_state.show_gaps_analysis = not st.session_state.show_gaps_analysis
            st.rerun() # Use rerun to show/hide the analysis section immediately
            
    if st.session_state.show_changes_analysis:
        st.markdown("---")
        st.subheader("Voyage Entity Change Analysis 🔎")
        
        deployment_voyage_entity_changes(product_df, flow_type, metric_col, metric_display_name)

    # --- Voyage Gap Analysis Section ---
    if st.session_state.show_gaps_analysis:
        st.markdown("---")
        st.subheader("Voyage Gap Analysis ⏳")
        deployment_voyage_gaps(product_df,flow_type)       

# Voyage Entity Change Analysis
def deployment_voyage_entity_changes(df, flow_type, metric_col, metric_display_name):
    if df.empty:
        st.info("No data available for analysis.")
        return
        
    # Use .copy() to avoid SettingWithCopyWarning
    df_cleaned = df.dropna(subset=['SAIL_DATE', 'ACCOUNTING_PERIOD']).copy()
    
    # <-- CHANGED: Remove the creation of 'SAIL_MONTH'.
    # df_cleaned['SAIL_MONTH'] = df_cleaned['SAIL_DATE'].dt.month 
    
    # <-- CHANGED: Ensure ACCOUNTING_PERIOD is a numeric type for comparisons.
    df_cleaned['ACCOUNTING_PERIOD'] = pd.to_numeric(df_cleaned['ACCOUNTING_PERIOD'], errors='coerce')
    df_cleaned.dropna(subset=['ACCOUNTING_PERIOD'], inplace=True) # Drop rows where conversion failed
    df_cleaned['ACCOUNTING_PERIOD'] = df_cleaned['ACCOUNTING_PERIOD'].astype(int)
    
    df_cleaned['FISCAL_YEAR'] = df_cleaned['FISCAL_YEAR'].astype(int)
    
    years = sorted(df_cleaned['FISCAL_YEAR'].unique())
    
    if len(years) < 2:
        st.info("At least two years of data are required to analyze changes.")
        return
        
    changes_found = False
    
    if flow_type == "Product-flow":
        grouping_entity = 'SHIP_CD'
        entity_name = 'Ships'
    else: # Ship-flow
        grouping_entity = 'RM_ROLLUP_PRODUCT_DESC'
        entity_name = 'Products'

    df_cleaned = df_cleaned.dropna(subset=[grouping_entity])

    # <-- CHANGED: Group by ACCOUNTING_PERIOD instead of SAIL_MONTH
    entities_by_month_year = df_cleaned.groupby(['FISCAL_YEAR', 'ACCOUNTING_PERIOD'])[grouping_entity].unique().apply(set).unstack(level='ACCOUNTING_PERIOD', fill_value=set())

    for i in range(len(years) - 1):
        current_year = years[i]
        next_year = years[i+1]

        # --- Logic to dynamically find the correct months to compare ---
        
        # <-- CHANGED: Use ACCOUNTING_PERIOD
        months_in_current_year = set(df_cleaned[df_cleaned['FISCAL_YEAR'] == current_year]['ACCOUNTING_PERIOD'].unique())
        months_in_next_year = set(df_cleaned[df_cleaned['FISCAL_YEAR'] == next_year]['ACCOUNTING_PERIOD'].unique())
        months_to_compare = sorted(list(months_in_current_year.union(months_in_next_year)))

        latest_year_in_data = years[-1]

        if next_year == latest_year_in_data:
            # <-- CHANGED: Use ACCOUNTING_PERIOD
            max_month_in_latest_year = df_cleaned[df_cleaned['FISCAL_YEAR'] == latest_year_in_data]['ACCOUNTING_PERIOD'].max()
            months_to_compare = [m for m in months_to_compare if m <= max_month_in_latest_year]
            
        for month in months_to_compare:
            month_str = pd.to_datetime(f"{month}", format='%m').strftime('%B')
            
            current_entities = entities_by_month_year.loc[current_year].get(month, set())
            next_entities = entities_by_month_year.loc[next_year].get(month, set())
            
            new_entities = next_entities - current_entities
            left_entities = current_entities - next_entities
            
            if new_entities or left_entities:
                changes_found = True
                
                with st.expander(f"Changes in {month_str} between {current_year} and {next_year}"):
                    st.write(f"**Changes in {month_str}:**")
                    if new_entities:
                        st.success(f"**New {entity_name}:** {', '.join(sorted(list(new_entities)))}")
                    if left_entities:
                        st.error(f"**{entity_name} Left:** {', '.join(sorted(list(left_entities)))}")
                    st.markdown("---")

                    # <-- CHANGED: Filter by ACCOUNTING_PERIOD
                    current_year_df = df_cleaned[(df_cleaned['FISCAL_YEAR'] == current_year) & (df_cleaned['ACCOUNTING_PERIOD'] == month)].copy()
                    next_year_df = df_cleaned[(df_cleaned['FISCAL_YEAR'] == next_year) & (df_cleaned['ACCOUNTING_PERIOD'] == month)].copy()
                    
                    deployment_display_monthly_comparison_chart(current_year_df, next_year_df, new_entities, left_entities, grouping_entity, metric_col, metric_display_name)

    if not changes_found:
        st.info(f"No significant {entity_name} changes found between the selected years.")

# Comparison charts - Voyage Change
def deployment_display_monthly_comparison_chart(current_year_df, next_year_df, new_entities, left_entities, grouping_entity, metric_col, metric_display_name):
            
    # Combine data for plotting
    combined_df = pd.concat([current_year_df, next_year_df], axis=0)
    #st.write(current_year_df.head())
    if metric_display_name in ('Per Capacity Day (Margin PCD)', 'Per Passenger Day (Margin PPD)'):
        granular_keys = [
            'FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 
            'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'
        ]
        denominator_df = combined_df[granular_keys + [metric_col]].drop_duplicates()
        aggregated_df = combined_df.groupby(['VOYAGE_ID', 'FISCAL_YEAR']).agg(
            Total_Component_Amount=('COMPONENT_AMOUNT', 'sum'),
            GROUPING_ENTITY=(grouping_entity, 'first') 
        ).reset_index()


        # --- Step 3: Sum the de-duplicated denominator to the final level ---
        # This gives us the correct total denominator for each voyage-year.
        final_denominators = denominator_df.groupby(
            ['VOYAGE_ID', 'FISCAL_YEAR']
        )[metric_col].sum().reset_index()
        final_denominators.rename(columns={metric_col: 'Denominator_Value'}, inplace=True)
        
        
        # --- Step 4: Merge and perform the final calculation ---
        # Join the correct numerator and denominator together.
        aggregated_df = pd.merge(
            aggregated_df,
            final_denominators,
            on=['VOYAGE_ID', 'FISCAL_YEAR'],
            how='left'
        )
        
        # Now, safely perform the division to get the final metric.
        aggregated_df['Metric_Value'] = np.divide(
            aggregated_df['Total_Component_Amount'], aggregated_df['Denominator_Value']
        )
    elif metric_display_name in ('Passenger Days', 'Capacity Days'):
        granular_keys = [
            'FISCAL_YEAR', 'ACCOUNTING_PERIOD', 'SHIP_CD', 
            'RM_ROLLUP_PRODUCT_DESC', 'VOYAGE_ID'
        ]
        
        # Step 1: De-duplicate using drop_duplicates()
        deduplicated_df = combined_df.drop_duplicates(subset=granular_keys)
        
        # Step 2: Group the de-duplicated data and aggregate to the final level
        aggregated_df = deduplicated_df.groupby(['VOYAGE_ID', 'FISCAL_YEAR']).agg(
            Metric_Value=(metric_col, 'sum'), # Sum the de-duplicated metric
            GROUPING_ENTITY=(grouping_entity, 'first') 
        ).reset_index()
    else:

        # Aggregate data by Voyage ID to get total COMPONENT_AMOUNT per voyage
        aggregated_df = combined_df.groupby(['VOYAGE_ID', 'FISCAL_YEAR']).agg(
            Metric_Value=('COMPONENT_AMOUNT', 'sum'),
            GROUPING_ENTITY=(grouping_entity, 'first') 
        ).reset_index()

    # Create the figure
    fig = go.Figure()

    # Define base and change colors
    base_color_map = {str(yr): '#1f4e79' for yr in aggregated_df['FISCAL_YEAR'].unique()}
    change_color = '#d35400' # A distinct color for emphasis
    
    # Get the number of voyages for the current year
    current_year_voyages = aggregated_df[aggregated_df['FISCAL_YEAR'] == sorted(aggregated_df['FISCAL_YEAR'].unique())[0]]['VOYAGE_ID'].count()
    
    # Add traces for each year
    for year in sorted(aggregated_df['FISCAL_YEAR'].unique()):
        year_df = aggregated_df[aggregated_df['FISCAL_YEAR'] == year]
        
        # Add a bar for each entity with a specific color
        bar_colors = []
        for entity in year_df['GROUPING_ENTITY']:
            if entity in new_entities or entity in left_entities:
                bar_colors.append(change_color)
            else:
                bar_colors.append(base_color_map[str(year)])
        
        fig.add_trace(go.Bar(
            x=[f"{voyage_id} ({year})" for voyage_id in year_df['VOYAGE_ID']],
            y=year_df['Metric_Value'],
            name=f'Revenue ({year})',
            marker_color=bar_colors,
            customdata=year_df[['VOYAGE_ID', 'GROUPING_ENTITY']].values,
            hovertemplate=f"""
            <b>Voyage ID: %{{customdata[0]}}</b><br>
            {grouping_entity}: %{{customdata[1]}}<br>
            Revenue: $%{{y:,.2f}}<br>
            <extra></extra>
            """
        ))

    # Add a divider between the years
    if len(aggregated_df['FISCAL_YEAR'].unique()) > 1:
        fig.add_shape(
            type="line",
            x0=current_year_voyages - 0.5,  # Position the line after the last bar of the first year
            y0=0,
            x1=current_year_voyages - 0.5,
            y1=1,
            yref='paper',
            line=dict(
                color="black",
                width=2,
                dash="dash",
            )
        )
    
    fig.update_layout(
        title='Monthly Performance Comparison',
        xaxis_title='Voyage ID',
        yaxis=dict(title='Total Revenue ($)', title_font=dict(color='#1f4e79')),
        barmode='group',
        legend=dict(x=0.01, y=0.99),
        height=500,
        margin=dict(t=50, b=10, l=10, r=10),
        hovermode='x unified',
        template="plotly_white"
    )
    
    st.plotly_chart(fig, use_container_width=True)
# Voyage gap analysis
def deployment_voyage_gaps(df, flow_type):
    if df.empty:
        st.info("No data available for analysis.")
        return

    # Determine the grouping entity based on the flow type
    if flow_type == "Product-flow":
        grouping_entity = 'RM_ROLLUP_PRODUCT_DESC'
        entity_name = "Product"
    else:  # Assumes 'Ship-flow'
        grouping_entity = 'SHIP_CD'
        entity_name = "Ship"

    # Check for required columns, and handle SAIL_DAY_QTY extraction
    required_cols = ['VOYAGE_ID', 'SAIL_DATE', grouping_entity, 'VOYAGE_CD', 'COMPONENT_AMOUNT']
    if 'SAIL_DAY_QTY' not in df.columns:
        required_cols.append('VOYAGE_CD')
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"Missing one or more required columns for gap analysis: {missing_cols}")
        st.warning("Please ensure your original data source includes these columns.")
        return
    
    # Check if SAIL_DAY_QTY exists, if not, create it
    if 'SAIL_DAY_QTY' not in df.columns:
        try:
            # Assuming format like VIV-20250126-07-SJU-SJU where 07 is the sail day qty
            df['SAIL_DAY_QTY'] = df['VOYAGE_CD'].str.split('-').str.get(2)
            df['SAIL_DAY_QTY'] = pd.to_numeric(df['SAIL_DAY_QTY'], errors='coerce')
        except (AttributeError, IndexError, ValueError):
            st.error("Failed to extract 'SAIL_DAY_QTY' from 'VOYAGE_CD'.")
            st.warning("Please check the format of your 'VOYAGE_CD' column.")
            return

    # Process and clean data for analysis
    df['SAIL_DATE'] = pd.to_datetime(df['SAIL_DATE'], errors='coerce')
    df['SAIL_DAY_QTY'] = pd.to_numeric(df['SAIL_DAY_QTY'], errors='coerce')

    analysis_df = df.dropna(subset=['SAIL_DATE', 'SAIL_DAY_QTY', grouping_entity, 'VOYAGE_ID', 'COMPONENT_AMOUNT']).copy()
    analysis_df['VOYAGE_END_DATE'] = analysis_df['SAIL_DATE'] + pd.to_timedelta(analysis_df['SAIL_DAY_QTY'], unit='D')
    analysis_df = analysis_df.sort_values(by=[grouping_entity, 'SAIL_DATE'])

    analysis_df['PREV_ENTITY'] = analysis_df.groupby(grouping_entity)[grouping_entity].shift(1)
    analysis_df['PREV_VOYAGE_ID'] = analysis_df.groupby(grouping_entity)['VOYAGE_ID'].shift(1)
    analysis_df['PREV_VOYAGE_END_DATE'] = analysis_df.groupby(grouping_entity)['VOYAGE_END_DATE'].shift(1)

    analysis_df['GAP_DAYS'] = (analysis_df['SAIL_DATE'] - analysis_df['PREV_VOYAGE_END_DATE']).dt.days
    
    gaps_found = analysis_df[
        (analysis_df['GAP_DAYS'] > 0) & 
        (analysis_df[grouping_entity] == analysis_df['PREV_ENTITY'])
    ].copy()

    if not gaps_found.empty:
        st.markdown("---")
        st.markdown(f"### Gaps Found for {entity_name}s Between Voyages:")
        
        # Display the timeline visualization
        deployment_visualize_voyage_gaps(gaps_found, analysis_df, entity_name, grouping_entity)
        
        # Display the dataframe with gap details
        st.markdown("#### Voyage Gap Details:")
        gaps_found = gaps_found[[
            grouping_entity, 
            'PREV_VOYAGE_ID',
            'VOYAGE_ID', 
            'SAIL_DATE', 
            'PREV_VOYAGE_END_DATE', 
            'GAP_DAYS'
        ]].sort_values(by='GAP_DAYS', ascending=False)
        
        st.dataframe(gaps_found, hide_index=True)
    else:
        st.info(f"No significant gaps found for the selected {entity_name}s between voyages.")

# Voyage gap visualization
def deployment_visualize_voyage_gaps(gaps_found_df, analysis_df, entity_name, grouping_entity):
    
    years = sorted(analysis_df['FISCAL_YEAR'].unique())
    
    for year in years:
        st.subheader(f"Voyage Timeline for {entity_name}s in {year}")
        
        # Filter data for the specific year
        year_df = analysis_df[analysis_df['FISCAL_YEAR'] == year].copy()
        
        # Create a continuous timeline of all days in the year
        min_date = year_df['SAIL_DATE'].min()
        max_date = year_df['VOYAGE_END_DATE'].max()
        
        if pd.isnull(min_date) or pd.isnull(max_date):
            st.info("No data to display for this year.")
            continue
            
        full_timeline = pd.DataFrame({'Date': pd.date_range(start=min_date, end=max_date, freq='D')})
        full_timeline['is_sailing'] = False
        full_timeline['is_gap'] = False
        full_timeline['revenue'] = 0.0

        # Mark sailing days in the full timeline
        for _, row in year_df.iterrows():
            date_range = pd.date_range(start=row['SAIL_DATE'], end=row['VOYAGE_END_DATE'], freq='D')
            
            # Mark the dates as sailing and add revenue
            full_timeline.loc[full_timeline['Date'].isin(date_range), 'is_sailing'] = True
            full_timeline.loc[full_timeline['Date'].isin(date_range), 'revenue'] += row['COMPONENT_AMOUNT'] / row['SAIL_DAY_QTY']
            
        # Mark gap days based on the gaps_found_df
        if not gaps_found_df.empty:
            for _, row in gaps_found_df.iterrows():
                if row['GAP_DAYS'] > 0:
                    gap_range = pd.date_range(start=row['PREV_VOYAGE_END_DATE'] + pd.to_timedelta(1, unit='D'),
                                               end=row['SAIL_DATE'], freq='D')
                    full_timeline.loc[full_timeline['Date'].isin(gap_range), 'is_gap'] = True

        # Create 10-day bins and aggregate
        merged_df = full_timeline.copy()
        merged_df['Date_Bin'] = merged_df['Date'].dt.floor('10D')
        
        # Group by bin and count sailing and gap days
        binned_df = merged_df.groupby('Date_Bin').agg(
            revenue=('revenue', 'sum'),
            sailing_days=('is_sailing', 'sum'),
            gap_days=('is_gap', 'sum'),
        ).reset_index()

        # New logic to correct for total days > 10
        binned_df['total_days'] = binned_df['sailing_days'] + binned_df['gap_days']
        binned_df['sailing_days'] = np.where(
            binned_df['total_days'] > 10,
            binned_df['sailing_days'] - 1,
            binned_df['sailing_days']
        )
        
        # Create stacked bar chart
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=binned_df['Date_Bin'],
            y=binned_df['sailing_days'],
            name='Sailing Days',
            marker_color='#1f4e79',
            hovertemplate="""
            <b>Date Bin: %{x|%Y-%m-%d}</b><br>
            Sailing Days: %{y}<br>
            Gap Days: %{customdata[0]}<br>
            Total Revenue: $%{customdata[1]:,.2f}
            <extra></extra>
            """,
            customdata=np.stack((binned_df['gap_days'], binned_df['revenue']), axis=-1)
        ))
        
        fig.add_trace(go.Bar(
            x=binned_df['Date_Bin'],
            y=binned_df['gap_days'],
            name='Gap Days',
            marker_color='#d35400',
            hovertemplate="""
            <b>Date Bin: %{x|%Y-%m-%d}</b><br>
            Sailing Days: %{customdata[0]}<br>
            Gap Days: %{y}<br>
            Total Revenue: $%{customdata[1]:,.2f}
            <extra></extra>
            """,
            customdata=np.stack((binned_df['sailing_days'], binned_df['revenue']), axis=-1)
        ))

        fig.update_layout(
            barmode='stack',
            title=f"10-Day Binned Timeline of Sailing and Gaps in {year}",
            xaxis_title="Date Range (10-Day Bins)",
            yaxis_title="Number of Days",
            legend_title="Voyage Status",
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Login Page
# ---------------------------------------------------------------------------------------------------------------------------------------------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except Exception:
        return False

def is_strong_password(password: str) -> bool:
    return (
        len(password) >= 8
        and bool(re.search(r"[A-Z]", password))
        and bool(re.search(r"[a-z]", password))
        and bool(re.search(r"[0-9]", password))
        and bool(re.search(r"[!@#$%^&*()]", password))
    )

# ---------------- User Functions ----------------
def register_user(username, password, email, project_name="VFM"):
    existing = session.table("VESSOPS_D.L00_STG.STREAMLIT_USER_TABLE") \
                      .filter(F.col("USERNAME") == username).collect()
    if existing:
        return False, "❌ Username already exists."

    user_count = session.table("VESSOPS_D.L00_STG.STREAMLIT_USER_TABLE").count()
    user_id = f"user_{user_count + 1:03d}"
    created_at = datetime.utcnow()

    # Insert new user into table
    new_user = session.create_dataframe(
        [(username, hash_password(password), user_id, email, project_name, created_at)],
        schema=["USERNAME", "PASSWORD_HASH", "USER_ID", "EMAIL", "PROJECT_NAME", "CREATED_AT"]
    )
    new_user.write.mode("append").save_as_table("VESSOPS_D.L00_STG.STREAMLIT_USER_TABLE")

    return True, "✅ Registration successful!"

def authenticate_user(username: str, password: str):
    result = session.table("VESSOPS_D.L00_STG.STREAMLIT_USER_TABLE") \
        .filter(F.col("USERNAME") == username).collect()
    
    if not result:
        return False, "❌ Username not found."
    
    user = result[0]
    if verify_password(password, user["PASSWORD_HASH"]):
        return True, user["USER_ID"]
    else:
        return False, "❌ Incorrect password."


logo_url = load_image("logo", return_url=True)
logo_b64 = load_image("NCL Streamlit Landing")

def login_signup_ui():

    if "mode" not in st.session_state:
        st.session_state.mode = "login"
    
    st.markdown(
        f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600&display=swap');
    
            .stApp {{
                background-image: url("data:image/png;base64,{logo_b64}");
                background-size: cover;
                background-position: center;
                background-repeat: no-repeat;
                font-family: 'Poppins', sans-serif;
            }}
            h4 {{
            color: white !important;
        }}
    
            h2 {{
                font-family: 'Poppins', sans-serif;
                text-align: center;
                color: white !important;
                font-size: 2rem;
                letter-spacing: 1px;
                font-weight: 600;
                text-shadow: 1px 1px 4px rgba(0,0,0,0.4);
                margin-top: 0.5rem;
                margin-bottom: 1.5rem;
            }}
    
            .stTextInput input {{
                background-color: black !important;  /* Dark grey */
                color: white !important;
                border: 1px solid #555 !important;
                border-radius: 6px !important;
                padding: 0.5rem !important;
                font-size: 1rem !important;
                caret-color: white !important; /* Blinking white cursor */
            }}
    
            div.stForm button[kind="secondaryFormSubmit"] {{
                background-color: transparent !important;  /* Red button */
                color: white !important;
                border: 1px solid #ffffff !important;
                border-radius: 8px !important;
                padding: 0.6rem 1.2rem !important;
                height: 25px !important;
                font-size: 1rem !important;
                font-weight: 600 !important;
                cursor: pointer !important;
                white-space: nowrap !important;
                }}
                div.stForm button[kind="secondaryFormSubmit"]:hover {{
                    background-color:  rgba(211, 211, 211, 0.2) !important;
                    transform: scale(1.03);
                }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    st.markdown("## &nbsp;", unsafe_allow_html=True)
    
    # ---------------------
    # LOGIN FORM
    # ---------------------
    if st.session_state.mode == "login":
        with st.form("login_form", clear_on_submit=False):
            st.markdown("""
                <style>
                    div.stForm {
                        background-color: rgba(51, 51, 51, 0.5);
                        padding: 20px;
                        border-radius: 10px;
                    }
                </style>
                """, unsafe_allow_html=True)
            
            st.markdown(
                f"""<div style='text-align: center;'>
                        <img src="{logo_url}" style="height:80px;" />
                    </div>""",
                unsafe_allow_html=True
            )
    
            st.markdown("<h2>&nbsp&nbsp&nbsp Voyage Financials</h2>", unsafe_allow_html=True)
    
            st.markdown("#### Username")
            username = st.text_input("Username", label_visibility="collapsed", key="login_username")
    
            st.markdown("#### Password")
            password = st.text_input("Password", type="password", label_visibility="collapsed", key="login_password")
    
            st.markdown("") 
    
            col_login_left, col_login_center, col_login_right = st.columns([3, 1, 3])
            with col_login_center:
                login_submitted = st.form_submit_button("Login", help="Click to log in")
    
            st.markdown("---")
    
            col_signup_text_left, col_signup_text_center, col_signup_text_right = st.columns([0.7, 1, 0.5])
            with col_signup_text_center:
                st.markdown("#### Don't have an account?")
    
            col_signup_btn_left, col_signup_btn_center, col_signup_btn_right = st.columns([1.7, 1, 1.7])
            with col_signup_btn_center:
                go_to_signup_submitted = st.form_submit_button("Go to Sign Up", help="Click to create a new account")
    
            if login_submitted:
                if not username or not password:
                    st.error("Please enter both username and password")
                else:
                    success, message = authenticate_user(username, password)
                    if success:
                        st.success("Login successful! Redirecting...")
                        st.session_state.logged_in = True
                        st.session_state.page = 'landing_page'
                        st.session_state.user_id = message
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(message)
            elif go_to_signup_submitted:
                st.session_state.mode = "signup"
                st.rerun()
    
    # ---------------------
    # SIGN UP FORM
    else:
        with st.form("signup_form", clear_on_submit=False):
            st.markdown("""
                <style>
                    div.stForm {
                        background-color: rgba(51, 51, 51, 0.5);
                        padding: 20px;
                        border-radius: 10px;
                    }
                </style>
                """, unsafe_allow_html=True)
    
            st.markdown(
                f"""<div style='text-align: center;'>
                        <img src="{logo_url}" style="height:80px;" />
                    </div>""",
                unsafe_allow_html=True
            )
    
            st.markdown("<h2>&nbsp&nbsp&nbsp Voyage Financials</h2>", unsafe_allow_html=True)
    
            st.markdown("#### Enter a Username")
            username = st.text_input("New Username", label_visibility="collapsed", key="signup_username")

            st.markdown("#### Enter a Email ID")
            email = st.text_input("Your Email", label_visibility="collapsed", key="signup_mail")
    
            st.markdown("#### Enter a Strong Password")
            password = st.text_input("New Password", type="password", label_visibility="collapsed", key="signup_password")
    
            st.markdown("#### Confirm Password")
            confirm = st.text_input("Confirm Password", type="password", label_visibility="collapsed", key="signup_confirm_password")
    
            st.markdown("")
    
            col_signup_submit_left, col_signup_submit_center, col_signup_submit_right = st.columns([3, 1.3, 3])
            with col_signup_submit_center:
                signup_submitted = st.form_submit_button("Sign Up", help="Click to create your account")
    
            st.markdown("---")
    
            col_login_text_left, col_login_text_center, col_login_text_right = st.columns([0.6, 1, 0.4])
            with col_login_text_center:
                st.markdown("#### Already have an account?")
    
            col_login_btn_left, col_login_btn_center, col_login_btn_right = st.columns([1, 1, 0.5])
            with col_login_btn_center:
                go_to_login_submitted = st.form_submit_button("Go to Login", help="Click to go back to login")
    
            if signup_submitted:
                if not username or not password or not confirm or not email:
                    st.error("⚠️ All fields are required.")
                elif password != confirm:
                    st.error("⚠️ Passwords do not match.")
                elif not is_strong_password(password):
                    st.error("⚠️ Password must be at least 8 chars and include upper, lower, number, and special character.")
                else:
                    success, msg = register_user(username, password, email)
                    if success:
                        st.success(msg)
                        time.sleep(1)
                        st.session_state.mode = "login"
                        st.rerun()
                    else:
                        # 👇 Show duplicate username message here
                        st.error(msg)
            elif go_to_login_submitted:
                st.session_state.mode = "login"
                st.rerun()

    
#------------------------------------------------------------------------------------------------------------------------------------
# LANDING PAGE
#------------------------------------------------------------------------------------------------------------------------------------

def landing_page():  
    st.markdown(
    """
    <style>
        /* Hide Streamlit header and make it 0 height */
        header[data-testid="stHeader"] {
            height: 0rem;
            visibility: hidden;
        }

        /* Optional: remove extra padding at the top */
        div.block-container {
            padding-top: 0rem;
        }
    </style>
    """,
    unsafe_allow_html=True
)
    df = load_data()
    load_hero = load_image("herosection", return_url=True)
    st.markdown(f"""
		<style>
			div.stForm {{
		background-color: rgba(0, 0, 0, 0) !important; /* fully transparent */
		padding: 18px !important;
		border-radius: 10px !important;
		border: 0.5px solid white !important;
		flex-wrap: wrap;/* thin white border */
	}}
			div.stForm button[kind="secondaryFormSubmit"] {{
			background-color: transparent !important;  /* Red button */
			color: white !important;
			border: 1px solid #ffffff !important;
			border-radius: 8px !important;
			padding: 0.6rem 1.2rem !important;
			height: 25px !important;
			font-size: 1rem !important;
			font-weight: 600 !important;
			cursor: pointer !important;
			white-space: nowrap !important;
			}}
			div.stForm button[kind="secondaryFormSubmit"]:hover {{
				background-color:  rgba(211, 211, 211, 0.2) !important;
				transform: scale(1.03);
			}}
			.stTextInput input::placeholder {{
		color: rgba(255, 255, 255, 0.7) !important; 
		opacity: 1 !important;  /* Some browsers require this */
	}}
		  /* Button Styling */
			div.stButton > button {{
				background-color: #007acc !important;
				color: white !important;
				border: none !important;
				border-radius: 8px !important;
				padding: 0.6rem 1.2rem !important;
				font-size: 1rem !important;
				font-weight: 600 !important;
				cursor: pointer !important;
			}}
			
					
			.hero-left {{
			padding: 4rem 3rem;
			border-top-left-radius: 0.75rem;
			border-bottom-left-radius: 0.75rem;
			flex: 1 1 50%;
			min-width: 300px;
			}}

			.hero-title {{
			font-size: 2.5rem;
			font-weight: 800;
			color: white;
			margin-bottom: 1rem;
			line-height: 1.2;
			}}

			.hero-title span {{ color: #3B82F6; }}

			.hero-subtext {{
				font-size: 1.1rem; margin-bottom: 2rem; color: #9CA3AF; line-height: 1.6;
			}}

					/* Dark grey text input */
			.stTextInput input, .stTextArea textarea {{
				background-color:  #3a3a3a !important;  /* Dark grey */
				color: white !important;
				border: none !important;
				border-radius: 6px !important;
				padding: 0.5rem !important;
				font-size: 1rem !important;
				caret-color: white !important; /* Blinking white cursor */
			}}
	
			/* Glow effect on focus */
			.stTextInput input:focus, .stTextArea textarea:focus {{
				border: none !important;  /* White border on focus */
				box-shadow: 0 0 6px rgba(255,255,255,0.5) !important;
				outline: none !important;
			}}

			.hero-right {{
			min-height: 380px;
			background: url('{load_hero}') center / cover no-repeat;
			border-top-right-radius: 0.75rem;
			border-bottom-right-radius: 0.75rem;
			-webkit-mask-image: linear-gradient(to right, transparent 0%, black 25%, black 100%);
			mask-image: linear-gradient(to right, transparent 0%, black 25%, black 100%);
			flex: 1 1 50%;
			min-width: 300px;
			}}

			.stats-wrapper {{
				display: flex; flex-wrap: wrap; gap: 2rem; justify-content: center;
				margin: 0 auto 3rem; padding: 0 2rem; max-width: 1200px;
			}}

			.stats-card {{
				flex: 1 1 300px;min-width: 250px; max-width: 400px; background: rgba(58, 134, 255, 0.1);
				border-left: 4px solid #3B82F6; padding: 2rem 1.5rem;
				border-radius: 12px; box-shadow: 0 8px 20px rgba(0, 0, 0, 0.2);
				color: white; text-align: left; transition: all 0.3s ease;
			}}

			.stats-card:hover {{
				transform: translateY(-5px); box-shadow: 0 12px 25px rgba(0, 0, 0, 0.3);
			}}
			/* Responsive adjustments */
		@media (max-width: 768px) {{
			.hero-left {{
				padding: 2rem 1.5rem;
			}}
			.hero-title {{
				font-size: 1.8rem;
			}}
			.hero-subtext {{
				font-size: 0.9rem;
			}}
			.hero-right {{
				min-height: 250px;
				-webkit-mask-image: linear-gradient(to right, transparent 0%, transparent 5%, black 20%, black 100%);
				mask-image: linear-gradient(to right, transparent 0%, transparent 5%, black 20%, black 100%);
			}}
		}}
		@media (max-width: 600px) {{
			.hero-right {{
				display: none;
			}}
			
		</style>
	""", unsafe_allow_html=True)
	# ----------------------------
	# Streamlit Navbar (Logo + Buttons)
	# ----------------------------
	
	# Add a wrapper div for the border and custom styling
    st.markdown('<div class="navbar-container">', unsafe_allow_html=True)
    with st.form("header_form"):
        logo_col, nav_col = st.columns([2, 3])
		
		# --- Left: Logo + Title --
        with logo_col:
            logo_url = load_image("logo", return_url=True)
            st.markdown(f"""
				<div class="logo-container">
					<img src="{logo_url}" width="100" height="55">
					<div class="title-text">Voyage Financials</div>
				</div>
			""", unsafe_allow_html=True)
	
		# --- Right: Navigation Links and Logout Button ---
        with nav_col:
            links_col, button_col = st.columns([5, 1]) 
            
            with links_col:
				# Group links in a single markdown with a flex container for right-alignment
                st.markdown("""
                <div class="nav-links-container">
                    <a href="https://prod-useast-b.online.tableau.com/#/site/nclh/views/NCLVoyageProfitability-PCDPPD/PRODUCTDETAIL?:iid=1" target="_blank" class="nav-btn">Dashboard</a>
                    <a href="https://nclcorp-my.sharepoint.com/personal/mpeacock_nclcorp_com/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmpeacock%5Fnclcorp%5Fcom%2FDocuments%2FMitch%20Files%2FMitch%20Initiatives%2FVessel%20Operations%20Analytics%2FVoyage%20Profitability%2FDeliverables%2FDocumentation&ga=1" target="_blank" class="nav-btn">Documentation</a>
                    <a href="https://nclcorp-my.sharepoint.com/personal/mpeacock_nclcorp_com/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmpeacock%5Fnclcorp%5Fcom%2FDocuments%2FMitch%20Files%2FMitch%20Initiatives%2FVessel%20Operations%20Analytics%2FVoyage%20Profitability%2FDeliverables%2FDocumentation&ga=1" target="_blank" class="nav-btn">Guide</a>
                </div>
                """, unsafe_allow_html=True)
				
            with button_col:
                # The native Streamlit logout button
                logout = st.form_submit_button("Logout")
                if logout:
                    st.session_state.logged_in = False
                    st.session_state.page = 'landing_page'
                    st.session_state.pop('user_id', None)
                    st.rerun()
	
	# Close the wrapper div
    st.markdown('</div>', unsafe_allow_html=True)
	
	
	# --- Global Styling ---
    st.markdown("""
    <style>
    
        /* Remove the default padding from Streamlit's form element */
        div[data-testid="stForm"] > form {
            padding: 0;
        }
    
        /* Logo and Title styling */
        .logo-container {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 1rem;
        }
        .title-text {
            font-size: 32px;
            font-weight: bold;
            color: white;
            font-family: 'Segoe UI', sans-serif;
        }
    
        /* Container for the navigation links */
        .nav-links-container {
            display: flex;
            align-items: center;
            justify-content: flex-end; /* Aligns links to the right */
            gap: 12px;
            height: 100%;
        }
    
        /* Styling for the <a> tag buttons */
        .nav-btn {
            background-color: transparent;
            color: #ADB5BD;
            border: 1px solid #495057;
            border-radius: 8px;
            font-family: 'Segoe UI', sans-serif;
            text-decoration: none;
            display: flex;
            align-items: center;
            justify-content: center;
            width: 130px;
            height: 44px; /* Matched to the Streamlit button height */
            transition: all 0.3s ease;
        }
        .nav-btn:hover {
            background-color: #343A40;
            color: white !important;
            border-color: #6C757D;
            text-decoration: none !important;
        }
        a.nav-btn:visited, a.nav-btn:active, a.nav-btn:link {
            color: #ADB5BD;
            text-decoration: none !important;
        }
        
        /* STYLING FOR THE NATIVE STREAMLIT LOGOUT BUTTON */
        div[data-testid="stFormSubmitButton"] button {
            background-color: #007acc;
            color: white;
            border: 1px solid #007acc;
            border-radius: 8px;
            width: 130px;
            font-family: 'Segoe UI', sans-serif;
            font-weight: normal; /* Override Streamlit's default bold */
        }
        div[data-testid="stFormSubmitButton"] button:hover {
            background-color: #0056b3;
            color: white;
            border-color: #0056b3;
        }
    </style>
    """, unsafe_allow_html=True)
	# ----------------------------
	# Hero Section
	# ----------------------------
    col1, col2 = st.columns([5, 5])
    
    with col1:
        st.markdown('<div class="hero-left">', unsafe_allow_html=True)
        st.markdown('<div class="hero-title">Transform Prompts into<span> Financial Insights</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hero-subtext"><br>This tool includes NCL data from 2023 through 2025,excluding Charter and Common Operating Units.</div>', unsafe_allow_html=True)
    
        user_prompt = st.text_input(
            "What would you like to know?",
            placeholder="e.g., Show product ranking by PCD and GSS asc",
            label_visibility="collapsed",
            key="landing_page_query_input"
        )
    
        # Trigger only if a new query is entered (i.e., Enter is pressed and it's different)
        if user_prompt.strip() and st.session_state.get("last_query_entered") != user_prompt:
            st.session_state.last_query_entered = user_prompt
            st.session_state.current_query = user_prompt
            st.session_state.page = 'query_results_page'
    
            # --- Initial Routing Logic ---
            corrected_query = correct_query(user_prompt)
            query_type_raw = detect_query_type(corrected_query)
            query_type_nlp = query_type_raw.upper() if query_type_raw else "UNKNOWN"
    
            ships_q, products_q, years_q, months_q, voyages_q, SHIP_CLASSs_q,component = extract_filters_from_query(corrected_query, df)
    
            order_column,order_type = extract_orders_from_query(corrected_query)
    
            st.session_state.selected_order_column = order_column
            st.session_state.selected_order_type = order_type
    
            # Determine initial metric based on NLP
            if query_type_nlp in metric_groups and metric_groups[query_type_nlp]:
                st.session_state.selected_primary_metric_sidebar = (
                    "Per Capacity Day (Margin PCD)" if query_type_nlp == "PCD"
                    else "Per Passenger Day (Margin PPD)" if query_type_nlp == "PPD"
                    else "Margin $"
                )
    
            # Determine initial flow based on query keywords
            query_lower = corrected_query.lower()
            if any(p_keyword in query_lower for p_keyword in PRODUCT_KEYWORDS):
                st.session_state.initial_flow_choice = "Product-Centric"
            elif any(s_keyword in query_lower for s_keyword in SHIP_KEYWORDS):
                st.session_state.initial_flow_choice = "Ship-Centric"
            elif any(v_keyword in query_lower for v_keyword in VOYAGE_KEYWORDS):
                st.session_state.initial_flow_choice = "Voyage-Centric"
            elif "outlier" in query_lower: # Direct to Outlier Analysis
                st.session_state.initial_flow_choice = "Outlier Analysis"
            elif any(d_keyword in query_lower for d_keyword in DEPLOYMENT_KEYWORDS):
                st.session_state.initial_flow_choice = "Deployment Analysis"
            else:
                st.session_state.initial_flow_choice = "General Overview"
    
            # Pre-populate sidebar filters based on query
            st.session_state.current_filters['Year'] = [int(y) for y in years_q] if years_q else []
            st.session_state.current_filters['Month'] = [int(m) for m in months_q] if months_q else []
            st.session_state.current_filters['RM_ROLLUP_PRODUCT_DESC'] = products_q
            st.session_state.current_filters['Ship Class'] = SHIP_CLASSs_q
            st.session_state.current_filters['Ship'] = ships_q
            st.session_state.current_filters['M0_AND_M1'] = component
    
            # Debug output (you can comment these out in production)
            st.write(f"DEBUG: Corrected Query: '{corrected_query}'")
            st.write(f"DEBUG: Detected Initial Flow Choice: '{st.session_state.initial_flow_choice}'")
    
            st.rerun()
    
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="hero-right"></div>', unsafe_allow_html=True)
        # ------------------------
    # ------------------------
    # CSS Styling
    # ------------------------
    st.markdown("""
		<style>
		body, .stApp {
			background-color: #0d1117;
			color: #e6e6e6;
		}
		.stats-wrapper {
			display: flex; gap: 2rem; justify-content: center;
			margin: 0 auto 3rem; padding: 0 2rem; max-width: 1200px;
		}
		div.stButton > button {
				background-color: #007acc !important;
				color: white !important;
				border: none !important;
				border-radius: 8px !important;
				padding: 0.6rem 1.2rem !important;
				font-size: 1rem !important;
				font-weight: 600 !important;
				cursor: pointer !important;
			}
			
			div.stButton > button:hover {
				background-color: #2563EB !important;  /* Tailwind's blue-600 */
				}
		.stats-card {
			flex: 1;
			background: rgba(58, 134, 255, 0.1);
			border-left: 4px solid #3B82F6;
			padding: 2rem 1.5rem;
			border-radius: 12px;
			box-shadow: 0 8px 20px rgba(0, 0, 0, 0.2);
			color: white;
			text-align: center;
			transition: all 0.3s ease;
		}
		.stats-card:hover {
			transform: translateY(-5px); box-shadow: 0 12px 25px rgba(0, 0, 0, 0.3);
		}
		.custom-card {
			padding: 1.5rem;
			border-radius: 0.75rem;
			margin-bottom: 1rem;
			display: flex;
			flex-direction: column;
			height: 280px;
			background-color: #161b22;
		}
		.card-header {
			display: flex;
			align-items: center;
			gap:  8px;
			margin-bottom: 0.75rem;
		}
		.icon-container {
			width: 2.5rem;
			height: 2.5rem;
			border-radius: 9999px;
			display: flex;
			align-items: center;
			justify-content: center;
			flex-shrink: 0;
			background-color: rgba(255, 255, 255, 0.1);
		}
		.product-title {
			font-weight: 450;
			color: black;
			margin-bottom: 0;
			font-size: 1rem;
		}
		.product-subtitle {
            font-size: 0.95rem !important;
            color: #9ca3af !important;
        }
		.card-body {
			flex-grow: 1;
			display: flex;
			flex-direction: column;
			justify-content: space-between;
		}
		.metric-row {
			display: flex;
			justify-content: space-between;
			font-size: 0.875rem;
			margin-bottom: 0.5rem;
		}
		.metric-label {
			color: #9ca3af;
		}
            div[data-testid="stCheckbox"] label p {
            color: white !important;
        }
				</style>
	""", unsafe_allow_html=True)
    left_col,samcol, center_col, right_col = st.columns([0.8,0.2, 3, 1])
    
    # --- Place the checkboxes in the left column ---
    with left_col:
        years_to_display = [2023, 2024, 2025]
        selected_years = []
        st.markdown("<p style='font-size:14px; color:white;'>Select Year</p>", unsafe_allow_html=True)
    
        # Use nested columns to keep the checkboxes close
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.checkbox(str(years_to_display[0]), value=True, key='2023_key'):
                selected_years.append(years_to_display[0])
        with col2:
            if st.checkbox(str(years_to_display[1]), value=True, key='2024_key'):
                selected_years.append(years_to_display[1])
        with col3:
            if st.checkbox(str(years_to_display[2]), value=True, key='2025_key'):
                selected_years.append(years_to_display[2])
        
        # --- Filtering Logic ---
        if not selected_years:
            st.warning("Please select at least one year.")
            filtered_df = pd.DataFrame(columns=df.columns)
        else:
            filtered_df = df[df['FISCAL_YEAR'].isin(selected_years)].copy()
    
    
    # --- Place "Key Metrics" title in the center column ---
    with center_col:
        st.markdown(f"""
            <h2 style="text-align:center; color:white; margin-top: 0rem;">Key Metrics</h2>
        """, unsafe_allow_html=True)
    
    # The rest of your code can now use filtered_df
    active_voyages = filtered_df["VOYAGE_ID"].nunique()
    product_count = filtered_df["RM_ROLLUP_PRODUCT_DESC"].nunique()
    
    def calculate_ytd_growth(df):
        df["Type"] = df["M0_AND_M1"].apply(
        lambda x: "Revenue" if x in all_Revenue_components else "Cost"
        )
        ytd_months = [1,2,3,4,5,6,7,8,9,10,11,12]  # Jan–Mar
        current_year = 2024
        previous_year = 2023
    
        rev_current = df[
            (df["FISCAL_YEAR"] == current_year) &
            (df["ACCOUNTING_PERIOD"].isin(ytd_months)) 
        ]["COMPONENT_AMOUNT"].sum()
    
        rev_previous = df[
            (df["FISCAL_YEAR"] == previous_year) &
            (df["ACCOUNTING_PERIOD"].isin(ytd_months)) 
        ]["COMPONENT_AMOUNT"].sum()
    
        ytd_growth = (((rev_current - rev_previous) / rev_previous) * 100) if rev_previous != 0 else 0
        return round(ytd_growth, 1)
    
    ytd_growth = calculate_ytd_growth(df)
    
    filtered_df["Type"] = filtered_df["M0_AND_M1"].apply(
        lambda x: "Revenue" if x in all_Revenue_components else "Cost"
        )
    total_revenue = filtered_df[filtered_df["Type"] == "Revenue"]["COMPONENT_AMOUNT"].sum()
    total_cost = filtered_df[filtered_df["Type"] == "Cost"]["COMPONENT_AMOUNT"].sum()
    margin = total_revenue - abs(total_cost)
    # ------------------------
    # Metrics Display
    # ------------------------
    st.markdown(f"""
		<div class="stats-wrapper">
			<div class="stats-card">
				<div style="font-size:1.75rem;margin-bottom:1rem;">🚢</div>
				<div style="font-size:1.8rem;font-weight:700;margin-bottom:0.5rem;color:#3B82F6;">{active_voyages}</div>
				<div style="color:rgba(156,163,175,0.8);">Active Voyages</div>
			</div>
			<div class="stats-card">
				<div style="font-size:1.75rem;margin-bottom:1rem;">📈</div>
				<div style="font-size:1.8rem;font-weight:700;margin-bottom:0.5rem;color:#10B981;">{ytd_growth:.1f}%</div>
				<div style="color:rgba(156,163,175,0.8);">YOY Growth</div>
			</div>
			<div class="stats-card">
				<div style="font-size:1.75rem;margin-bottom:1rem;">💰</div>
				<div style="font-size:1.8rem;font-weight:700;margin-bottom:0.5rem;color:#8B5CF6;">${margin/1e6:.1f}M</div>
				<div style="color:rgba(156,163,175,0.8);">Net Margin</div>
			</div>
			<div class="stats-card">
				<div style="font-size:1.75rem;margin-bottom:1rem;">🧭</div>
				<div style="font-size:1.8rem;font-weight:700;margin-bottom:0.5rem;color:#F59E0B;">{product_count}</div>
				<div style="color:rgba(156,163,175,0.8);">Number of Products</div>
			</div>
		</div>
	""", unsafe_allow_html=True)

	# ------------------------
	# Product Aggregation
	# ------------------------
    def get_product_margin_data(df):
        """
        Returns all products aggregated with Revenue, Cost, Margin, and VF Percentage in millions.
        Uses M0_AND_M1 to classify as Revenue or Cost and COMPONENT_AMOUNT as the value.
        """
        df["Type"] = df["M0_AND_M1"].apply(
            lambda x: "Revenue" if x in all_Revenue_components else "Cost"
        )
    
        df_agg = df.groupby("RM_ROLLUP_PRODUCT_DESC").agg(
            Revenue=("COMPONENT_AMOUNT", lambda x: x[df.loc[x.index, "Type"] == "Revenue"].sum()),
            Cost=("COMPONENT_AMOUNT", lambda x: x[df.loc[x.index, "Type"] == "Cost"].sum()),
            Voyages=("VOYAGE_ID", "nunique"),
            Ships=("SHIP_CD", "nunique")
        ).reset_index()
    
        # Convert to millions
        df_agg["Revenue"] = df_agg["Revenue"] / 1e6
        df_agg["Cost"] = abs(df_agg["Cost"]) / 1e6
        df_agg["Margin"] = df_agg["Revenue"] - df_agg["Cost"]
    
        # VF Percentage
        df_agg["VF_Percentage"] = df_agg.apply(
            lambda row: (row["Margin"] / row["Revenue"] * 100) if row["Revenue"] != 0 else 0,
            axis=1
        )
    
        return df_agg
    
    df_agg = get_product_margin_data(filtered_df)

	# ------------------------
	# Top Product Cards
	# ------------------------
	# Inject CSS for white radio label & options
	# Inject CSS for white radio label & options (force override)
    st.markdown("""
        <style>
        /* Radio group label ("Select View") */
        .stRadio > label {
            color: white !important;
            font-weight: bold !important;
            font-size: 16px !important;
        }
        /* Radio option text (deep target) */
        div[role="radiogroup"] > div {
            color: white !important;
        }
        div[role="radiogroup"] label p {
            color: white !important;
            font-size: 16px !important;
            font-weight: 500 !important;
        }
        </style>
    """, unsafe_allow_html=True)
	
    # Title
    st.markdown("<h2 style='text-align:center; color:white;'>Product Performance Overview</h2>", unsafe_allow_html=True)
    
    # Radio
    performance_choice = st.radio(
        "Select View (Sorted by VF Margin %)",
        options=["Top Performing Products", "Bottom Performing Products"],
        horizontal=True,
        index=0
    )
    
    # Choose top or bottom 5 based on toggle
    if performance_choice == "Top Performing Products":
        df_display = df_agg.sort_values(by="VF_Percentage", ascending=False).head(5).reset_index(drop=True)
    else:
        df_display = df_agg.sort_values(by="VF_Percentage", ascending=True).head(5).reset_index(drop=True)
        
    # Generate the KPI cards
    cols = st.columns(min(5, len(df_display)))
    for i, (col, product) in enumerate(zip(cols, df_display.to_dict("records"))):
        region = product['RM_ROLLUP_PRODUCT_DESC']
        region_query = f"Show product {region}"
        region_emoji = "🌊"
    
        # Pick color based on index
        border_color = "#3B82F6"  # Loop over if more than 5 
        with col:
            with st.form(f"product_form_{i}"):
                # Determine margin color
                margin_color = "#10B981" if product['Margin'] >= 0 else "#EF4444"
        
                # Round values
                revenue_m = round(product['Revenue'], 2)
                cost_m = round(product['Cost'], 2)
                margin_m = round(product['Margin'], 2)
                margin_pct = round((margin_m / revenue_m * 100), 2) if revenue_m != 0 else 0
                
                margin_color = "#10B981" if margin_m >= 0 else "#EF4444"
                
                card_html = f"""
                        <div class="custom-card" style="border-left: 4px solid {border_color};">
                            <div class="card-header">
                                <div class="icon-container" style="background-color: {border_color}33;">
                                    <div style="font-size: 1.25rem;">{region_emoji}</div>
                                </div>
                                <div>
                                    <h4 class="product-title" style="font-size: 1.075rem; color: #e6e6e6; font-weight: 600; margin: 0;">
                                        {region}
                                    </h4>
                                    <p class="product-subtitle">{product['Voyages']} Voyages | {product['Ships']} Ships</p>
                                </div>
                            </div>
                            <div class="card-body">
                                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                    <div style="background-color: #10B98133; padding: 0.5rem 1rem; border-radius: 8px;text-align: center;">
                                        <strong style="color: #10B981;">Revenue</strong><br>
                                        <span style="color: white;">${revenue_m:.2f}M</span>
                                    </div>
                                    <div style="background-color: #7f1d1d33; padding: 0.5rem 1rem; border-radius: 8px; text-align: center;">
                                        <strong style="color: #b91c1c;">Cost</strong><br>
                                        <span style="color: white;">${cost_m:.2f}M</span>
                                    </div>
                                </div>
                                <div style="text-align: center; font-weight: 600; color: {margin_color}; margin-top: 0.75rem;">
                                    VF Margin: ${margin_m:.2f}M 
                                </div>
                                <div style="text-align: center; font-weight: 600; color: {margin_color};margin-bottom: 0.75rem;">
                                    VF Margin %: {margin_pct:.2f}%
                                </div>
                            </div>
                        </div> """
                    
                st.markdown(card_html, unsafe_allow_html=True)
        
                # Centered Explore button
                btn_left, btn_center, btn_right = st.columns([1, 2, 1])
                with btn_center:
                    if st.form_submit_button("Explore →",use_container_width=True):
                        process_query_and_navigate(region_query, df)
    
    st.markdown("<h2 style='text-align:center; color:white;'>Ship Performance Overview</h2>", unsafe_allow_html=True)
    def get_ships_by_margin(df):
        df["Type"] = df["M0_AND_M1"].apply(
        lambda x: "Revenue" if x in all_Revenue_components else "Cost"
        )
        df_agg = df.groupby("SHIP_CD").agg(
            Revenue=("COMPONENT_AMOUNT", lambda x: x[df.loc[x.index, "Type"] == "Revenue"].sum()),
            Cost=("COMPONENT_AMOUNT", lambda x: x[df.loc[x.index, "Type"] == "Cost"].sum()),
            Voyages=("VOYAGE_ID", "nunique"),
            Products=("RM_ROLLUP_PRODUCT_DESC", "nunique")
        ).reset_index()
        
        df_agg["Margin"] = df_agg["Revenue"] - abs(df_agg["Cost"])
        df_agg["Revenue"] = df_agg["Revenue"] / 1e6
        df_agg["Cost"] = df_agg["Cost"] / 1e6
        df_agg["Margin"] = df_agg["Margin"] / 1e6

        # VF Percentage
        df_agg["VF_Percentage"] = df_agg.apply(
            lambda row: (row["Margin"] / row["Revenue"] * 100) if row["Revenue"] != 0 else 0,
            axis=1
        )
        
        return df_agg
            
    df_ships = get_ships_by_margin(filtered_df)
    # Top/Bottom Toggle
    ship_choice = st.radio(
        "Select View (Sorted by VF Margin %)",
        options=["Top Performing Ships", "Bottom Performing Ships"],
        horizontal=True,
        index=0,
        format_func=lambda x: " " + x if "Top" in x else " " + x
    )
    
    if ship_choice == "Top Performing Ships":
        df_display = df_ships.sort_values(by="VF_Percentage", ascending=False).head(5).reset_index(drop=True)
    else:
        df_display = df_ships.sort_values(by="VF_Percentage", ascending=True).head(5).reset_index(drop=True)
    
    # Display cards
    cols = st.columns(min(5, len(df_display)))
    for i, col in enumerate(cols):
        ship = df_display.iloc[i]
        ship_code = ship["SHIP_CD"]
        border_color = "#3B82F6"
        margin_color = "#10B981" if ship["Margin"] >= 0 else "#EF4444"
        query = f"Show ship {ship_code}"
        
        with col:
            with st.form(f"ship_form_{i}"):
                card_html = f"""
                <div class="custom-card" style="border-left: 4px solid {border_color}; min-height: 280px;">
                    <div class="card-header">
                        <div class="icon-container" style="background-color: {border_color}33;">
                            <div style="font-size: 1.25rem;">🚢</div>
                        </div>
                        <div>
                            <h4 class="product-title" style="font-size: 1.150rem; color: #e6e6e6; font-weight: 600; margin: 0;">
                                {ship_code}
                            </h4>
                            <p class="product-subtitle">{ship['Voyages']} Voyages | {ship['Products']} Products</p>
                        </div>
                    </div>
                    <div class="card-body">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                            <div style="background-color: #10B98133; padding: 0.5rem 1rem; border-radius: 8px; text-align: center;">
                                <strong style="color: #10B981;">Revenue</strong><br>
                                <span style="color: white;">${ship['Revenue']:.2f}M</span>
                            </div>
                            <div style="background-color: #7f1d1d33; padding: 0.5rem 1rem; border-radius: 8px; text-align: center;">
                                <strong style="color: #b91c1c;">Cost</strong><br>
                                <span style="color: white;">${abs(ship['Cost']):.2f}M</span>
                            </div>
                        </div>
                        <div style="text-align: center; font-weight: 600; color: {margin_color}; margin-top: 0.75rem;">
                            VF Margin: ${ship['Margin']:.2f}M 
                        </div>
                        <div style="text-align: center; font-weight: 600; color: {margin_color};margin-bottom: 0.75rem;">
                                    VF Margin %: {ship['VF_Percentage']:.2f}%
                        </div>
                    </div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)
    
                # Centered Explore button
                btn_left, btn_center, btn_right = st.columns([1, 2, 1])
                with btn_center:
                    if st.form_submit_button("Explore →",use_container_width=True):
                        process_query_and_navigate(query, df)
    
    img1 =  load_image("herosection", return_url=True)
    img2 =  load_image("key_insights3", return_url=True)
    img3 =  load_image("key_insights2", return_url=True)
    img4 =   load_image("key_insights4", return_url=True)
        
	
	# Insight definitions and their queries
    insights = [
		{"img": img1, "title": "Itinerary Insights", "desc": "**Europe's** Itinerary Route of **CIV–CIV** Roundtrip Nets **$87M** Margin with **13 Voyages and 3 ships**.", "query": "Show itinerary components Europe"},
		{"img": img2, "title": "Outlier Insights", "desc": "Identify unusual performance patterns for products or ships based on selected components", "query":  "Outlier Analysis"},
		{"img": img3, "title": "Deployment Insights", "desc": "JAD mirrored JWL, but JOY’s exceptional yield in 2025 drove a major margin improvement for Alaska.", "query": "deployment alaska"},
		{"img": img4, "title": "Deployment Insights", "desc": "AME’s 2025 margin drop is driven by its dry-dock downtime.", "query": "deployment ame"}
	]
	
    def render_insight_cards():
        st.markdown("<h3 style='text-align:center; color:white;'>Key Insights</h3>", unsafe_allow_html=True)
		
        cols = st.columns(4)
		
        for idx, col in enumerate(cols):
            insight = insights[idx]
            with col:
                with st.form(f"form_{idx}"):
					# Card container with consistent styling
                    with st.container():
						# Image with fixed aspect ratio container
                        st.markdown(
							f"""
							<div style="
								height: 180px;
								overflow: hidden;
								display: flex;
								align-items: center;
								justify-content: center;
								margin-bottom: 12px;
								border-radius: 8px;
							">
								<img src="{insight['img']}" style="
									width: 100%;
									height: 100%;
									object-fit: cover;
								">
							</div>
							""",
							unsafe_allow_html=True
						)
						
						# Title
                        title = insight["title"] if insight["title"] else "More Insights"
                        st.markdown(f"**{title}**")
						
						# Description
                        if insight["desc"]:
                            st.caption(insight["desc"])
                        else:
                            st.write("\n")  # First empty line
                            st.caption("Coming soon") 
							
							# Placeholder text
							 # Second empty line
				
				# Centered form submit button using columns
                        col1, col2, col3 = st.columns([1, 2, 1])
                        with col2:
                            if st.form_submit_button("Explore →", use_container_width=True):
								# Existing rerun logic
                                query = insight["query"]
                                st.session_state.current_query = query
                                st.session_state.page = 'query_results_page'
		
                                corrected_query = correct_query(query)
                                query_type_raw = detect_query_type(corrected_query)
                                query_type_nlp = query_type_raw.upper() if query_type_raw else "UNKNOWN"
                                		
                                ships_q, products_q, years_q, months_q, voyages_q, SHIP_CLASSs_q,component = extract_filters_from_query(corrected_query, df)
                                
                                order_column,order_type = extract_orders_from_query(corrected_query)
                                
                                st.session_state.selected_order_column = order_column
                                st.session_state.selected_order_type = order_type
								
                                if query_type_nlp in metric_groups and metric_groups[query_type_nlp]:
                                    st.session_state.selected_primary_metric_sidebar = (
                                        "Per Capacity Day (Margin PCD)" if query_type_nlp == "PCD"
                                        else "Per Passenger Day (Margin PPD)" if query_type_nlp == "PPD"
                                        else "Margin $"
                                    )
		
                                query_lower = corrected_query.lower()
                                if any(p_keyword in query_lower for p_keyword in PRODUCT_KEYWORDS):
                                    st.session_state.initial_flow_choice = "Product-Centric"
                                elif any(s_keyword in query_lower for s_keyword in SHIP_KEYWORDS):
                                    st.session_state.initial_flow_choice = "Ship-Centric"
                                elif any(v_keyword in query_lower for v_keyword in VOYAGE_KEYWORDS):
                                    st.session_state.initial_flow_choice = "Voyage-Centric"
                                elif "outlier" in query_lower:
                                    st.session_state.initial_flow_choice = "Outlier Analysis"
                                elif any(d_keyword in query_lower for d_keyword in DEPLOYMENT_KEYWORDS):
                                    st.session_state.initial_flow_choice = "Deployment Analysis"
                                else:
                                    st.session_state.initial_flow_choice = "General Overview"
                                
                                # Pre-populate sidebar filters based on query
                                st.session_state.current_filters['Year'] = [int(y) for y in years_q] if years_q else []
                                st.session_state.current_filters['Month'] = [int(m) for m in months_q] if months_q else []
                                st.session_state.current_filters['RM_ROLLUP_PRODUCT_DESC'] = products_q
                                st.session_state.current_filters['Ship Class'] = SHIP_CLASSs_q
                                st.session_state.current_filters['Ship'] = ships_q
                                st.session_state.current_filters['M0_AND_M1'] = component

                                # Debug output (you can comment these out in production)
                                st.write(f"DEBUG: Corrected Query: '{corrected_query}'")
                                st.write(f"DEBUG: Detected Initial Flow Choice: '{st.session_state.initial_flow_choice}'")
                                
                                st.rerun()


    # Render Insight Cards on Page
    render_insight_cards()
        

    # Centered sleek footer that appears only when user scrolls to the end of the page
    st.markdown(
        """
        <footer style="
            background: #111827; 
            padding: 8px 16px; 
            width: 100%; 
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin-top: 50px; 
            border-top: 1px solid #3B82F6;
            box-shadow: 0 -2px 8px rgba(0,0,0,0.25);
            border-radius: 6px 6px 0 0;
            transition: all 0.4s ease;
        ">
            <span style="font-weight: 500; font-size: 0.99rem; color: #fff;">
                Vessops <span style=\"color: #3B82F6;font-size: 0.99rem\">x</span> Musigma
            </span>
        </footer>
        <script>
            window.addEventListener('scroll', function() {
                var footer = document.querySelector('footer');
                if ((window.innerHeight + window.scrollY) >= document.body.offsetHeight) {
                    footer.style.display = 'flex';
                    footer.style.opacity = '1';
                } else {
                    footer.style.opacity = '0';
                    footer.style.display = 'none';
                }
            });
            document.querySelector('footer').style.display = 'none';
        </script>
        """,
        unsafe_allow_html=True
    )

#________________________________________________________________________
#--- Tracking user query logs-----
#------------------------------------------------------------------------
def log_query_to_snowflake(username, prompt, status):
    created_at = datetime.utcnow()
    log_df = session.create_dataframe(
        [(username, prompt, status, created_at)],
        schema=["USERNAME", "PROMPT", "STATUS", "CREATED_AT"]
    )
    log_df.write.mode("append").save_as_table("VESSOPS_D.L00_STG.STREAMLIT_QUERY_LOGS")


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# --- Main Application Logic ---
def main():
    global all_insights # Ensure main function can clear/use the global list

    # st.title("Voyage Financial Performance Insights")

    if "initial_flow_choice" not in st.session_state:
        st.session_state.initial_flow_choice = "General Overview" 

    if 'selected_primary_metric_sidebar' not in st.session_state:
        st.session_state.selected_primary_metric_sidebar = "Margin $"  

    if "selected_order_column" not in st.session_state:
        st.session_state.selected_order_column = None # Or None, or ""

    if "selected_order_type" not in st.session_state:
        st.session_state.selected_order_type = None

    # Load data once
    df = load_data()
    future_df = load_future_data()

    if df.empty:
        st.error("No data loaded. Please check the data source and permissions.")
        st.stop()
    
    # --- Conditional Display based on Session State ---
    if st.session_state.page == 'landing_page':
        landing_page()


    elif st.session_state.page == 'query_results_page':
        st.session_state["all_insights"].clear() # Clear insights for this run

        user_prompt = st.session_state.current_query
        corrected_query = correct_query(user_prompt)
        # ships_q, products_q, years_q, months_q, voyages_q, SHIP_CLASSs_q,component = extract_filters_from_query(corrected_query, df)
        # st.write(component)
        st.write(f"**Interpreted Query:** {corrected_query}")
        DASHBOARD_URL = "https://prod-useast-b.online.tableau.com/#/site/nclh/views/NCLVoyageProfitability-PCDPPD/PRODUCTDETAIL?:iid=1"# Replace with actual dashboard URL
        col1, col2 = st.columns([7, 2])

        with col2:
            st.markdown(f"""
                <a href="{DASHBOARD_URL}" target="_blank">
                        <button style="
                            background-color: "skyblue";
                            color: white;
                            padding: 0.5em 1.5em;
                            border: none;
                            border-radius: 5px;
                            font-weight: bold;
                            cursor: pointer;
                            text-align: center;
                            display: inline-block;
                            font-size: 14px;">
                            Go to NCL VF Dashboard <br>
                        </button>
                    </a>
                """, unsafe_allow_html=True)
            
        # Reset all sort states first to ensure mutual exclusivity
        for key in st.session_state.sort_state.keys():
            st.session_state.sort_state[key] = None
        if st.session_state.selected_order_column and st.session_state.selected_order_type:
            column = st.session_state.selected_order_column
            order = st.session_state.selected_order_type
            column = column.upper()
   
            # Then set the state for the column from the query
            if column in st.session_state.sort_state:
                st.session_state.sort_state[column] = order
        if "nav_stack" not in st.session_state:
            st.session_state.nav_stack = []
        
        def go_to(page_name):
            current = st.session_state.get("initial_flow_choice", None)
            if current and (not st.session_state.nav_stack or st.session_state.nav_stack[-1] != current):
                st.session_state.nav_stack.append(current)
            st.session_state.initial_flow_choice = page_name
            st.rerun()
        
        def go_back():
            if st.session_state.nav_stack:
                st.session_state.initial_flow_choice = st.session_state.nav_stack.pop()
                st.rerun()
        

        
        if "nav_stack" not in st.session_state:
            st.session_state.nav_stack = []
        
        def go_to(page_name):
            current = st.session_state.get("initial_flow_choice", None)
            if current and (not st.session_state.nav_stack or st.session_state.nav_stack[-1] != current):
                st.session_state.nav_stack.append(current)
            st.session_state.initial_flow_choice = page_name
            st.rerun()
        
        def go_back():
            if st.session_state.nav_stack:
                st.session_state.initial_flow_choice = st.session_state.nav_stack.pop()
                st.rerun()


        with st.sidebar:
            st.header("Navigation")
        
            # Home button reset
            if st.button("Go to Home Page", key="reset_query_button"):
                st.session_state.page = 'landing_page'
                st.session_state.current_query = ""
                st.session_state.current_filters = {}
                st.session_state.filtered_data = pd.DataFrame()  # Clear filtered data
                st.session_state["all_insights"].clear()  # Clear insights
                st.session_state.selected_outlier_period = None  # Reset outlier period
                st.session_state.initial_flow_choice = "General Overview"  # Reset initial flow
                st.session_state.page_index = 0  # Reset page index
                st.session_state.nav_stack = []  # Reset nav stack
                st.rerun()
        
            # Back button (only show if stack is not empty)
            if st.session_state.nav_stack:
                if st.button("⬅️ Go Back", key="go_back_button"):
                    go_back()

                        
            st.markdown("---")
            st.subheader("Ask Another Question")
            
            # Text input will automatically sync with st.session_state["ask_another_question"]
            new_query = st.text_input(
                "Enter a new query:",
                key="ask_another_question"
            )
            
            if st.button("Submit Query", key="submit_new_query_btn"):
                query = new_query.strip()
                if query:
                    st.session_state.current_query = query
                    st.session_state.page = "query_results_page"
        
                    # --- Initial Routing Logic ---
                    corrected_query = correct_query(user_prompt)
                    query_type_raw = detect_query_type(corrected_query)
                    query_type_nlp = query_type_raw.upper() if query_type_raw else "UNKNOWN"
            
                    ships_q, products_q, years_q, months_q, voyages_q, SHIP_CLASSs_q,component = extract_filters_from_query(corrected_query, df)
    
                    order_column,order_type = extract_orders_from_query(corrected_query)
    
                    st.session_state.selected_order_column = order_column
                    st.session_state.selected_order_type = order_type
                    
                    # Determine initial metric based on NLP
                    if query_type_nlp in metric_groups and metric_groups[query_type_nlp]:
                        st.session_state.selected_primary_metric_sidebar = (
                            "Per Capacity Day (Margin PCD)" if query_type_nlp == "PCD"
                            else "Per Passenger Day (Margin PPD)" if query_type_nlp == "PPD"
                            else "Passenger Days" if query_type_nlp == "PASSENGER DAYS"
                            else "Capacity Days" if query_type_nlp == "CAPACITY DAYS"
                            else "Margin $"
                        )
            
                    # Determine initial flow based on query keywords
                    query_lower = corrected_query.lower()
                    if any(p_keyword in query_lower for p_keyword in PRODUCT_KEYWORDS):
                        st.session_state.initial_flow_choice = "Product-Centric"
                    elif any(s_keyword in query_lower for s_keyword in SHIP_KEYWORDS):
                        st.session_state.initial_flow_choice = "Ship-Centric"
                    elif any(v_keyword in query_lower for v_keyword in VOYAGE_KEYWORDS):
                        st.session_state.initial_flow_choice = "Voyage-Centric"
                    elif "outlier" in query_lower: # Direct to Outlier Analysis
                        st.session_state.initial_flow_choice = "Outlier Analysis"
                    elif any(d_keyword in query_lower for d_keyword in DEPLOYMENT_KEYWORDS):
                        st.session_state.initial_flow_choice = "Deployment Analysis"
                    else:
                        st.session_state.initial_flow_choice = "General Overview"
            
                    # Pre-populate sidebar filters based on query
                    st.session_state.current_filters['Year'] = [int(y) for y in years_q] if years_q else []
                    st.session_state.current_filters['Month'] = [int(m) for m in months_q] if months_q else []
                    st.session_state.current_filters['RM_ROLLUP_PRODUCT_DESC'] = products_q
                    st.session_state.current_filters['Ship Class'] = SHIP_CLASSs_q
                    st.session_state.current_filters['Ship'] = ships_q
                    st.session_state.current_filters['M0_AND_M1'] = component
             
                    st.rerun()# Rerun to go back to landing page
            
            st.markdown("---")
            st.header("Global Filters")

            # Get unique values for filters from the full dataframe
            all_years = sorted(df['FISCAL_YEAR'].dropna().unique().tolist())
            all_months = sorted(df['ACCOUNTING_PERIOD'].dropna().unique().tolist(), key=int)
            all_products = sorted(df['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist())
            all_ship_classes = sorted(df['SHIP_CLASS'].dropna().unique().tolist())
            all_ships = sorted(df['SHIP_CD'].dropna().unique().tolist())
            all_m0_M1 = sorted(df['M0_AND_M1'].dropna().unique().tolist())

            # Deployment change appended data
            if st.session_state.initial_flow_choice ==  "Deployment Analysis" and st.session_state.use_future_data:
                #st.write(future_df.head())
                df_un = pd.concat([df,future_df])
                all_years = sorted(df_un['FISCAL_YEAR'].dropna().unique().tolist())
                all_months = sorted(df_un['ACCOUNTING_PERIOD'].dropna().unique().tolist(), key=int)
                all_products = sorted(df_un['RM_ROLLUP_PRODUCT_DESC'].dropna().unique().tolist())
                all_ship_classes = sorted(df_un['SHIP_CLASS'].dropna().unique().tolist())
                all_ships = sorted(df_un['SHIP_CD'].dropna().unique().tolist())
                all_m0_M1 = sorted(df_un['M0_AND_M1'].dropna().unique().tolist())
    
            # Initialize selected filters based on session state or query defaults
            # This part will be updated after initial query processing
            selected_years_sidebar = st.multiselect(
                "Select Fiscal Year(s):",
                options=all_years,
                default=[x for x in st.session_state.current_filters.get('Year', []) if x in all_years],
                key="sidebar_years"
            )
            selected_months_sidebar = st.multiselect(
                "Select Accounting Period(s):",
                options=all_months,
                format_func=lambda x: calendar.month_name[int(x)],
                default=[x for x in st.session_state.current_filters.get('Month', []) if x in all_months],
                key="sidebar_months"
            )
            selected_products_sidebar = st.multiselect(
                "Select Product(s):",
                options=all_products,
                default=[x for x in st.session_state.current_filters.get('RM_ROLLUP_PRODUCT_DESC', []) if x in all_products],
                key="sidebar_products"
            )
            selected_ship_classes_sidebar = st.multiselect(
                "Select Ship Class(es):",
                options=all_ship_classes,
                default=[x for x in st.session_state.current_filters.get('Ship Class', []) if x in all_ship_classes],
                key="sidebar_ship_classes"
            )
            selected_ships_sidebar = st.multiselect(
                "Select Ship(s):",
                options=all_ships,
                default=[x for x in st.session_state.current_filters.get('Ship', []) if x in all_ships],
                key="sidebar_ships"
            )
            selected_components_sidebar = st.multiselect(
                "Select Component(s):",
                options=all_m0_M1,
                default=[x for x in st.session_state.current_filters.get('M0_AND_M1', []) if x in all_m0_M1],
                key="sidebar_components"
            )
            st.markdown("---")
            st.header("Primary Metric Selection")
            selected_metric_from_sidebar = st.radio(
                "Select a metric for analysis:",
                options=list(metric_display_to_col.keys()), # Use display names as options
                # Set the default index based on the session state.
                # It's good practice to add a fallback in case 'Margin' isn't found for some reason,
                # though it should be if your metric_display_to_col is consistent.
                index=list(metric_display_to_col.keys()).index(
                    st.session_state.selected_primary_metric_sidebar
                ) if st.session_state.selected_primary_metric_sidebar in list(metric_display_to_col.keys()) else 0, # Fallback to first option if 'Margin' somehow isn't there
                key="primary_metric_sidebar_radio"
            )
            #st.write(st.session_state.selected_primary_metric_sidebar)
            query_type_raw = detect_query_type(corrected_query)
            query_type_nlp = query_type_raw.upper() if query_type_raw else "UNKNOWN"
            # if query_type_nlp in metric_groups and metric_groups[query_type_nlp]:
            #         selected_metric_from_sidebar = ("Per Capacity Day (Margin PCD)" if query_type_nlp == "PCD"
            #                                                                  else "Per Passenger Day (Margin PPD)" if query_type_nlp == "PPD"
            #                                                                  else "Margin $")

            st.session_state.selected_primary_metric_sidebar = selected_metric_from_sidebar
            #st.write(st.session_state.selected_primary_metric_sidebar)

            st.markdown("---")
            st.header("Sort by")
            
            
            # A function to handle button clicks and state updates
            def handle_sort_click(metric):
                """Handles the click event for a sort button and cycles its state."""
                # Reset all other sort states to None to ensure only one is active
                for key in st.session_state.sort_state.keys():
                    if key != metric:
                        st.session_state.sort_state[key] = None
            
                # Cycle the state of the clicked button
                current_state = st.session_state.sort_state[metric]
                if current_state is None:
                    st.session_state.sort_state[metric] = 'dsc' # Default sort is descending
                elif current_state == 'dsc':
                    st.session_state.sort_state[metric] = 'asc'
                else:  # 'asc'
                    st.session_state.sort_state[metric] = None
                
                # Update the global order column and type based on the new state
                # This is what your plotting/data processing functions will read
                if st.session_state.sort_state[metric]:
                    st.session_state.selected_order_column = metric
                    st.session_state.selected_order_type = st.session_state.sort_state[metric]
                else:
                    st.session_state.selected_order_column = None
                    st.session_state.selected_order_type = None
            
            st.markdown("""
            <style>
            /* Clean Slate Gray buttons */
            div.stButton > button {
                background-color: #2c3e50 !important; /* Cool dark slate gray */
                color: white !important;
                border-radius: 8px !important;
                padding: 0.6rem 1.2rem !important;
                font-size: 1rem !important;
                font-weight: 600 !important;
                cursor: pointer !important;
                border: 1px solid #2c3e50;
                border-left: 5px solid #4FC3F7 !important; /* Cyan accent */
                transition: all 0.3s ease-in-out;
            }
             
            div.stButton > button:hover {
                background-color: #34495e !important; /* Lighter gray on hover */
                transform: translateY(-2px);
                box-shadow: 0 4px 10px rgba(0, 0, 0, 0.1);
            }
            </style>
            """, unsafe_allow_html=True)
            # Create columns for the buttons
            cols = st.columns(3)
            
            # GSS Button Logic
            with cols[0]:
                gss_state = st.session_state.sort_state.get("GSS")
                gss_label = "GSS"
                if gss_state == 'asc':
                    gss_label += " ▲"
                elif gss_state == 'dsc':
                    gss_label += " ▼"
                
                st.button(gss_label, on_click=handle_sort_click, args=("GSS",), use_container_width=True)
            
            # CII Button Logic
            with cols[1]:
                cii_state = st.session_state.sort_state.get("CII")
                cii_label = "CII"
                if cii_state == 'asc':
                    cii_label += " ▲"
                elif cii_state == 'dsc':
                    cii_label += " ▼"
            
                st.button(cii_label, on_click=handle_sort_click, args=("CII",), use_container_width=True)
            
            # LF Button Logic
            with cols[2]:
                lf_state = st.session_state.sort_state.get("LF")
                lf_label = "LF"
                if lf_state == 'asc':
                    lf_label += " ▲"
                elif lf_state == 'dsc':
                    lf_label += " ▼"
                st.button(lf_label, on_click=handle_sort_click, args=("LF",), use_container_width=True)
                
            if st.session_state.initial_flow_choice ==  "Deployment Analysis":
                st.markdown("---")
                st.header("Data Selection")
                # Center button using empty columns
                col2 = st.columns(1)
                with col2[0]:
                    if st.button("📈 Show Future Data"):
                        st.session_state.use_future_data = not st.session_state.use_future_data
                        st.rerun()
                # Status message
                if st.session_state.use_future_data:
                    st.success("✅ Future data is included in the analysis.")
                    
                else:
                    st.info("ℹ️ Currently showing only historical data (2023–2025).")
                                       

        # --- Apply Sidebar Filters to DataFrame ---
        filtered_df = df.copy()

        if selected_years_sidebar:
            filtered_df = filtered_df[filtered_df["FISCAL_YEAR"].isin(selected_years_sidebar)]
        if selected_months_sidebar:
            filtered_df = filtered_df[filtered_df["ACCOUNTING_PERIOD"].isin(selected_months_sidebar)]
        if selected_products_sidebar:
            filtered_df = filtered_df[filtered_df["RM_ROLLUP_PRODUCT_DESC"].isin(selected_products_sidebar)]
        if selected_ship_classes_sidebar:
            filtered_df = filtered_df[filtered_df["SHIP_CLASS"].isin(selected_ship_classes_sidebar)]
        if selected_ships_sidebar:
            filtered_df = filtered_df[filtered_df["SHIP_CD"].isin(selected_ships_sidebar)]
        if selected_components_sidebar:
            filtered_df = filtered_df[filtered_df["M0_AND_M1"].isin(selected_components_sidebar)]
        
        st.session_state.filtered_data = filtered_df # Update session state with filtered data
        if st.session_state.filtered_data.empty and not(st.session_state.initial_flow_choice ==  "Deployment Analysis" and st.session_state.use_future_data):
            st.warning("No data found matching your filters. Please try adjusting the filters in the sidebar or a different query.")
            log_query_to_snowflake(current_user, user_prompt, "FAIL")
    
        else:
            log_query_to_snowflake(current_user, user_prompt, "PASS")
            #st.write(st.session_state.selected_primary_metric_sidebar)
            primary_metric_col = metric_display_to_col.get(st.session_state.selected_primary_metric_sidebar, "COMPONENT_AMOUNT")
            selected_metric_display_name = st.session_state.selected_primary_metric_sidebar
            primary_metric_col = (
            "NEW_PRTD_CAPS_DAYS" if primary_metric_col == "PCD"
            else "NEW_PRTD_PAX_DAYS" if primary_metric_col == "PPD"
            else "NEW_PRTD_PAX_DAYS" if primary_metric_col == "NEW_PRTD_PAX_DAYS"
            else "NEW_PRTD_CAPS_DAYS" if primary_metric_col == "NEW_PRTD_CAPS_DAYS"
            else "COMPONENT_AMOUNT"
            )
          
            if "initial_flow_choice" not in st.session_state:
                st.session_state.initial_flow_choice = "General Overview"
            
            # --- Content Rendering Based on Flow ---
            selected_order_column = st.session_state.selected_order_column
            selected_order_type = st.session_state.selected_order_type
            
            if st.session_state.initial_flow_choice == "General Overview":
                st.header("📊 General Overview")
                st.info("This section provides a high-level overview based on your selected filters and metric.")                  
                display_kpis(filtered_df)
                st.markdown("---")
                plot_product_ranking(filtered_df, primary_metric_col, selected_metric_display_name, selected_order_column, selected_order_type)
                st.markdown("---")
                plot_shipclass_ranking(filtered_df, primary_metric_col, selected_metric_display_name, selected_order_column, selected_order_type)
                st.markdown("---")
                plot_ship_ranking(filtered_df, primary_metric_col, selected_metric_display_name, selected_order_column, selected_order_type)
                st.markdown("---")
                display_trend_chart(filtered_df, primary_metric_col, selected_metric_display_name, key="general_trend")
                st.markdown("---")
                plot_components_breakdown(filtered_df, primary_metric_col, selected_metric_display_name)
            
                st.markdown("---")

            elif st.session_state.initial_flow_choice == "Product-Centric":
                    # Get stored values (fallback to None)
                jump_product = st.session_state.get("selected_product_jump")
                jump_component = st.session_state.get("selected_component_jump")
            
                # Apply these filters on top of the filtered_df you already have
                filtered_df_product = filtered_df.copy()
            
                if jump_product:
                    filtered_df_product = filtered_df_product[
                        filtered_df_product["RM_ROLLUP_PRODUCT_DESC"] == jump_product
                    ]
            
                if jump_component:
                    filtered_df_product = filtered_df_product[
                        filtered_df_product["M0_AND_M1"] == jump_component
                    ]
                product_analysis_flow(df, filtered_df, corrected_query, primary_metric_col, selected_metric_display_name, selected_order_column, selected_order_type)
            
            elif st.session_state.initial_flow_choice == "Ship-Centric":
                                    # Get stored values (fallback to None)
                jump_ship= st.session_state.get("selected_ship_jump")
                jump_component = st.session_state.get("selected_component_jump")
            
                # Apply these filters on top of the filtered_df you already have
                filtered_df_ship= filtered_df.copy()
            
                if jump_ship:
                    filtered_df_ship = filtered_df_ship[
                        filtered_df_ship["SHIP_CD"] == jump_ship
                    ]
            
                if jump_component:
                    filtered_df_ship= filtered_df_ship[
                        filtered_df_ship["M0_AND_M1"] == jump_component
                    ]
                ship_analysis_flow(df, filtered_df, corrected_query, primary_metric_col, selected_metric_display_name, selected_order_column, selected_order_type)
            
            elif st.session_state.initial_flow_choice == "Voyage-Centric":
                voyage_analysis_flow(df, filtered_df, corrected_query, primary_metric_col, selected_metric_display_name, selected_order_column, selected_order_type)
            
            elif st.session_state.initial_flow_choice == "Outlier Analysis":
                outlier_analysis_flow(df, filtered_df, corrected_query, primary_metric_col, selected_metric_display_name)

            elif st.session_state.initial_flow_choice ==  "Deployment Analysis":
                st.header("🚢 Deployment Analysis")
                if st.session_state.use_future_data:
                    df_un = pd.concat([df, future_df])
                else:
                    df_un = df
            
                # Apply the same filter logic to unified dataset
                filtered_df_un = df_un.copy()
                if selected_years_sidebar:
                    filtered_df_un = filtered_df_un[filtered_df_un["FISCAL_YEAR"].isin(selected_years_sidebar)]
                if selected_months_sidebar:
                    filtered_df_un = filtered_df_un[filtered_df_un["ACCOUNTING_PERIOD"].isin(selected_months_sidebar)]
                if selected_products_sidebar:
                    filtered_df_un = filtered_df_un[filtered_df_un["RM_ROLLUP_PRODUCT_DESC"].isin(selected_products_sidebar)]
                if selected_ship_classes_sidebar:
                    filtered_df_un = filtered_df_un[filtered_df_un["SHIP_CLASS"].isin(selected_ship_classes_sidebar)]
                if selected_ships_sidebar:
                    filtered_df_un = filtered_df_un[filtered_df_un["SHIP_CD"].isin(selected_ships_sidebar)]
                if selected_components_sidebar:
                    filtered_df_un = filtered_df_un[(filtered_df_un["M0_AND_M1"].isin(selected_components_sidebar)) |   (filtered_df_un["M0_AND_M1"].isna())]

                deployment_analysis_flow(df, filtered_df, corrected_query, primary_metric_col, selected_metric_display_name, selected_order_column, selected_order_type,filtered_df_un)
            st.markdown("_________________________________________________________")
            st.subheader("Ready for a deeper dive?")
            with st.expander("Choose an Analysis"):
                if st.button(" Product-Centric Analysis", key="go_to_produsct_btn",help="An end-to-end drilldown from overall product performance to the most granular account details"):
                    go_to("Product-Centric")
            
                if st.button("Ship-Centric Analysis", key="go_to_ship_sbtn", help="An end-to-end drilldown from overall ship performance to the most granular account details"):
                    go_to("Ship-Centric")
            
                if st.button(" Voyage-Centric Analysis", key="go_to_voyage_sbtn", help="Voyage & Itinerary Contribution Breakdown"):
                    go_to("Voyage-Centric")
            
                if st.button(" Outlier Analysis", key="go_to_outlier_bsstn", help="Spot anomalies across ships, or products"):
                    go_to("Outlier Analysis")
            
                if st.button("Deployment Analysis", key="go_to_deployment_btsn" , help="Comparative Study of Deployment Patterns (Current vs Future)"):
                    go_to("Deployment Analysis")

            st.markdown("_________________________________________________________")
            # --- Helper: clean markdown from text ---
            def clean_markdown(text):
                if not text:
                    return ""
                text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
                text = text.replace("**", "").replace("*", "").replace("####", "").replace("###", "").replace("##", "").replace("#", "")
                return text.strip()
            
            # --- Function: create HTML report ---
            def create_html_report(query, filters, insights_list):
                ship = filters.get('Ship', 'All')
                year = filters.get('Year', 'All')
                month = filters.get('Month', 'All')
                product = filters.get('RM_ROLLUP_PRODUCT_DESC', 'All')
            
                # HTML header
                html_parts = [
                    "<html>",
                    "<head>",
                    "<meta charset='UTF-8'>",
                    "<style>",
                    "body { font-family: Arial, sans-serif; margin: 30px; }",
                    "h1 { color: #003366; }",
                    "h2 { color: #005599; margin-top: 25px; }",
                    ".chart { margin: 15px 0; border: 1px solid #ddd; padding: 10px; border-radius: 6px; }",
                    ".insight { margin-bottom: 40px; }",
                    ".meta { font-size: 14px; color: #555; margin-bottom: 20px; }",
                    "</style>",
                    "</head>",
                    "<body>",
                    "<h1>NCL VFM Analysis Report</h1>",
                    f"<p class='meta'><strong>User Query:</strong> {query or ''}</p>",
                    f"<p class='meta'><strong>Filters Applied:</strong> Ship = {ship}, Year = {year}, Month = {month}, PRODUCT = {product}</p>",
                ]
            
                # Add each insight section
                for section in insights_list:
                    title = section.get("title", "Insight")
                    text = clean_markdown(section.get("text", ""))
                    chart = section.get("chart", None)
            
                    html_parts.append("<div class='insight'>")
                    html_parts.append(f"<h2>{title}</h2>")
            
                    if text:
                        html_parts.append(f"<p>{text}</p>")
            
                    # Embed chart if available
                    if chart is not None:
                        try:
                            # Use plotly.io to get HTML div for chart
                            import plotly.io as pio
                            chart_html = pio.to_html(chart, include_plotlyjs="cdn", full_html=False)
                            html_parts.append(f"<div class='chart'>{chart_html}</div>")
                        except Exception as e:
                            html_parts.append(f"<p style='color:red;'>Chart render failed: {str(e)}</p>")
            
                    html_parts.append("</div>")
            
                # Close HTML
                html_parts.append("</body></html>")
            
                # Combine everything
                html_report = "\n".join(html_parts)
                return html_report
            
            # --- Streamlit logic for cleaning + downloading ---
            if "all_insights" in st.session_state and st.session_state["all_insights"]:
                # Clean and prepare all insights
                cleaned_all_insights = []
                for insight in st.session_state["all_insights"]:
                    cleaned_all_insights.append({
                        "title": insight.get("title", "Insight"),
                        "text": clean_markdown(insight.get("text", "")),
                        "chart": insight.get("chart", None)
                    })
            
                # Create HTML report
                html_report = create_html_report(
                    query=user_prompt,
                    filters=st.session_state.get("current_filters", {}),
                    insights_list=cleaned_all_insights
                )
            
                # Convert to bytes for download
                html_bytes = html_report.encode("utf-8")
            
                st.download_button(
                    label="Download Analysis Report (.html)",
                    data=html_bytes,
                    file_name="VFM_Analysis_Report.html",
                    mime="text/html"
                )
            else:
                st.info("No insights available yet to generate the report.")

if __name__ == "__main__":
    main()

