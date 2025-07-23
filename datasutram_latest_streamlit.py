import streamlit as st
import pandas as pd
import ast

# --- Load Data from S3 ---
@st.cache_data(show_spinner="Loading Pan-India GST Data...")
def load_pan_india_gst():
    return pd.read_parquet("s3://data-science-ds/find_DB/gst_base11_rv/")

# --- Helper Functions ---
def convert_to_list(hsn_string):
    try:
        parsed = ast.literal_eval(hsn_string)
        if isinstance(parsed, list):
            return [str(hsn) for hsn in parsed]
    except Exception:
        pass
    return []

def check_hsn_in_list(hsn_list, hsn_codes):
    if not isinstance(hsn_list, list):
        return []
    return [code for code in hsn_codes if any(str(hsn).startswith(code) for hsn in hsn_list)]

def filter_by_turnover(df, selected_slabs=None):
    turnover_ranges = {
        'Slab: Rs. 0 to 40 lakhs': (0, 4000000),
        'Slab: Rs. 40 lakhs to 1.5 Cr.': (4000000, 15000000),
        'Slab: Rs. 1.5 Cr. to 5 Cr.': (15000000, 50000000),
        'Slab: Rs. 5 Cr. to 25 Cr.': (50000000, 250000000),
        'Slab: Rs. 25 Cr. to 100 Cr.': (250000000, 1000000000),
        'Slab: Rs. 100 Cr. to 500 Cr.': (1000000000, 5000000000)
    }
    df['turnover'] = pd.to_numeric(df['turnover'], errors='coerce')

    def assign_slab(row):
        if pd.notna(row.get('turnover_slab')) and row['turnover_slab'] != '#NA':
            return row['turnover_slab']
        val = row['turnover']
        if pd.isna(val) or val <= 0:
            return '#NA'
        for slab, (lo, hi) in turnover_ranges.items():
            if lo <= val <= hi:
                return slab
        return '#NA'

    df['turnover_slab'] = df.apply(assign_slab, axis=1)
    df = df[df['turnover_slab'] != '#NA']
    if selected_slabs:
        df = df[df['turnover_slab'].isin(selected_slabs)]
    return df

# --- Streamlit UI ---
st.set_page_config(page_title="GST Data Tool", layout="wide")

col1, col2, col3 = st.columns([1, 8, 3])
with col1:
    st.image("https://datasutram.com/_next/image?url=%2Fimages%2Flogos%2Fds_icon.png&w=256&q=100", width=40)
with col2:
    st.markdown("<h1 style='margin-bottom: 0;'>Data Sutram</h1>", unsafe_allow_html=True)
with col3:
    if 'available_credits' not in st.session_state:
        st.session_state.available_credits = 10000
    st.markdown(f"<div style='text-align:right'><b>🪙 Credits:</b> {st.session_state.available_credits}</div>", unsafe_allow_html=True)

st.subheader("📡 Fetching GST Data....")

# --- Load and Pre-filter data ---
df_raw = load_pan_india_gst()
df_filtered = df_raw[(df_raw['status'] == 'Active') & (df_raw['pincode_status'].isin(['matched_pincode', 'adjacent_pincode']))]

all_states = sorted(df_filtered['state'].dropna().unique())
all_business_types = ["Manufacturer", "Trader:Distributor", "Trader: Retailer", "Service Provider"]

# --- Filters Form ---
with st.form("gst_input_form"):
    st.markdown("### 🔍 Apply Filters")

    hsn_input = st.text_input("Enter HSN Code(s) (comma-separated)")
    hsn_list = [h.strip() for h in hsn_input.split(",") if h.strip()]

    selected_states = st.multiselect("Select State(s)", all_states)

    if selected_states:
        available_cities = df_filtered[df_filtered['state'].isin(selected_states)]['city'].dropna().unique()
        selected_cities = st.multiselect("Select City(s)", sorted(available_cities))
    else:
        selected_cities = []

    selected_business_types = st.multiselect("Select Core Nature of Business", all_business_types)

    selected_slabs_ui = st.multiselect(
        "Select Turnover Slabs",
        ['Slab: Rs. 0 to 40 lakhs', 'Slab: Rs. 40 lakhs to 1.5 Cr.', 'Slab: Rs. 1.5 Cr. to 5 Cr.',
         'Slab: Rs. 5 Cr. to 25 Cr.', 'Slab: Rs. 25 Cr. to 100 Cr.', 'Slab: Rs. 100 Cr. to 500 Cr.']
    )

    submitted = st.form_submit_button("📊 Fetch Filtered Data")

# --- Filtering ---
if submitted:
    try:
        with st.spinner("Filtering data..."):
            df = df_filtered.copy()

            if selected_states:
                df = df[df['state'].isin(selected_states)]

            if selected_cities:
                df = df[df['city'].isin(selected_cities)]

            df['goods_hsns'] = df['goods_hsns'].apply(convert_to_list)

            if hsn_list:
                df['matched_hsn_code'] = df['goods_hsns'].apply(lambda x: check_hsn_in_list(x, hsn_list))
                df = df[df['matched_hsn_code'].apply(lambda x: len(x) > 0)]
            else:
                df['matched_hsn_code'] = [[] for _ in range(len(df))]

            # Business Type Filter
            if "core_nature_of_business" in df.columns and "nature_of_business" in df.columns and selected_business_types:
                keyword_map = {
                    "Manufacturer": ["Factory / Manufacturing", "Manufacturing"],
                    "Trader:Distributor": ["Wholesale", "Distributor"],
                    "Service Provider": ["Services"],
                    "Trader: Retailer": ["Retail Business"]
                }

                def business_type_match(row):
                    if row['core_nature_of_business'] in selected_business_types:
                        return True
                    if str(row['core_nature_of_business']).strip() == '#NA':
                        return any(
                            any(k.lower() in str(row['nature_of_business']).lower() for k in keyword_map.get(bt, []))
                            for bt in selected_business_types
                        )
                    return False

                df = df[df.apply(business_type_match, axis=1)]

            df = filter_by_turnover(df, selected_slabs_ui)

            total_count = len(df)
            st.success(f"✅ Total Records Fetched: {total_count}")
            st.dataframe(df.head(100))

            # --- Chart 1: Turnover-wise Count per City ---
            st.subheader("📊 Turnover-wise Count Table by City")
            if 'city' in df.columns and 'turnover_slab' in df.columns:
                city_pivot = df.pivot_table(index='city', columns='turnover_slab', aggfunc='size', fill_value=0)
                city_pivot = city_pivot.sort_values(by=city_pivot.columns.tolist(), ascending=False)
                st.dataframe(city_pivot, use_container_width=True)

            # --- Chart 2: Turnover-wise Count by HSN Code ---
            st.subheader("📊 Turnover-wise Count Table by HSN Code")
            hsn_expanded = df.explode('matched_hsn_code')
            hsn_expanded = hsn_expanded[hsn_expanded['matched_hsn_code'].notna() & (hsn_expanded['matched_hsn_code'] != '')]

            if not hsn_expanded.empty:
                hsn_pivot = hsn_expanded.pivot_table(index='matched_hsn_code', columns='turnover_slab', aggfunc='size', fill_value=0)
                hsn_pivot = hsn_pivot.sort_values(by=hsn_pivot.columns.tolist(), ascending=False)
                st.dataframe(hsn_pivot, use_container_width=True)

            # --- Export Option ---
            csv_data = df.to_csv(index=False).encode("utf-8")

            if total_count <= st.session_state.available_credits:
                st.download_button(
                    label=f"📁 Export {total_count} records (uses credits)",
                    data=csv_data,
                    file_name="gst_filtered_export.csv",
                    mime="text/csv",
                    key="export_button"
                )
                if st.session_state.get("exported") is not True:
                    st.session_state.available_credits -= total_count
                    st.session_state.exported = True
                    st.success("✅ File is ready for download. Credits deducted.")
                    st.info(f"💰 Remaining Credits: {st.session_state.available_credits}")

    except Exception as e:
        st.exception(e)
