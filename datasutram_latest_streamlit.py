import streamlit as st
import pandas as pd
import ast
from pymongo import MongoClient
from tqdm import tqdm

# -------------------- Mongo Setup ------------------------
client1 = MongoClient('mongodb://localhost:27017')
vikrant_db1 = client1.vikrant_db
shapes_db = vikrant_db1.shape_DB
shapes2 = shapes_db
pincode_db = vikrant_db1.pincode_DB
gst_data = client1.manthan.gst_v11

# -------------------- Utility Functions ------------------------

def get_shapes(location_list):
    shapes = pd.DataFrame(list(shapes_db.find({
        'name': {'$in': location_list},
        'level': {'$in': ['city', 'state']}
    })))
    return shapes

def get_pincodeshapes(pincode_list):
    return pd.DataFrame(pincode_db.find({'name': {'$in': pincode_list}}))

def fetch_gst_data1(final_shapes):
    gst_list = []
    if final_shapes.empty:
        raise ValueError("No shapes found for the selected location(s).")

    for i in tqdm(range(final_shapes.shape[0])):
        shape = final_shapes.iloc[i]
        result = list(gst_data.find({
            'geometry': {'$geoWithin': {'$geometry': shape['geometry']}}
        }))
        if result:
            res = pd.DataFrame(result)
            res['location'] = shape['name']
            gst_list.append(res)

    if not gst_list:
        raise ValueError("No GST data found for the given shapes.")

    return pd.concat(gst_list, ignore_index=True)

@st.cache_data(show_spinner="Loading Pan-India GST Data...")
def load_pan_india_gst():
    return pd.read_parquet("s3://data-science-ds/find_DB/gst_base11_rv/")

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

def saint_gobain_turnoverwise_gst_data(hsn_6_digits, location_list=None, pincode_list=None, level="city", selected_slabs=None):
    if level == "city":
        final_shapes = get_shapes(location_list)
        final_gst_data = fetch_gst_data1(final_shapes)
    elif level == "pan_india":
        final_gst_data = load_pan_india_gst()
    elif level == "pincode":
        final_shapes = get_pincodeshapes(pincode_list)
        final_gst_data = fetch_gst_data1(final_shapes)
    else:
        raise ValueError("Invalid level. Choose from 'city', 'pan_india', or 'pincode'.")

    if 'pincode_status' in final_gst_data.columns:
        final_df = final_gst_data[
            (final_gst_data['status'] == 'Active') &
            (final_gst_data['pincode_status'].isin(['matched_pincode', 'adjacent_pincode']))]
    else:
        final_df = final_gst_data[final_gst_data['status'] == 'Active']

    final_df['goods_hsns'] = final_df['goods_hsns'].apply(convert_to_list)
    final_df['matched_hsn_code'] = final_df['goods_hsns'].apply(lambda x: check_hsn_in_list(x, hsn_6_digits))
    filtered_gst = final_df[final_df['matched_hsn_code'].apply(lambda x: len(x) > 0)]
    filtered_gst = filter_by_turnover(filtered_gst, selected_slabs)
    return filtered_gst

# -------------------- Streamlit UI ------------------------

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

with st.form("gst_filter_form"):
    st.subheader("🔎 Apply Filters")

    level = st.selectbox("📍 Select Data Level", ["Shapes", "Pan India"], index=0)

    location_list = []
    selected_states = []
    selected_cities = []

    if level == "Shapes":
        all_states_raw = shapes2.find({"level": "state", "name": {"$exists": True}})
        all_states = sorted({doc["name"] for doc in all_states_raw if isinstance(doc["name"], str)})
        selected_states = st.multiselect("🌐 Select State(s)", all_states)

        all_cities_raw = shapes2.find({"level": "city", "name": {"$exists": True}})
        all_cities = sorted({
            doc["name"] for doc in all_cities_raw
            if isinstance(doc.get("name"), str) and not doc["name"].isdigit()
        })
        selected_cities = st.multiselect("🏙️ Select City(ies)", all_cities)

        location_list = selected_states + selected_cities

    hsn_input = st.text_input("📉 Enter HSN Codes (comma-separated)")
    hsn_6_digits = [hsn.strip() for hsn in hsn_input.split(",") if hsn.strip()]

    selected_slabs_ui = st.multiselect(
        "💰 Select Turnover Slabs",
        ['Slab: Rs. 0 to 40 lakhs', 'Slab: Rs. 40 lakhs to 1.5 Cr.',
         'Slab: Rs. 1.5 Cr. to 5 Cr.', 'Slab: Rs. 5 Cr. to 25 Cr.',
         'Slab: Rs. 25 Cr. to 100 Cr.', 'Slab: Rs. 100 Cr. to 500 Cr.']
    )

    business_types = ["Manufacturer", "Trader:Distributor", "Trader: Retailer", "Service Provider"]
    selected_business_types = st.multiselect("🏢 Select Core Nature of Business", business_types)

    submitted = st.form_submit_button("📊 Fetch Data")

if submitted:
    if level == "Shapes" and not location_list:
        st.warning("⚠️ Please select at least one state or city to proceed.")
    elif not hsn_6_digits:
        st.warning("⚠️ Please enter at least one HSN code.")
    else:
        try:
            with st.spinner("🔄 Fetching and Processing..."):
                df = saint_gobain_turnoverwise_gst_data(
                    hsn_6_digits=hsn_6_digits,
                    location_list=location_list if level == "Shapes" else None,
                    level="city" if level == "Shapes" else "pan_india",
                    selected_slabs=selected_slabs_ui
                )

                if selected_business_types:
                    keyword_map = {
                        "Manufacturer": ["manufactur"],
                        "Trader:Distributor": ["wholesale", "distributor"],
                        "Trader: Retailer": ["retail"],
                        "Service Provider": ["service"]
                    }

                    def business_match(row):
                        core = str(row.get("core_nature_of_business", "")).lower()
                        nature = str(row.get("nature_of_business", "")).lower()
                        for selected_type in selected_business_types:
                            keywords = keyword_map.get(selected_type, [])
                            core_match = any(kw in core for kw in keywords)
                            nature_match = any(kw in nature for kw in keywords)
                            if core_match or (core in ["", "#na", "#NA"] and nature_match):
                                return True
                        return False

                    df = df[df.apply(business_match, axis=1)]

                st.success(f"✅ Total Records Fetched: {len(df)}")
                st.dataframe(df.head(100))

                df_exploded = df.explode('matched_hsn_code')
                df_exploded = df_exploded[df_exploded['matched_hsn_code'].notna()]
                df_exploded['count'] = 1

                hsn_by_turnover = (
                    df_exploded
                    .groupby(['turnover_slab', 'matched_hsn_code'])['count']
                    .sum()
                    .reset_index()
                    .pivot(index='matched_hsn_code', columns='turnover_slab', values='count')
                    .fillna(0).astype(int)
                )
                st.subheader("📊 HSN Code vs Turnover Slab")
                st.dataframe(hsn_by_turnover, use_container_width=True)

                pivot_col = 'location' if level == "Shapes" else 'city'
                if pivot_col not in df_exploded.columns and level == "pan_india":
                    df_exploded[pivot_col] = df.get('city', 'Unknown')

                hsn_by_location = (
                    df_exploded
                    .groupby([pivot_col, 'matched_hsn_code'])['count']
                    .sum()
                    .reset_index()
                    .pivot(index='matched_hsn_code', columns=pivot_col, values='count')
                    .fillna(0).astype(int)
                )
                st.subheader(f"📊 HSN Code vs {'Location' if level == 'Shapes' else 'City'}")
                st.dataframe(hsn_by_location, use_container_width=True)

                csv_data = df.to_csv(index=False).encode("utf-8")
                if len(df) <= st.session_state.available_credits:
                    st.download_button("📅 Download CSV", data=csv_data, file_name="gst_filtered.csv", mime="text/csv")
                    st.session_state.available_credits -= len(df)
                else:
                    st.warning("❗ Not enough credits to download data.")

        except Exception as e:
            st.error("❌ Error fetching data")
            st.exception(e)
