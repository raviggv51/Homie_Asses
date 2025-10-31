# 1
import pandas as pd
import json
from pathlib import Path
#2
import pandas as pd
from pandas import json_normalize
#3
import numpy as np
import re
json_path = Path("../data/fake_property_data_new.json")

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)
# Convert JSON array → DataFrame
df = pd.json_normalize(data)
print(f"Shape: {df.shape}")
# display(df.head(2))
# Check columns containing nested data (Valuation, HOA, Rehab)
nested_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
print("\nNested columns:", nested_cols)

# step 1 DF creation

# 1️⃣ Assign unique ID to each property
df['property_id'] = range(1, len(df) + 1)
# 2️⃣ PROPERTY TABLE — flat columns directly mapped to property
property_cols = [
    'property_id', 'Property_Title', 'Address', 'Market', 'Flood', 'Street_Address', 'City', 'State', 'Zip',
    'Property_Type', 'Highway', 'Train', 'Tax_Rate', 'SQFT_Basement', 'HTW', 'Pool', 'Commercial', 'Water',
    'Sewage', 'Year_Built', 'SQFT_MU', 'SQFT_Total', 'Parking', 'Bed', 'Bath', 'BasementYesNo', 'Layout',
    'Rent_Restricted', 'Neighborhood_Rating', 'Latitude', 'Longitude', 'Subdivision', 'School_Average'
]
property_df = df[property_cols].copy()
# 3️⃣ LEADS TABLE
leads_cols = [
    'property_id', 'Reviewed_Status', 'Most_Recent_Status', 'Source', 'Occupancy',
    'Net_Yield', 'IRR', 'Selling_Reason', 'Seller_Retained_Broker', 'Final_Reviewer'
]
leads_df = df[leads_cols].copy()
# 4️⃣ TAXES TABLE
taxes_cols = ['property_id', 'Taxes']
taxes_df = df[taxes_cols].copy()
# 5️⃣ VALUATION TABLE — nested list
valuation_records = []
for _, row in df[['property_id', 'Valuation']].dropna(subset=['Valuation']).iterrows():
    for val in row['Valuation']:
        val['property_id'] = row['property_id']
        valuation_records.append(val)
valuation_df = pd.DataFrame(valuation_records)
# 6️⃣ HOA TABLE — nested list
hoa_records = []
for _, row in df[['property_id', 'HOA']].dropna(subset=['HOA']).iterrows():
    for h in row['HOA']:
        h['property_id'] = row['property_id']
        hoa_records.append(h)
hoa_df = pd.DataFrame(hoa_records)
# 7️⃣ REHAB TABLE — nested list
rehab_records = []
for _, row in df[['property_id', 'Rehab']].dropna(subset=['Rehab']).iterrows():
    for r in row['Rehab']:
        r['property_id'] = row['property_id']
        rehab_records.append(r)
rehab_df = pd.DataFrame(rehab_records)
# Quick preview of each
# display(property_df.head(2))
# display(leads_df.head(2))
# display(valuation_df.head(2))
# display(hoa_df.head(2))
# display(rehab_df.head(2))
# display(taxes_df.head(2))
# common helper for cleaning data
import re, numpy as np, pandas as pd

def clean_text(x):
    if pd.isna(x): 
        return np.nan
    if isinstance(x, str):
        x = x.strip().replace("Null", "").replace("null", "").replace("NULL", "")
        return np.nan if x == "" else x
    return x

def yes_no_normalize(val):
    if isinstance(val, str):
        v = val.strip().lower()
        if v in ['yes','y','true']: return 'Yes'
        if v in ['no','n','false']: return 'No'
    return np.nan

def extract_numeric(val):
    if pd.isna(val): return np.nan
    if isinstance(val, str):
        digits = re.findall(r'\d+', val)
        return float(digits[0]) if digits else np.nan
    return val

def clean_name(x):
    if isinstance(x, str):
        return x.strip().title()
    return np.nan

# cleaning property_df


# ---------- Helper Functions ----------

def clean_text(x):
    """Strip spaces, replace 'Null'/'null' with NaN."""
    if pd.isna(x): 
        return np.nan
    if isinstance(x, str):
        x = x.strip().replace("Null", "").replace("null", "").replace("NULL", "")
        return np.nan if x == "" else x
    return x

def yes_no_normalize(val):
    """Normalize yes/no variants."""
    if isinstance(val, str):
        v = val.strip().lower()
        if v in ['yes','y','true']: return 'Yes'
        if v in ['no','n','false']: return 'No'
    return np.nan

def extract_numeric(val):
    """Extract numeric value from strings like '5649 sqft'."""
    if pd.isna(val): return np.nan
    if isinstance(val, str):
        digits = re.findall(r'\d+', val)
        return float(digits[0]) if digits else np.nan
    return val

def fix_market(val):
    """Fix market names and tag unknowns."""
    mapping = {"Chicgo": "Chicago", "Dalas": "Dallas", "Dalas.": "Dallas"}
    if isinstance(val, str):
        val = val.strip().title()
        return mapping.get(val, val)
    return "Unknown"

# ---------- Cleaning Pipeline ----------

# 1️⃣ Clean text-like columns (only the string/object ones)
for col in property_df.select_dtypes(include='object').columns:
    property_df[col] = property_df[col].map(clean_text)

# 2️⃣ Market cleaning — fill missing with 'Unknown'
property_df['Market'] = property_df['Market'].map(fix_market)
property_df['Market'] = property_df['Market'].fillna('Unknown')

# 3️⃣ Normalize Yes/No columns
for col in ['Pool', 'Commercial', 'Rent_Restricted', 'HTW']:
    if col in property_df.columns:
        property_df[col] = property_df[col].map(yes_no_normalize)

# 4️⃣ Convert numeric-like columns
numeric_cols = [
    'Tax_Rate', 'SQFT_Basement', 'SQFT_MU', 'Year_Built',
    'Neighborhood_Rating', 'School_Average', 'Latitude', 'Longitude'
]
for col in numeric_cols:
    property_df[col] = pd.to_numeric(property_df[col], errors='coerce')

# 5️⃣ Handle SQFT_Total with text like "5649 sqft"
property_df['SQFT_Total'] = property_df['SQFT_Total'].map(extract_numeric)

# 6️⃣ Format address fields
property_df['Zip'] = property_df['Zip'].astype(str).str.zfill(5)
property_df['City'] = property_df['City'].str.title()
property_df['State'] = property_df['State'].str.upper()

# 7️⃣ Geo sanity checks
property_df.loc[~property_df['Latitude'].between(-90, 90), 'Latitude'] = np.nan
property_df.loc[~property_df['Longitude'].between(-180, 180), 'Longitude'] = np.nan

# 8️⃣ Year sanity check
property_df.loc[(property_df['Year_Built'] < 1800) | (property_df['Year_Built'] > 2025), 'Year_Built'] = np.nan

# 9️⃣ Fill empty Market as "Unknown"
property_df['Market'] = property_df['Market'].fillna("Unknown")

# ---------- Preview ----------
print("✅ property_df cleaned successfully.")
print("Rows:", len(property_df))
# display(property_df.head(3))


import numpy as np
import pandas as pd

# ---------- Helper mappers ----------
def normalize_status(status):
    """Standardize Reviewed and Most_Recent status."""
    if pd.isna(status):
        return np.nan
    s = status.strip().lower()
    mapping = {
        "close": "Closed",
        "closed": "Closed",
        "in progress": "In Progress",
        "inprogress": "In Progress",
        "open": "Open",
        "reviewed": "Reviewed",
        "pending": "Pending",
        "": np.nan
    }
    return mapping.get(s, status.title())

def normalize_source(src):
    """Normalize Source field."""
    if pd.isna(src):
        return np.nan
    s = src.strip().lower()
    mapping = {
        "internal.": "internal",
        "mls.": "mls",
        "broker.": "broker",
        "external": "external"
    }
    return mapping.get(s, s)

def normalize_occupancy(x):
    """Standardize occupancy field."""
    if pd.isna(x): 
        return np.nan
    val = str(x).strip().lower()
    if val in ["yes", "y", "occupied", "1"]:
        return "Yes"
    elif val in ["no", "n", "vacant", "0"]:
        return "No"
    return "Unknown"

# ---------- Cleaning Pipeline ----------

# 1️⃣ Clean all text-like columns first
for col in leads_df.select_dtypes(include='object').columns:
    leads_df[col] = leads_df[col].map(clean_text)

# 2️⃣ Normalize key categorical columns
leads_df['Reviewed_Status'] = leads_df['Reviewed_Status'].map(normalize_status)
leads_df['Most_Recent_Status'] = leads_df['Most_Recent_Status'].map(normalize_status)
leads_df['Source'] = leads_df['Source'].map(normalize_source)
leads_df['Occupancy'] = leads_df['Occupancy'].map(normalize_occupancy)
leads_df['Final_Reviewer'] = leads_df['Final_Reviewer'].map(clean_name)

# 3️⃣ Convert numeric columns
leads_df['Net_Yield'] = pd.to_numeric(leads_df['Net_Yield'], errors='coerce')
leads_df['IRR'] = pd.to_numeric(leads_df['IRR'], errors='coerce')

# 4️⃣ Handle outliers
leads_df.loc[leads_df['Net_Yield'] > 100, 'Net_Yield'] = np.nan
leads_df.loc[leads_df['IRR'] > 100, 'IRR'] = np.nan

# 5️⃣ Fill missing statuses as 'Unknown'
leads_df['Reviewed_Status'] = leads_df['Reviewed_Status'].fillna("Unknown")
leads_df['Most_Recent_Status'] = leads_df['Most_Recent_Status'].fillna("Unknown")

# ---------- Drop invalid or useless records ----------
leads_df = leads_df[~(
    (leads_df['Reviewed_Status'].isin(['Unknown', np.nan])) &
    (leads_df['Most_Recent_Status'].isin(['Unknown', np.nan]))
)]
leads_df = leads_df[~leads_df['Source'].isin(['Unknown', np.nan])]
leads_df = leads_df[~leads_df['Final_Reviewer'].isna()]
leads_df = leads_df.dropna(subset=['Net_Yield', 'IRR'], how='all')
leads_df = leads_df.drop_duplicates(subset=['property_id'], keep='first')

# ---------- Preview ----------
print("✅ leads_df cleaned and filtered successfully.")
print("Remaining rows:", len(leads_df))
# display(leads_df.head(5))

# cleaning of valution_df
import numpy as np
import pandas as pd

# ---------- Cleaning Pipeline ----------

# 1️⃣ Clean text-like columns using your shared helper
for col in valuation_df.select_dtypes(include='object').columns:
    valuation_df[col] = valuation_df[col].map(clean_text)

# 2️⃣ Ensure all expected columns exist
expected_cols = [
    'List_Price', 'Previous_Rent', 'ARV', 'Rent_Zestimate',
    'Low_FMR', 'High_FMR', 'Zestimate', 'Expected_Rent', 'Redfin_Value'
]
for col in expected_cols:
    if col not in valuation_df.columns:
        valuation_df[col] = np.nan

# 3️⃣ Convert all numeric columns to proper floats
for col in expected_cols:
    valuation_df[col] = pd.to_numeric(valuation_df[col], errors='coerce')

# 4️⃣ Handle outliers
# Drop unrealistic valuations (negative)
for col in ['List_Price', 'ARV', 'Zestimate', 'Redfin_Value']:
    valuation_df.loc[valuation_df[col] < 0, col] = np.nan


# 5️⃣ Remove duplicates (same property_id + List_Price + ARV combination)
valuation_df = valuation_df.drop_duplicates(subset=['property_id', 'List_Price', 'ARV'], keep='first')

# 6️⃣ Drop rows that have no valuation information at all
valuation_df = valuation_df.dropna(
    subset=['List_Price','ARV','Zestimate','Expected_Rent','Redfin_Value'],
    how='all'
)

# 7️⃣ Optional: Fill missing rents with average of available rent columns
rent_cols = ['Previous_Rent','Expected_Rent','Rent_Zestimate','Low_FMR','High_FMR']
valuation_df['avg_rent'] = valuation_df[rent_cols].mean(axis=1, skipna=True)

# ---------- Preview ----------
print("✅ valuation_df cleaned and filtered successfully.")
print("Rows:", len(valuation_df))
# display(valuation_df.head(5))

# cleaning HOA_DF 
import numpy as np
import pandas as pd

# ---------- Cleaning Pipeline ----------

# 1️⃣ Clean all text-like columns first
for col in hoa_df.select_dtypes(include='object').columns:
    hoa_df[col] = hoa_df[col].map(clean_text)

# 2️⃣ Convert HOA amount to numeric
hoa_df['HOA'] = pd.to_numeric(hoa_df['HOA'], errors='coerce')

# 3️⃣ Normalize HOA_Flag values (Yes/No)
hoa_df['HOA_Flag'] = hoa_df['HOA_Flag'].map(yes_no_normalize)

# # 4️⃣ Handle invalid or extreme HOA fees
# hoa_df.loc[hoa_df['HOA'] < 0, 'HOA'] = np.nan
# hoa_df.loc[hoa_df['HOA'] > 10000, 'HOA'] = np.nan   # outlier cap (10k/month)

# 5️⃣ Drop useless rows (both HOA and HOA_Flag missing)
hoa_df = hoa_df.dropna(subset=['HOA', 'HOA_Flag'], how='all')

# 6️⃣ Remove duplicates for same property_id + HOA value
hoa_df = hoa_df.drop_duplicates(subset=['property_id', 'HOA'], keep='first')

# ---------- Preview ----------
print("✅ hoa_df cleaned and filtered successfully.")
print("Rows:", len(hoa_df))
# display(hoa_df.head(5))

# cleaning rehab_df
import numpy as np
import pandas as pd

# ---------- Cleaning Pipeline ----------

# 1️⃣ Clean all text columns first
for col in rehab_df.select_dtypes(include='object').columns:
    rehab_df[col] = rehab_df[col].map(clean_text)

# 2️⃣ Convert numeric cost columns
num_cols = ['Underwriting_Rehab', 'Rehab_Calculation']
for col in num_cols:
    rehab_df[col] = pd.to_numeric(rehab_df[col], errors='coerce')

# 3️⃣ Normalize all yes/no flag columns
flag_cols = [
    'Paint', 'Flooring_Flag', 'Foundation_Flag', 'Roof_Flag',
    'HVAC_Flag', 'Kitchen_Flag', 'Bathroom_Flag', 'Appliances_Flag',
    'Windows_Flag', 'Landscaping_Flag', 'Trashout_Flag'
]
for col in flag_cols:
    if col in rehab_df.columns:
        rehab_df[col] = rehab_df[col].map(yes_no_normalize)

# 4️⃣ Handle missing/invalid numeric values
# Replace negative with NaN
for col in num_cols:
    rehab_df.loc[rehab_df[col] < 0, col] = np.nan
    

# 5️⃣ Drop rows where both cost columns are missing
rehab_df = rehab_df.dropna(subset=['Underwriting_Rehab', 'Rehab_Calculation'], how='all')

# 6️⃣ Optional: Fill missing numeric with 0 (only if needed for analytics)
rehab_df[['Underwriting_Rehab','Rehab_Calculation']] = rehab_df[['Underwriting_Rehab','Rehab_Calculation']].fillna(0)

# 7️⃣ Remove duplicate entries for same property + cost
rehab_df = rehab_df.drop_duplicates(subset=['property_id', 'Underwriting_Rehab', 'Rehab_Calculation'], keep='first')

# ---------- Preview ----------
print("✅ rehab_df cleaned and filtered successfully.")
print("Rows:", len(rehab_df))
# display(rehab_df.head(5))

# cleaning taxes_df
import numpy as np
import pandas as pd

# ---------- Cleaning Pipeline ----------

# 1️⃣ Clean text-like columns (in case any string formatting)
for col in taxes_df.select_dtypes(include='object').columns:
    taxes_df[col] = taxes_df[col].map(clean_text)

# 2️⃣ Convert 'Taxes' to numeric
taxes_df['Taxes'] = pd.to_numeric(taxes_df['Taxes'], errors='coerce')

# 3️⃣ Handle invalid 
taxes_df.loc[taxes_df['Taxes'] < 0, 'Taxes'] = np.nan               # negative taxes → NaN

# 4️⃣ Drop rows with missing or invalid taxes
taxes_df = taxes_df.dropna(subset=['Taxes'])

# 5️⃣ Remove duplicate property entries (keep first)
taxes_df = taxes_df.drop_duplicates(subset=['property_id'], keep='first')

# 6️⃣ Optional: round taxes to nearest integer
taxes_df['Taxes'] = taxes_df['Taxes'].round(0).astype('Int64')

# ---------- Preview ----------
print("✅ taxes_df cleaned and filtered successfully.")
print("Rows:", len(taxes_df))
# display(taxes_df.head(5))

# drop dupilcates from all DF
# property_df drops
property_df = property_df.dropna(subset=['Address', 'Property_Title'])
property_df = property_df[~property_df['Market'].isin(['Unknown', np.nan])]

# leads_df drops
leads_df = leads_df[~(
    (leads_df['Reviewed_Status'].isin(['Unknown', np.nan])) &
    (leads_df['Most_Recent_Status'].isin(['Unknown', np.nan]))
)]
leads_df = leads_df[~leads_df['Source'].isin(['Unknown', np.nan])]
leads_df = leads_df[~leads_df['Final_Reviewer'].isna()]
leads_df = leads_df.dropna(subset=['Net_Yield', 'IRR'], how='all')
leads_df = leads_df.drop_duplicates(subset=['property_id'], keep='first')

# valuation_df drops
valuation_df = valuation_df.dropna(
    subset=['List_Price','ARV','Zestimate','Expected_Rent','Redfin_Value'],
    how='all'
)

# hoa_df drops
hoa_df = hoa_df.dropna(subset=['HOA','HOA_Flag'], how='all')

# rehab_df drops
rehab_df = rehab_df.dropna(subset=['Underwriting_Rehab','Rehab_Calculation'], how='all')

# taxes_df drops
taxes_df = taxes_df.dropna(subset=['Taxes'])



from sqlalchemy import create_engine, text

# ---------- MySQL connection settings ----------
DB_USER = "db_user"
DB_PASS = "6equj5_db_user"   # replace with your container’s user password
DB_HOST = "mysql_ctn"          # service/container name (from docker-compose)
DB_PORT = "3306"
DB_NAME = "home_db"
DB_HOST = "127.0.0.1"

# ---------- Create SQLAlchemy engine ----------
connection_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_url)

print("✅ MySQL connection engine created successfully.")

# creating engine to execute DDl create table
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS property (
            property_id INT PRIMARY KEY,
            Property_Title VARCHAR(255),
            Address VARCHAR(255),
            Market VARCHAR(50),
            Flood VARCHAR(50),
            Street_Address VARCHAR(255),
            City VARCHAR(100),
            State VARCHAR(10),
            Zip VARCHAR(10),
            Property_Type VARCHAR(50),
            Highway VARCHAR(50),
            Train VARCHAR(50),
            Tax_Rate FLOAT,
            SQFT_Basement FLOAT,
            HTW VARCHAR(10),
            Pool VARCHAR(10),
            Commercial VARCHAR(10),
            Water VARCHAR(50),
            Sewage VARCHAR(50),
            Year_Built INT,
            SQFT_MU FLOAT,
            SQFT_Total FLOAT,
            Parking VARCHAR(50),
            Bed INT,
            Bath INT,
            BasementYesNo VARCHAR(10),
            Layout VARCHAR(50),
            Rent_Restricted VARCHAR(10),
            Neighborhood_Rating FLOAT,
            Latitude FLOAT,
            Longitude FLOAT,
            Subdivision VARCHAR(100),
            School_Average FLOAT
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS leads (
            lead_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            Reviewed_Status VARCHAR(50),
            Most_Recent_Status VARCHAR(50),
            Source VARCHAR(50),
            Occupancy VARCHAR(10),
            Net_Yield FLOAT,
            IRR FLOAT,
            Selling_Reason VARCHAR(255),
            Seller_Retained_Broker VARCHAR(255),
            Final_Reviewer VARCHAR(100),
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS valuation (
            valuation_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            List_Price FLOAT,
            Previous_Rent FLOAT,
            ARV FLOAT,
            Rent_Zestimate FLOAT,
            Low_FMR FLOAT,
            High_FMR FLOAT,
            Zestimate FLOAT,
            Expected_Rent FLOAT,
            Redfin_Value FLOAT,
            avg_rent FLOAT,
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS hoa (
            hoa_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            HOA FLOAT,
            HOA_Flag VARCHAR(10),
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rehab (
            rehab_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            Underwriting_Rehab FLOAT,
            Rehab_Calculation FLOAT,
            Paint VARCHAR(10),
            Flooring_Flag VARCHAR(10),
            Foundation_Flag VARCHAR(10),
            Roof_Flag VARCHAR(10),
            HVAC_Flag VARCHAR(10),
            Kitchen_Flag VARCHAR(10),
            Bathroom_Flag VARCHAR(10),
            Appliances_Flag VARCHAR(10),
            Windows_Flag VARCHAR(10),
            Landscaping_Flag VARCHAR(10),
            Trashout_Flag VARCHAR(10),
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS taxes (
            tax_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            Taxes FLOAT,
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );
    """))

    print("✅ All tables created successfully in MySQL.")

# Find property IDs in leads_df not present in property_df
missing_prop_ids = set(leads_df['property_id']) - set(property_df['property_id'])
print(f"❌ Missing property IDs in property_df: {len(missing_prop_ids)}")
if missing_prop_ids:
    print(list(missing_prop_ids)[:20])
# Keep only valid property_id references across all related tables
valid_property_ids = set(property_df['property_id'])

leads_df = leads_df[leads_df['property_id'].isin(valid_property_ids)].copy()
valuation_df = valuation_df[valuation_df['property_id'].isin(valid_property_ids)].copy()
hoa_df = hoa_df[hoa_df['property_id'].isin(valid_property_ids)].copy()
rehab_df = rehab_df[rehab_df['property_id'].isin(valid_property_ids)].copy()
taxes_df = taxes_df[taxes_df['property_id'].isin(valid_property_ids)].copy()

print("✅ All child DataFrames filtered to only valid property_id values.")

# 1️⃣ Load property table first (must exist before children)
property_df.to_sql('property', engine, if_exists='append', index=False)

# # 2️⃣ Load all child tables now
# leads_df.to_sql('leads', engine, if_exists='append', index=False)
# valuation_df.to_sql('valuation', engine, if_exists='append', index=False)
# hoa_df.to_sql('hoa', engine, if_exists='append', index=False)
# rehab_df.to_sql('rehab', engine, if_exists='append', index=False)
# taxes_df.to_sql('taxes', engine, if_exists='append', index=False)

print("✅ All tables loaded successfully with referential integrity maintained!")
leads_df.to_sql('leads', engine, if_exists='append', index=False)
valuation_df.to_sql('valuation', engine, if_exists='append', index=False)
hoa_df.to_sql('hoa', engine, if_exists='append', index=False)
rehab_df.to_sql('rehab', engine, if_exists='append', index=False)
taxes_df.to_sql('taxes', engine, if_exists='append', index=False)
