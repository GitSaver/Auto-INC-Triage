import streamlit as st
import pandas as pd
import numpy as np
import cx_Oracle
import os
import re
from datetime import datetime, timedelta
import pytz


# Set the path to the Oracle Instant Client directory within the Docker container
instant_client_dir = os.path.join(os.getcwd(), "instantclient_21_13")

# Add the Oracle Instant Client directory to the LD_LIBRARY_PATH environment variable
# os.environ["LD_LIBRARY_PATH"] = instant_client_dir

# Initialize the Oracle client library path
try:
    cx_Oracle.init_oracle_client(lib_dir=instant_client_dir)
except cx_Oracle.Error as e:
    st.error("DB Already Connected / Issue with DB Setup")

try:
    con = cx_Oracle.connect('PRDOMSSEL/PRDOMSSEL@localhost:11743/PRDOMS')
except cx_Oracle.Error as e:
    st.error("Error connecting to Oracle database: " + str(e))
    st.stop()

# Set the upload folder
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Function to extract numeric data from SUMMARY


def extract_order_id(SUMMARY):
    if not isinstance(SUMMARY, str):
        return None

    # Regular expression pattern to extract numeric data

    # pattern = r'\d+'
    # match = re.search(pattern, SUMMARY)
    # if match:
    #     return int(match.group())
    # else:
    #     return None

    # Main pattern: extract numeric data between "OrderID:" and "A"
    pattern_main = r'OrderID:(\d+)A'
    match_main = re.search(pattern_main, SUMMARY)
    if match_main:
        return str(match_main.group(1))

    # Additional pattern: "OrderID:<numeric>"
    pattern_alt1 = r'OrderID:(\d+)'
    match_alt1 = re.search(pattern_alt1, SUMMARY)
    if match_alt1:
        return str(match_alt1.group(1))

    # Additional pattern: "OrderID<numeric>"
    pattern_alt2 = r'OrderID(\d+)'
    match_alt2 = re.search(pattern_alt2, SUMMARY)
    if match_alt2:
        return str(match_alt2.group(1))

    # Additional pattern: "Orden"
    pattern_alt3 = r'Orden(\d+)'
    match_alt3 = re.search(pattern_alt3, SUMMARY)
    if match_alt3:
        return str(match_alt3.group(1))

    # Additional pattern: "OrderID: <numeric>"
    pattern_alt4 = r'OrderID: (\d+)A'
    match_alt4 = re.search(pattern_alt4, SUMMARY)
    if match_alt4:
        return str(match_alt4.group(1))

    # Additional pattern: "OrderID: <numeric>"
    pattern_alt5 = r'OrderId:(\d+)A'
    match_alt5 = re.search(pattern_alt5, SUMMARY)
    if match_alt5:
        return str(match_alt5.group(1))

    # Additional pattern: "OrderID:ServiceId:<numeric>A"
    pattern_alt6 = r'OrderID:ServiceId:(\d+)A'
    match_alt6 = re.search(pattern_alt6, SUMMARY)
    if match_alt6:
        return str(match_alt6.group(1))

    # If no pattern matches, return None
    return None

# Function to execute SQL query and fetch Status for ORDER_ID in tborder table


def get_status(order_id):
    try:
        cursor = con.cursor()
        query = "SELECT STATUS FROM TBORDER WHERE ORDER_UNIT_ID = :id"
        cursor.execute(query, id=str(order_id))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 'IVO'  # IVO - Invalid Order

    except cx_Oracle.Error as e:
        print("Error executing SQL query:", e)
        return 'NA'
    except TypeError:
        return 'NA'  # Handle cases where fetchone() returns None

# Function to execute SQL query and fetch maximum date for ORDER_ID


def get_max_order_date(order_id):
    try:
        cursor = con.cursor()
        # Query to get the maximum CTDB_CRE_DATETIME
        query = """
            SELECT TO_CHAR(MAX(CTDB_CRE_DATETIME), 'DD-Mon-YYYY HH24:MI:SS')
            FROM TBORDER_ACTION 
            WHERE ORDER_ID = :id 
            AND (
                (PARENT_RELATION = 'CA' AND STATUS <> 'CA' 
                AND EXISTS (SELECT ORDER_UNIT_ID 
                            FROM TBORDER_ACTION 
                            WHERE ORDER_ID = :id 
                            AND PARENT_RELATION = 'CA'))
                OR (PARENT_RELATION = 'NA')
            )
        """
        cursor.execute(query, id=str(order_id))
        result = cursor.fetchone()[0]
        cursor.close()
        return result
    except cx_Oracle.Error as e:
        st.error("Error executing SQL query: " + str(e))
        return None

# Function to fetch recent stuck case and stuck owner for ORDER_ID


def get_recent_stuck_case(order_id):
    try:
        cursor = con.cursor()
        query = """
            SELECT TSC.GROUP_CASE, TSC.STUCK_OWNER 
            FROM PRDSOMA.TBSO_KPI_WORK@OMS2SOMA KPI, PRDSOMA.TBSO_STUCK_CASE@OMS2SOMA TSC
            WHERE ORDER_ID = :id
            AND KPI.STUCKCASE_ID = TSC.ID
            ORDER BY STUCK_DATE DESC
            FETCH FIRST 1 ROW ONLY
        """
        cursor.execute(query, id=str(order_id))
        result = cursor.fetchone()
        cursor.close()
        return result if result else (None, None)
    except cx_Oracle.Error as e:
        st.error("Error executing SQL query: " + str(e))
        return None, None

# Function to process the uploaded Excel file


def process_excel(contents, location_option):
    try:
        # Read the Excel file into a DataFrame
        df = pd.read_excel(contents, engine='xlrd')

        df = df.rename(columns={'Incident ID': 'INCIDENT_ID'})
        df = df.rename(columns={'Summary': 'SUMMARY'})
        df = df.rename(columns={'Assigned Group': 'ASSIGNED_GROUP'})
        df = df.rename(columns={'Reported Date': 'REPORTED_DATE'})

        # Convert REPORTED_DATE from IST to PET timezone
        if 'REPORTED_DATE' in df.columns:
            df['REPORTED_DATE'] = pd.to_datetime(
                df['REPORTED_DATE']).dt.strftime("%d-%b-%Y %H:%M:%S")

        # Perform any data processing operations here
        # For now, let's just select the required columns
        selected_columns = ['INCIDENT_ID', 'SUMMARY',
                            'ASSIGNED_GROUP', 'REPORTED_DATE']
        processed_df = df[selected_columns]

        # Filter rows where ASSIGNED_GROUP equals 'O2A'
        processed_df = processed_df[processed_df['ASSIGNED_GROUP'] == 'O2A']

        # Extract numeric data from SUMMARY and create new column ORDER_ID
        processed_df['ORDER_ID'] = processed_df['SUMMARY'].apply(
            extract_order_id)

        # Extract Order Status
        processed_df['ORDER_STATUS'] = processed_df['ORDER_ID'].apply(
            get_status)

        # Fetch recent stuck case and stuck owner
        processed_df['RECENT_STUCK_CASE'], processed_df['STUCK_OWNER'] = zip(
            *processed_df['ORDER_ID'].apply(get_recent_stuck_case))

        if location_option == 'OFF-SHORE (INDIA)':
            # Fetch max order date for each ORDER_ID
            processed_df['MAX_ORDER_DATE'] = processed_df['ORDER_ID'].apply(
                get_max_order_date)

            # Convert REPORTED_DATE from IST to PET and create new column PERU_SITE_TIME
            processed_df['PERU_SITE_TIME'] = processed_df['REPORTED_DATE'].apply(
                convert_to_pet)

            # Calculate LIMIT_8HR and COMMENTS columns
            processed_df['LIMIT_8HR'] = processed_df.apply(
                lambda row: 'Y' if check_LIMIT_8HR_offshore(row) else 'N', axis=1)
            processed_df['COMMENTS'] = processed_df.apply(
                lambda row: get_comments(row), axis=1)

        elif location_option == 'ON-SHORE (BRAZIL)':

            # Fetch max order date for each ORDER_ID
            processed_df['MAX_ORDER_DATE'] = processed_df['ORDER_ID'].apply(
                get_max_order_date)

            # No conversion needed, keep REPORTED_DATE as it is
            processed_df['PERU_SITE_TIME'] = processed_df['REPORTED_DATE']

            # Calculate LIMIT_8HR and COMMENTS columns
            processed_df['LIMIT_8HR'] = processed_df.apply(
                lambda row: 'Y' if check_LIMIT_8HR_onshore(row) else 'N', axis=1)
            processed_df['COMMENTS'] = processed_df.apply(
                lambda row: get_comments(row), axis=1)

        # TRACKING Column
        processed_df['TRACKING'] = location_option

        # Modify the processed_df DataFrame
        processed_df['REPORTED_DATE'] = processed_df['REPORTED_DATE'].astype(
            str)
        processed_df['PERU_SITE_TIME'] = processed_df['PERU_SITE_TIME'].astype(
            str)
        processed_df['MAX_ORDER_DATE'] = processed_df['MAX_ORDER_DATE'].astype(
            str)

        # Save the processed data to a new Excel file
        output_file_path = os.path.join(UPLOAD_FOLDER, 'processed_data.xlsx')
        processed_df.to_excel(output_file_path, index=False)
        return output_file_path
    except Exception as e:
        st.error("Error processing Excel file: " + str(e))
        return None


def convert_to_pet(ist_datetime_str):
    try:
        # Define possible date formats
        date_formats = ['%d-%b-%Y %H:%M:%S',
                        '%d-%b-%Y %I:%M:%S %p', '%Y-%m-%d %H:%M:%S']
        # Attempt to parse the datetime string using each format
        for date_format in date_formats:
            try:
                ist_datetime = datetime.strptime(ist_datetime_str, date_format)
                break  # Break loop if parsing successful
            except ValueError:
                print(
                    f"Failed to parse datetime string '{ist_datetime_str}' with format '{date_format}'")
                continue  # Continue to next format if parsing fails
        # If parsing fails for all formats, raise an error
        else:
            raise ValueError("Could not parse datetime string")

        # Define time zones
        ist_tz = pytz.timezone('Asia/Kolkata')
        pet_tz = pytz.timezone('America/Lima')

        # Localize IST datetime to IST timezone
        ist_datetime = ist_tz.localize(ist_datetime)

        # Convert localized IST datetime to PET timezone
        pet_datetime = ist_datetime.astimezone(pet_tz)

        return pet_datetime.strftime('%d-%b-%Y %H:%M:%S')
    except ValueError as e:
        st.error("Error converting datetime: " + str(e))
        return None


# Function to check 8-hour limit for OFF-SHORE (INDIA)
def check_LIMIT_8HR_offshore(row):
    if pd.isnull(row['MAX_ORDER_DATE']):
        return False
    else:
        pet_datetime = datetime.strptime(
            row['PERU_SITE_TIME'], '%d-%b-%Y %H:%M:%S')
        max_order_date = datetime.strptime(
            row['MAX_ORDER_DATE'], '%d-%b-%Y %H:%M:%S')
        # max_order_date = row['MAX_ORDER_DATE']
        return pet_datetime > (max_order_date + timedelta(hours=8))


# Function to check 8-hour limit for ON-SHORE (BRAZIL)
def check_LIMIT_8HR_onshore(row):
    if pd.isnull(row['MAX_ORDER_DATE']):
        return False
    else:
        pet_datetime = datetime.strptime(
            row['PERU_SITE_TIME'], '%d-%b-%Y %H:%M:%S')
        max_order_date = datetime.strptime(
            row['MAX_ORDER_DATE'], '%d-%b-%Y %H:%M:%S')
        # max_order_date = row['MAX_ORDER_DATE']
        return pet_datetime > (max_order_date + timedelta(hours=8))

# Function to get comments


def get_comments(row):
    if row['ORDER_STATUS'] == 'IVO':
        return 'Invalid Order Id'
    elif row['LIMIT_8HR'] == 'N':
        return 'REJECT INC TO TEF AS 8-HR CRITERIA ISN\'T FULFILLED'
    elif row['ORDER_STATUS'] == 'DO':
        return f"Order is already completed in OMS: Last_Stuck at - {row['RECENT_STUCK_CASE']}"
    elif row['ORDER_STATUS'] == 'CA':
        return f"Order is already cancelled in OMS: Last_Stuck at - {row['RECENT_STUCK_CASE']}"
    elif row['STUCK_OWNER'] in ['TELEFONICA IT', 'TELEFONICA CSR']:
        return f"Possible Reject to TEF: {row['RECENT_STUCK_CASE']}"
    else:
        return f"O2A Check Required: {row['RECENT_STUCK_CASE']}"

# Function to check if table exists in the database


def table_exists(table_name):
    try:
        cursor = con.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM = 1")
        cursor.close()
        return True
    except cx_Oracle.Error as e:
        return False

# Function to create the table AUTO_INC_TRIAGE if it doesn't exist


def create_table_if_not_exists():
    if not table_exists('AUTO_INC_TRIAGE'):
        try:
            cursor = con.cursor()
            cursor.execute("""
                CREATE TABLE AUTO_INC_TRIAGE (
                    INCIDENT_ID VARCHAR2(255) PRIMARY KEY,
                    SUMMARY VARCHAR2(1000),
                    ASSIGNED_GROUP VARCHAR2(50),
                    REPORTED_DATE DATE,
                    ORDER_ID VARCHAR2(50),
                    ORDER_STATUS VARCHAR2(10),
                    RECENT_STUCK_CASE VARCHAR2(500),
                    STUCK_OWNER VARCHAR2(30),
                    MAX_ORDER_DATE DATE,
                    PERU_SITE_TIME DATE,
                    LIMIT_8HR VARCHAR2(10),
                    COMMENTS VARCHAR2(500),
                    TRACKING VARCHAR2(50)
                )
            """)
            cursor.close()
            con.commit()
            st.success("Table AUTO_INC_TRIAGE created successfully!")
        except cx_Oracle.Error as e:
            st.error("Error creating table: " + str(e))

# Function to export data to table AUTO_INC_TRIAGE
# def export_data_to_table(df):
#     try:
#         cursor = con.cursor()

#         df = df.where(pd.notnull(df), None)

#         # Convert VARCHAR columns to DATE format when exporting to Oracle
#         df['REPORTED_DATE'] = pd.to_datetime(df['REPORTED_DATE'], format='%d-%b-%Y %H:%M:%S')
#         df['PERU_SITE_TIME'] = pd.to_datetime(df['PERU_SITE_TIME'], format='%d-%b-%Y %H:%M:%S')
#         df['MAX_ORDER_DATE'] = pd.to_datetime(df['MAX_ORDER_DATE'], format='%d-%b-%Y %H:%M:%S')

#         columns = df.columns.tolist()
#         cursor.prepare(f"INSERT INTO AUTO_INC_TRIAGE ({','.join(columns)}) VALUES ({','.join([':' + str(i+1) for i in range(len(columns))])})")
#         cursor.executemany(None, df.values.tolist())
#         cursor.close()
#         con.commit()
#         st.success("Data exported to table AUTO_INC_TRIAGE successfully!")
#     except cx_Oracle.Error as e:
#         st.error("Error exporting data to table: " + str(e))


def export_data_to_table(df):
    try:
        cursor = con.cursor()

        # Convert date columns to datetime objects, set to None if they are null
        df['REPORTED_DATE'] = df['REPORTED_DATE'].apply(
            lambda x: pd.to_datetime(x, format='%d-%b-%Y %H:%M:%S', errors='coerce') if pd.notnull(x) else None)
        df['PERU_SITE_TIME'] = df['PERU_SITE_TIME'].apply(
            lambda x: pd.to_datetime(x, format='%d-%b-%Y %H:%M:%S', errors='coerce') if pd.notnull(x) else None)
        df['MAX_ORDER_DATE'] = df['MAX_ORDER_DATE'].apply(
            lambda x: pd.to_datetime(x, format='%d-%b-%Y %H:%M:%S', errors='coerce') if pd.notnull(x) else None)

        # Replace NaN with None to handle null values correctly
        df = df.where(pd.notnull(df), None)

        # Prepare data for insertion, setting None for all blank entries
        data_to_insert = []
        for row in df.itertuples(index=False, name=None):
            processed_row = tuple(None if pd.isna(x) else x for x in row)
            data_to_insert.append(processed_row)

        # Define the insert query
        columns = df.columns.tolist()
        insert_query = f"""
            INSERT INTO AUTO_INC_TRIAGE ({','.join(columns)}) 
            VALUES ({','.join([':' + str(i+1) for i in range(len(columns))])})
        """

        # Execute the insert query
        cursor.executemany(insert_query, data_to_insert)
        con.commit()
        cursor.close()
        st.success("Data exported to table AUTO_INC_TRIAGE successfully!")
    except cx_Oracle.Error as e:
        st.error("Error exporting data to table: " + str(e))

# Authentication function


def check_password():
    def password_entered():
        if st.session_state["username"] == "tefperu" and st.session_state["password"] == "peru2024":
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Username", key="username")
        st.text_input("Password", type="password", key="password")
        st.button("Login", on_click=password_entered)
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Username", key="username")
        st.text_input("Password", type="password", key="password")
        st.error("Username or password is incorrect")
        st.button("Login", on_click=password_entered)
        return False
    else:
        return True


# Main part of the app
if check_password():
    # Streamlit app layout
    st.title('AUTO INC TRIAGE')
    st.write("Developed By - TEF PERU O2A TEAM")
    # File uploader
    file = st.file_uploader("Upload Excel File", type=["xlsx", "xls"])
    if file is not None:
        location_option = st.selectbox(
            "Select Location", ['OFF-SHORE (INDIA)', 'ON-SHORE (BRAZIL)'])
        export_to_table = st.checkbox("Export Data to Table - AUTO_INC_TRIAGE")
        if st.button("Process File"):
            processed_file_path = process_excel(file, location_option)
            if processed_file_path:
                st.success("File processed successfully!")
                st.write("Transformed Data Output:")
                df = pd.read_excel(processed_file_path)
                df.index = np.arange(1, len(df) + 1)
                st.write(df)
                if export_to_table:
                    create_table_if_not_exists()
                    export_data_to_table(df)
