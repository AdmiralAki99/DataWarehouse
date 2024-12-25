import pandas as pd
import numpy as np
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

## Predefined Subsets From Datasets For ETL
HOSPITAL_SUBSET = ['Provider CCN','Hospital Name','State Code','County','City','Rural Versus Urban','Medicare CBSA Number','Medicaid Charges','Stand-Alone CHIP Charges','Total Costs','Cost To Charge Ratio','Net Income','Net Revenue from Medicaid','Net Revenue from Stand-Alone CHIP','Net Income from Service to Patients','Net Revenue from Medicaid','Total Other Expenses','Fiscal Year Begin Date','Fiscal Year End Date']
DEMOGRAPHIC_SUBSET = ['NAICS','NAICS_TITLE','H_PCT10','H_PCT25','H_MEDIAN','H_PCT75','H_PCT90','A_PCT10','A_PCT25','A_MEDIAN','A_PCT75','A_PCT90','H_MEAN','A_MEAN','AREA_TYPE','AREA_TITLE','OCC_CODE','OCC_TITLE','TOT_EMP','EMP_PRSE','OWN_CODE','PRIM_STATE']

def connect_to_warehouse():
# Creating the database connection to run the SQL queries
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'UID=sa;'
        f'PWD={os.getenv("PASSWORD")};'
        'DATABASE=HealthcareCostDatabase;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()
    return cursor

# Creating methods to make the extraction process much easier
def read_hospital_data(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    return df

def read_demographic_data(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path)
    return df

def get_subset(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    return df[subset]

# Create an umbrella method for all the extraction processes in the ETL process
def extract(file_path: str, subset: list) -> pd.DataFrame:
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)
    df = df[subset]
    df.dropna(inplace=True)
    return df

# Creating methods to make the extraction process much easier
def transform_hospital_data(df: pd.DataFrame) -> pd.DataFrame:
    # Need to transform the data
    df['Medicaid Charges'] = df['Medicaid Charges'].replace('[\$,]', '', regex=True).astype(float)
    df['Stand-Alone CHIP Charges'] = df['Stand-Alone CHIP Charges'].replace('[\$,]', '', regex=True).astype(float)
    df['Total Costs'] = df['Total Costs'].replace('[\$,]', '', regex=True).astype(float)
    df['Net Income'] = df['Net Income'].replace('[\$,]', '', regex=True).astype(float)
    df['Net Revenue from Medicaid'] = df['Net Revenue from Medicaid'].replace('[\$,]', '', regex=True).astype(float)
    df['Net Revenue from Stand-Alone CHIP'] = df['Net Revenue from Stand-Alone CHIP'].replace('[\$,]', '', regex=True).astype(float)
    df['Net Income from Service to Patients'] = df['Net Income from Service to Patients'].replace('[\$,]', '', regex=True).astype(float)
    df['Net Revenue from Medicaid'] = df['Net Revenue from Medicaid'].replace('[\$,]', '', regex=True).astype(float)
    df['Total Other Expenses'] = df['Total Other Expenses'].replace('[\$,]', '', regex=True).astype(float)
    
    # Renaming the ID column to match the other dataset
    df.rename(
        columns={
            "Provider CCN": "Hospital_ID",
            "Hospital Name": "Hospital_Name",
        }
    )
    # Removing duplicate columns that may have been created during the transformation process
    df = df.loc[:, ~df.columns.duplicated()]
    # Remove duplicate rows in the dataset that were made due to the data entry process where only the ID is made but the rest of the data is left empty or is the same exact record
    df.drop_duplicates(inplace=True)
    
    # Creating new columns that will be used for analysis
    df['Total Revenue'] = df['Net Income from Service to Patients'] + df['Net Revenue from Medicaid'] + df['Net Revenue from Stand-Alone CHIP']
    df['Profit Margin'] = df['Net Income from Service to Patients'] / df['Total Revenue']
    df['Cost To Revenue Ratio'] = df['Total Costs'] / df['Total Revenue']
    
    # Resetting the index
    df.reset_index(drop=True, inplace=True)
    
    return df

def transform_demographic_data(df: pd.DataFrame, year:int) -> pd.DataFrame:
    df.replace({'#': np.nan, "*": np.nan}, inplace=True)
    # df = df.apply(pd.to_numeric, errors='ignore')
    
    # Need to transform the data and fill the numeric columns with the median value
    for column in df.columns:
        if df[column].dtype == np.float64:
            df[column] = df[column].fillna(df[column].median())
            
    # Add a column for the year
    df['Year'] = year
    # Resetting the index
    df.reset_index(drop=True, inplace=True)
    
    return df

def create_surrogate_key(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df[column_name] = range(1, len(df) + 1)
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    return df
    
# Creating the dimension dataframes so that we can load them into the database

# Adding this function as a measure to ensure that the data keys are unique and dont clash with each other
def get_max_id(cursor: pyodbc.Cursor, table_name: str, column_name: str) -> int:
      query = f"SELECT MAX({column_name}) AS max_id FROM {table_name}"
      cursor.execute(query)
      result = cursor.fetchone()
      if result[0] is None:
         return 0
      return result[0]
      

def create_hospital_dimension(df: pd.DataFrame) -> pd.DataFrame:
   hospital_dim = pd.DataFrame({
      'Provider_CCN' : df['Provider CCN'],
      'Hospital_Name' : df['Hospital Name'],
   })
   
   hospital_dim = hospital_dim.drop_duplicates(subset=['Provider_CCN'])

   return hospital_dim

def create_geographic_info_dimension(df: pd.DataFrame) -> pd.DataFrame:
   geographic_info_dim = pd.DataFrame({
      'State_Code' : df['State Code'],
      'County' : df['County'],
      'City' : df['City'],
      'RuralVsUrban' : df['Rural Versus Urban'],
   })
   
   # Dropping duplicates in the dataset
   geographic_info_dim = geographic_info_dim.drop_duplicates(subset=['State_Code','County','City'])

   return geographic_info_dim

def create_date_dimension(df: pd.DataFrame) -> pd.DataFrame:
   date_dim = pd.DataFrame({
      # 'Date_Key' : range(1, 1+len(df['Fiscal Year Begin Date'].unique())),
      # 'Date' : pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'].unique())),
      'Year': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'].unique())).dt.year,
      'Quarter': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'].unique())).dt.quarter,
      'Month': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'].unique())).dt.month,
      'Day': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'].unique())).dt.day
   })
   
   date_dim.reset_index(drop=True, inplace=True)
   
   return date_dim

def create_occupational_info_dimension(df: pd.DataFrame) -> pd.DataFrame:
   occupational_info_dim = pd.DataFrame({
      # 'DemographicID': range(1, 1+len(df)),
      'NAICS' : df['NAICS'],
      'OccupationTitle' : df['OCC_TITLE'],
      'IndustryGroup' : df['OCC_CODE'],
      'AreaName': df['AREA_TITLE'],
      'AreaCode': df['PRIM_STATE'],
   })
   
   return occupational_info_dim

def create_financial_measurement(df: pd.DataFrame) -> pd.DataFrame:
   financial_measurement = pd.DataFrame({
   #  'ID' : range(1, 1+len(df)),
    'ProviderCCN': df['Provider CCN'],
    'MedicaidCharges': df['Medicaid Charges'],
    'StandAloneCHIPCharges': df['Stand-Alone CHIP Charges'],
    'TotalIncome': df['Total Revenue'],
    'Profit': df['Total Revenue'] - df['Total Costs'],
    'NetIncome': df['Net Income'],
    'Date': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])),
    'StateCode': df['State Code'],
   })
   
   return financial_measurement

def create_demographic_measurement(df: pd.DataFrame) -> pd.DataFrame:
   demographic_measurement = pd.DataFrame({
      # 'ID' : range(1, 1+len(df)),
      'NAICS': df['NAICS'],
      'OccupationTitle': df['OCC_TITLE'],
      'AreaName': df['AREA_TITLE'],
      'AreaCode': df['PRIM_STATE'],
   })
   
   return demographic_measurement

def create_health_care_cost_staging_table(df: pd.DataFrame) -> pd.DataFrame:
   health_care_cost_staging_table = pd.DataFrame({
      # 'CostID': range(1, 1+len(df)),
      'Year': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])).dt.year,
      'Quarter': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])).dt.quarter,
      'Month': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])).dt.month,
      'Day': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])).dt.day,
      'HospitalName': df['Hospital Name'],
      'ProviderCCN': df['Provider CCN'],
      'StateCode': df['State Code'],
      'County': df['County'],
      'City': df['City'],
      'MedicaidCharges': df['Medicaid Charges'],
      'StandAloneCHIPCharges': df['Stand-Alone CHIP Charges'],
      'TotalCosts': df['Total Costs'],
      'NetIncome': df['Net Income'],
      'NetRevenueFromMedicaid': df['Net Revenue from Medicaid'],
      'NetRevenueFromStandAloneCHIP': df['Net Revenue from Stand-Alone CHIP'],
      'NetIncomeFromServiceToPatients': df['Net Income from Service to Patients'],
      'CostToChargeRatio': df['Cost To Charge Ratio'],
   })
   
   return health_care_cost_staging_table

def create_financial_measurement_staging_table(df: pd.DataFrame) -> pd.DataFrame:
   financial_staging_table = pd.DataFrame({
      'HospitalName' : df['Hospital Name'],
      'StateCode' : df['State Code'],
      'County' : df['County'],
      'City' : df['City'],
      'Year': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])).dt.year,
      'Quarter': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])).dt.quarter,
      'Day': pd.Series(pd.to_datetime(df['Fiscal Year Begin Date'])).dt.day,
      'TotalIncome': df['Total Revenue'],
      'Profit': df['Total Revenue'] - df['Total Costs'],
      'ProfitMargin': df['Profit Margin'],
   })
   
   return financial_staging_table

def create_demographic_measurement_staging_table(df: pd.DataFrame) -> pd.DataFrame:
   demographic_staging_table = pd.DataFrame({
      'NAICS': df['NAICS'],
      'OccupationTitle': df['OCC_TITLE'],
      'AreaName': df['AREA_TITLE'],
      'AreaCode': df['PRIM_STATE'],
      'Year': df['Year'],
      'MeanHourlyWage' : df['H_MEAN'],
      'MeanWage': df['A_MEAN'],
      'MedianAnnualWage': df['A_MEDIAN'],
      'PCT25AnnualWage': df['A_PCT25'],
      'PCT25HourlyWage': df['H_PCT25'],
      'PCT75AnnualWage': df['A_PCT75'],
      'PCT75HourlyWage': df['H_PCT75'],
   })
   
   return demographic_staging_table


def merge_date_dimension_to_financial_fact(fact_table: pd.DataFrame, date_dimension: pd.DataFrame) -> pd.DataFrame:
   fact_table = fact_table.merge(date_dimension[['Date']], left_on='Date', right_on='Date', how='inner')[['ProviderCCN','MedicaidCharges','StandAloneCHIPCharges','TotalIncome','Profit','NetIncome','Date']]
   return fact_table

def merge_health_care_cost_to_financial_fact(fact_table: pd.DataFrame, health_care_cost_dimension: pd.DataFrame) -> pd.DataFrame:
   fact_table = fact_table.merge(health_care_cost_dimension[['NetIncome','MedicaidCharges','StandAloneCHIPCharges']], how='left')[['CostID','Date_Key','ProviderCCN','TotalIncome','Profit']]
   return fact_table

def merge_occupational_info_to_demographic_measurement(demographic_measurement: pd.DataFrame, occupational_info_dimension: pd.DataFrame) -> pd.DataFrame:
   demographic_measurement = demographic_measurement.merge(occupational_info_dimension[['NAICS','OccupationTitle','IndustryGroup','AreaName','AreaCode','MeanWage','MeanHourlyWage','MedianAnnualWage','PCT25AnnualWage','PCT75AnnualWage','PCT25HourlyWage','PCT75HourlyWage']], how='left')[['OccupationTitle','AreaName','AreaCode']]
   return demographic_measurement
    
# Create an umbrella function that does all the transofrmations in one call
def transform_data(hospital_dataframe: pd.DataFrame, demographic_dataframe: pd.DataFrame,year:int) -> tuple:    
    df_hospital = transform_hospital_data(hospital_dataframe)
    df_demographic = transform_demographic_data(demographic_dataframe,year)
    
    # Creating Surrogate Keys for the dataframes
    df_hospital = create_surrogate_key(df_hospital, 'Hospital_ID')
    df_demographic = create_surrogate_key(df_demographic, 'Demographic_ID')
    
    # Now creating the dimension dataframes
    hospital_dim = create_hospital_dimension(df_hospital)
    date_dim = create_date_dimension(df_hospital)
    geographic_info_dimension = create_geographic_info_dimension(df_hospital)
    occupational_info_dim = create_occupational_info_dimension(df_demographic)
    
    # Creating the staging tables
    financial_staging_table = create_financial_measurement_staging_table(df_hospital)
    demographic_staging_table = create_demographic_measurement_staging_table(df_demographic)
    health_care_cost_staging_table = create_health_care_cost_staging_table(df_hospital)
    
    # Returning all the dimensions and fact tables for the ETL Process
    return hospital_dim, date_dim, occupational_info_dim,geographic_info_dimension,financial_staging_table,demographic_staging_table,health_care_cost_staging_table


# Create the load functions for the ETL process

# Creating the tables in the database, but needs to take care of initial load as well as incremental load
def check_if_tables_exist(cursor: pyodbc.Cursor,table_name: str) -> bool:
    # First we need to check if the the tables exist in the database
    query = f"""
        SELECT CASE WHEN EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'
        ) THEN 1 ELSE 0 END AS Result
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result[0] == 1:
        return True
    else:
        return False
    
# If the tables dont exist then the tables need to be created in the database
def create_hospital_dimension_table(cursor: pyodbc.Cursor) -> None:
    query = """
        CREATE TABLE HospitalInfo (
            HospitalID INT IDENTITY(1,1) PRIMARY KEY,
            Provider_CCN INT,
            Hospital_Name VARCHAR(255),
            EffectiveDate DATE DEFAULT GETDATE(), -- Creating the default date to the current date, will get changed when the record is updated
            ExpirationDate DATE DEFAULT '9999-12-31', -- Expiration date which will never be reached
            IsCurrent BIT DEFAULT 1 -- SCD Varaible to show if the record is the current record or not
        )
    """
    cursor.execute(query)
    cursor.commit()
    
    print('Hospital Dimension Table Created Successfully!')
    
def create_geographic_info_dimension_table(cursor: pyodbc.Cursor) -> None:
    query = """
        CREATE TABLE GeographicInfo (
            GeographicID INT IDENTITY(1,1) PRIMARY KEY,
            State_Code VARCHAR(10),
            County VARCHAR(255),
            City VARCHAR(255),
            RuralVsUrban VARCHAR(50),
            EffectiveDate DATE DEFAULT GETDATE(),
            ExpirationDate DATE DEFAULT '9999-12-31',
            IsCurrent BIT DEFAULT 1
        )
    
    """
    
    cursor.execute(query)
    cursor.commit()
    
def create_date_dimension_table(cursor: pyodbc.Cursor) -> None:
    query = """
        Create Table DateDimension (
            DateID INT IDENTITY(1,1) PRIMARY KEY,
            Year INT,
            Quarter INT,
            Month INT,
            Day INT,
        )
    """
    
    cursor.execute(query)
    cursor.commit()
    
    print('Date Dimension Table Created Successfully!')
    
def create_occupational_info_dimension_table(cursor: pyodbc.Cursor) -> None:
    query = """
        CREATE TABLE OccupationalInfo(
            DemographicID INT IDENTITY(1,1) PRIMARY KEY,
            NAICS INT,
            OccupationTitle VARCHAR(255),
            IndustryGroup VARCHAR(255),
            AreaName VARCHAR(255),
            AreaCode VARCHAR(10),
            EffectiveDate DATE DEFAULT GETDATE(),
            ExpirationDate DATE DEFAULT '9999-12-31',
            IsCurrent BIT DEFAULT 1
        )
    """
    cursor.execute(query)
    cursor.commit()
    
    print('Occupational Info Dimension Table Created Successfully!')
    
def create_financial_measurement_table(cursor: pyodbc.Cursor) -> None:
    query = """
        CREATE TABLE FinancialMeasurement(
            ID INT IDENTITY(1,1) PRIMARY KEY,
            HospitalID INT DEFAULT -1,
            GeographicID INT DEFAULT -1,
            Date_Key INT DEFAULT -1,
            TotalIncome FLOAT,
            Profit FLOAT,
            ProfitMargin FLOAT,
        )
    """

    cursor.execute(query)
    cursor.commit()
    
    print('Financial Measurement Table Created Successfully!')
    
def create_demographic_measurement_table(cursor: pyodbc.Cursor) -> None:
    query = """
        CREATE TABLE DemographicMeasurement(
            ID INT IDENTITY(1,1) PRIMARY KEY,
            DemographicID INT DEFAULT -1,
            DateID INT DEFAULT -1,
            MeanHourlyWage FLOAT,
            MeanWage FLOAT,
            MedianAnnualWage FLOAT,
            PCT25AnnualWage FLOAT,
            PCT25HourlyWage FLOAT,
            PCT75AnnualWage FLOAT,
            PCT75HourlyWage FLOAT,
        )
    """
    
    cursor.execute(query)
    cursor.commit()
    
    print('Demographic Measurement Table Created Successfully!')
    
def create_health_care_cost_table(cursor: pyodbc.Cursor) -> None:
    query = """
        CREATE TABLE HealthCareCost(
            ID INT IDENTITY(1,1) PRIMARY KEY,
            DateID INT DEFAULT -1,
            HospitalID INT DEFAULT -1,
            GeographicID INT DEFAULT -1,
            MedicaidCharges FLOAT,
            StandAloneCHIPCharges FLOAT,
            TotalCosts FLOAT,
            NetIncome FLOAT,
            NetRevenueFromMedicaid FLOAT,
            NetRevenueFromStandAloneCHIP FLOAT,
            NetIncomeFromServiceToPatients FLOAT,
            CostToChargeRatio FLOAT,
        )
    
    """
    
    cursor.execute(query)
    cursor.commit()
    
    print('Health Care Cost Table Created Successfully!')
    
def create_staging_table_for_financial_measurement(cursor: pyodbc.Cursor) -> None:
    query = """
            CREATE TABLE FinancialMeasurementStaging(
                HospitalName VARCHAR(255),
                StateCode VARCHAR(10),
                County VARCHAR(255),
                City VARCHAR(255),
                Year INT,
                Quarter INT,
                Day INT,
                TotalIncome FLOAT,
                Profit FLOAT,
                ProfitMargin FLOAT,
            )
    """
    cursor.execute(query)
    cursor.commit()
    
    print('Financial Measurement Staging Table Created Successfully!')
    
def create_staging_table_for_demographic_measurement(cursor: pyodbc.Cursor) -> None:
    query = """
            CREATE TABLE DemographicMeasurementStaging(
                NAICS INT,
                OccupationTitle VARCHAR(255),
                AreaName VARCHAR(255),
                AreaCode VARCHAR(10),
                Year INT,
                MeanHourlyWage FLOAT,
                MeanWage FLOAT,
                MedianAnnualWage FLOAT,
                PCT25AnnualWage FLOAT,
                PCT25HourlyWage FLOAT,
                PCT75AnnualWage FLOAT,
                PCT75HourlyWage FLOAT,
            )
    
    """
    
    cursor.execute(query)
    cursor.commit()
    
    print('Demographic Measurement Staging Table Created Successfully!')
    
def create_staging_table_for_health_care_cost(cursor: pyodbc.Cursor) -> None:
    query = """
        CREATE TABLE HealthCareCostStaging(
            Year INT,
            Quarter INT,
            Month INT,
            Day INT,
            ProviderCCN INT,
            HospitalName VARCHAR(255),
            StateCode VARCHAR(10),
            County VARCHAR(255),
            City VARCHAR(255),
            MedicaidCharges FLOAT,
            StandAloneCHIPCharges FLOAT,
            TotalCosts FLOAT,
            NetIncome FLOAT,
            NetRevenueFromMedicaid FLOAT,
            NetRevenueFromStandAloneCHIP FLOAT,
            NetIncomeFromServiceToPatients FLOAT,
            CostToChargeRatio FLOAT,
        )
    """
    
    cursor.execute(query)
    cursor.commit()
    
    print('Health Care Cost Staging Table Created Successfully!')
    
def verify_tables_in_database(cursor: pyodbc.Cursor) -> None:
    if not check_if_tables_exist(cursor, 'HospitalInfo'):
        create_hospital_dimension_table(cursor)
    
    if not check_if_tables_exist(cursor, 'DateDimension'):
        create_date_dimension_table(cursor)
    
    if not check_if_tables_exist(cursor, 'OccupationalInfo'):
        create_occupational_info_dimension_table(cursor)
    
    if not check_if_tables_exist(cursor, 'FinancialMeasurement'):
        create_financial_measurement_table(cursor)
    
    if not check_if_tables_exist(cursor, 'DemographicMeasurement'):
        create_demographic_measurement_table(cursor)
        
    if not check_if_tables_exist(cursor, 'HealthCareCost'):
        create_health_care_cost_table(cursor)
        
    if not check_if_tables_exist(cursor, 'GeographicInfo'):
        create_geographic_info_dimension_table(cursor)
        
    if not check_if_tables_exist(cursor, 'FinancialMeasurementStaging'):
        create_staging_table_for_financial_measurement(cursor)
        
    if not check_if_tables_exist(cursor, 'DemographicMeasurementStaging'):
        create_staging_table_for_demographic_measurement(cursor)
        
    if not check_if_tables_exist(cursor, 'HealthCareCostStaging'):
        create_staging_table_for_health_care_cost(cursor)
        
# After veryfying the tables in the database, we can now load the data into the tables
def load_data_into_table(cursor: pyodbc.Cursor, table_name: str,df: pd.DataFrame, is_scd: bool = True,scd_num: int = 1) -> None:
    # First we need to check if the table is empty or not
    # If it is empty then it is initial load and there are no problems with all the SCD's
    # If it is not empty then staging tables need to be created and the SCD's need to be handled
    query = f"""
        SELECT COUNT(*) FROM {table_name}
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result[0] == 0:
        # This is initial load
        # We can directly load the data into the table
        query = f"""
            INSERT INTO {table_name} ({', '.join(df.columns)})
            VALUES ({','.join(['?']*len(df.columns))})
        """
        
        cursor.executemany(query, df.values.tolist())
        cursor.commit()
    else:
        # This is incremental load
        # We need to create the staging tables and handle the SCD's
        # Check which records are new and which records are updated
        if is_scd:
            if scd_num == 0:
                # SCD Type 0 is where the record is inserted and there are no changes allowed
                pass
            if scd_num == 1:
                # SCD Type 1 is where the record is overwritten
                # We need to overwrite the existing records in the table
                if table_name == 'DateDimension':
                    # Get all the records from the table
                    query = f"""
                        SELECT * FROM {table_name}
                    """
                    
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    result = cursor.fetchall()
                    
                    # Create a dataframe from the result
                    current_df = pd.DataFrame.from_records(result,columns=columns)
                    
                    # Check which records are new and which records are updated
                    # Check with Year,Quarter,Month,Day
                    df['unique_ID'] = df['Year'].astype(str)+"_"+df['Quarter'].astype(str)+"_"+df['Month'].astype(str)+"_"+df['Day'].astype(str)
                    current_df['unique_ID'] = current_df['Year'].astype(str)+"_"+current_df['Quarter'].astype(str)+"_"+current_df['Month'].astype(str)+"_"+current_df['Day'].astype(str)
                    
                    new_records = df[~df['unique_ID'].isin(current_df['unique_ID'])]
                    updated_records = df[df['unique_ID'].isin(current_df['unique_ID'])]
                    
                    new_records = new_records.drop(columns=['unique_ID'])
                    updated_records = updated_records.drop(columns=['unique_ID'])
                    
                    # Load the new records into the table
                    print("Records to be inserted into Date Dimension: ",len(new_records))
                    if len(new_records) > 0:
                        # If the new records are greater than 0 then we need to load the new records into the table else there is an error
                        for _, row in new_records.iterrows():
                            print(f'NEW DATE RECORD : YEAR: {row["Year"]}, QUARTER: {row["Quarter"]}, MONTH: {row["Month"]}, DAY: {row["Day"]}')
                        query = f"""
                            INSERT INTO {table_name} ({', '.join(new_records.columns)})
                            VALUES ({','.join(['?']*len(new_records.columns))})
                        """
                        
                        cursor.executemany(query, new_records.values.tolist())
                        cursor.commit()
                        
                    # Update the existing records in the table
                    print("Records to be updated in Date Dimension: ",len(updated_records))
                    if len(updated_records) > 0:
                        # Check if any columns have changed
                        for _, row in updated_records.iterrows():
                            # Need to check for year, quarter, month, day
                            if current_df[(current_df['Year'] == row['Year']) & (current_df['Quarter'] == row['Quarter']) & (current_df['Month'] == row['Month']) & (current_df['Day'] == row['Day'])].empty:
                                pass
                            else:
                                current_row = current_df[(current_df['Year'] == row['Year']) & (current_df['Quarter'] == row['Quarter']) & (current_df['Month'] == row['Month']) & (current_df['Day'] == row['Day'])].iloc[0]
                                changed_columns = []
                                for column in updated_records.columns:
                                    if row[column] != current_row[column]:
                                        changed_columns.append(column)
                                        
                                print(f'MATCHED DATE RECORD: YEAR: {row["Year"]}, QUARTER: {row["Quarter"]}, MONTH: {row["Month"]}, DAY: {row["Day"]}')
                                    
                                if len(changed_columns) > 0:
                                    # There are changes that need overwriting
                                    print(f'UPDATED RECORDS: YEAR: {row["Year"]}, QUARTER: {row["Quarter"]}, MONTH: {row["Month"]}, DAY: {row["Day"]}')
                                    update_query = f"""
                                        UPDATE {table_name}
                                        SET {', '.join([f'{column} = ?' for column in changed_columns])}
                                        WHERE Year = ? AND Quarter = ? AND Month = ? AND Day = ?
                                    """
                                
                                    cursor.execute(update_query, tuple(row[changed_columns]) + tuple(row[['Year','Quarter','Month','Day']]))
                                    cursor.commit()
                            
                    print("Updated Date Dimension Table Successfully!")
                
            if scd_num == 2:
                
                if table_name == 'HospitalInfo':
                    # Get all the records from the table
                    query = f"""
                        SELECT * FROM {table_name}
                    """
        
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    result = cursor.fetchall()
        
                    # Create a dataframe from the result
                    current_df = pd.DataFrame.from_records(result,columns=columns)
        
                    # Check which records are new and which records are updated
                    hospital_new_records = df[~df['Provider_CCN'].isin(current_df['Provider_CCN'])]
                    hospital_updated_records = df[df['Provider_CCN'].isin(current_df['Provider_CCN'])]
        
                    # Load the new records into the table
                    if len(hospital_new_records) > 0:
                        # If the new records are greater than 0 then we need to load the new records into the table else there is an error
                        for _, row in hospital_new_records.iterrows():
                            print(f'NEW HOSPITAL INFO RECORD : PROVIDER CCN: {row["Provider_CCN"]}, HOSPITAL NAME: {row["Hospital_Name"]}')
                            
                        query = f"""
                            INSERT INTO {table_name} ({', '.join(hospital_new_records.columns)})
                            VALUES ({','.join(['?']*len(hospital_new_records.columns))})
                        """
        
                        cursor.executemany(query, hospital_new_records.values.tolist())
        
                        cursor.commit()
        
                    # Update the existing records in the table
                    # We need to handle the SCD's here
                    # We need to set the IsCurrent to 0 for the existing records
                    # We need to set the ExpirationDate to the current date for the existing records
                    # We need to insert the updated records as new records in the table
                    # We need to set the EffectiveDate to the current date for the updated records
                    # We need to set the IsCurrent to 1 for the updated records
                    # We need to set the ExpirationDate to '9999-12-31' for the updated records
                    if len(hospital_updated_records) > 0:
                        # Check if any columns have changed
                        added_records = []
                        for _, row in hospital_updated_records.iterrows():
                            # Need to check for provider ccn
                            current_row = current_df[current_df['Provider_CCN'] == row['Provider_CCN']].iloc[0]
                            if current_row.empty:
                                continue
                            else:
                                print(f'MATCHED HOSPITAL INFO RECORD : PROVIDER CCN: {row["Provider_CCN"]}, HOSPITAL NAME: {row["Hospital_Name"]} , MATCHED PROVIDER CCN: {current_row["Provider_CCN"]}, HOSPITAL NAME: {current_row["Hospital_Name"]}')
                                changed_columns = []
                                for column in hospital_updated_records.columns:
                                    if row[column] != current_row[column]:
                                        changed_columns.append(column)
        
                                if len(changed_columns) > 0:
                                    # First Making the Current Bit 0 and Expiring the record
                                    print(f'UPDATED HOSPITAL INFO RECORD : PROVIDER CCN: {row["Provider_CCN"]}, HOSPITAL NAME: {row["Hospital_Name"]}, CHANGED COLUMNS: {changed_columns}, MATCHED PROVIDER CCN: {current_row["Provider_CCN"]}, HOSPITAL NAME: {current_row["Hospital_Name"]}')
                                    update_query = f"""
                                        UPDATE {table_name}
                                        SET IsCurrent = 0, ExpirationDate = GETDATE()
                                        WHERE Provider_CCN = ?
                                    """
                                    cursor.execute(update_query, (row['Provider_CCN'],))
                                    cursor.commit()
        
                                    # Insert the updated record with new data
                                    insert_query = f"""
                                        INSERT INTO {table_name} ({', '.join(hospital_updated_records.columns)})
                                        VALUES ({','.join(['?']*len(hospital_updated_records.columns))})
                                    """
                                    cursor.execute(insert_query, tuple(row))
                                    cursor.commit()
                                
                                    # Add this updated row to the added records dataframe
                                    added_records.append(row)
                        
                        hospital_updated_records = pd.DataFrame(added_records,columns=hospital_updated_records.columns)
                        print('Records Updated in HospitalInfo Dimension: ',len(hospital_updated_records))
                        
                                        
                    print("Updated Hospital Dimension Table Successfully!")
                elif table_name == 'GeographicInfo':
                    # Get all the records from the table
                    query = f"""
                        SELECT * FROM {table_name}
                    """
                    
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    result = cursor.fetchall()
                    
                    # Create a dataframe from the result
                    current_df = pd.DataFrame.from_records(result,columns=columns)
                    
                    # Check which records are new and which records are updated
                    df['unique_ID'] = df['State_Code']+"_"+df['County']+"_"+df['City']
                    current_df['unique_ID'] = current_df['State_Code']+"_"+current_df['County']+"_"+current_df['City']
                    
                    geographic_new_records = df[~df['unique_ID'].isin(current_df['unique_ID'])]
                    geographic_updated_records = df[df['unique_ID'].isin(current_df['unique_ID'])]
                    
                    geographic_new_records = geographic_new_records.drop(columns=['unique_ID'])
                    geographic_updated_records = geographic_updated_records.drop(columns=['unique_ID'])
                    
                    geographic_updated_records = geographic_updated_records[['State_Code','County','City','RuralVsUrban']]
                    
                    # Load the new records into the table
                    if len(geographic_new_records) > 0:
                        # If the new records are greater than 0 then we need to load the new records into the table else there is an error
                        for _, row in geographic_new_records.iterrows():
                            print(f'NEW RECORD : STATE_CODE: {row["State_Code"]}, COUNTY: {row["County"]}, CITY: {row["City"]}, RURAL VS URBAN: {row["RuralVsUrban"]}')
                            
                        query = f"""
                            INSERT INTO {table_name} ({', '.join(geographic_new_records.columns)})
                            VALUES ({','.join(['?']*len(geographic_new_records.columns))})
                        """
                        
                        cursor.executemany(query, geographic_new_records.values.tolist())
                        cursor.commit()
                        
                    # Update the existing records in the table
                    # We need to handle the SCD's here
                    # We need to set the IsCurrent to 0 for the existing records
                    # We need to set the ExpirationDate to the current date for the existing records
                    # We need to insert the updated records as new records in the table
                    # We need to set the EffectiveDate to the current date for the updated records
                    # We need to set the IsCurrent to 1 for the updated records
                    # We need to set the ExpirationDate to '9999-12-31' for the updated records
                    if len(geographic_updated_records) > 0:
                        # Check if any columns have changed
                        added_records = []
                        for _, row in geographic_updated_records.iterrows():
                            # Need to check for state code, county, city, rural vs urban
                            
                            # We need to check all columns of the geographic info table
                            current_row = current_df.loc[(current_df['State_Code'] == row['State_Code']) & (current_df['County'] == row['County']) & (current_df['City'] == row['City'])]
                            if current_row.empty:
                                continue
                            else:  
                                current_row = current_row.iloc[0]
                                changed_columns = [col for col in geographic_updated_records.columns if row[col] != current_row[col]]
                                print(f'MATCHED RECORD : Row State Code: {row["State_Code"]}, County: {row["County"]}, City: {row["City"]}, Changed Columns: {changed_columns}, Matched Row State Code: {current_row["State_Code"]}, County: {current_row["County"]}, City: {current_row["City"]}')
                                if len(changed_columns) > 0:
                                    # First Making the Current Bit 0 and Expiring the record
                                    print(f'UPDATED : Row State Code: {row["State_Code"]}, County: {row["County"]}, City: {row["City"]}, Changed Columns: {changed_columns}, Matched Row State Code: {current_row["State_Code"]}, County: {current_row["County"]}, City: {current_row["City"]}')
                                    update_query = f"""
                                        UPDATE {table_name}
                                        SET IsCurrent = 0, ExpirationDate = GETDATE()
                                        WHERE State_Code = ? AND County = ? AND City = ?
                                    """
                                    cursor.execute(update_query, (row['State_Code'],row['County'],row['City']))
                                    cursor.commit()
                                
                                    # Insert the updated record with new data
                                    insert_query = f"""
                                        INSERT INTO {table_name} ({', '.join(geographic_updated_records.columns)})
                                        VALUES ({','.join(['?']*len(geographic_updated_records.columns))})
                                    """
                                    cursor.execute(insert_query, tuple(row))
                                    cursor.commit()
                                    added_records.append(row)
                                
                        geographic_updated_records = pd.DataFrame(added_records,columns=geographic_updated_records.columns)
                        print('Records Updated in GeographicInfo Dimension: ',len(geographic_updated_records))
                                
                    print("Updated Geographic Info Dimension Table Successfully!")    
                elif table_name == 'OccupationalInfo':
                    # Get all the records from the table
                    query = f"""
                        SELECT * FROM {table_name}
                    """
                    
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    result = cursor.fetchall()
                    
                    # Create a dataframe from the result
                    current_df = pd.DataFrame.from_records(result,columns=columns)
                    
                    # Check which records are new and which records are updated
                    current_df['unique_ID'] = current_df['NAICS'].astype(str)+"_"+current_df['OccupationTitle']+"_"+current_df['AreaName']+"_"+current_df['AreaCode']
                    df['unique_ID'] = df['NAICS'].astype(str)+"_"+df['OccupationTitle']+"_"+df['AreaName']+"_"+df['AreaCode']
                   
                    occupational_new_records = df[~df['unique_ID'].isin(current_df['unique_ID'])]
                    occupational_updated_records = df[df['unique_ID'].isin(current_df['unique_ID'])]
                    
                    occupational_new_records = occupational_new_records.drop(columns=['unique_ID'])
                    occupational_updated_records = occupational_updated_records.drop(columns=['unique_ID'])
                    
                    occupational_updated_records = occupational_updated_records[['NAICS','OccupationTitle','IndustryGroup','AreaName','AreaCode']]
                    
                    # Load the new records into the table
                    if len(occupational_new_records) > 0:
                        # If the new records are greater than 0 then we need to load the new records into the table else there is an error
                        for _, row in occupational_new_records.iterrows():
                            print(f'NEW OCCUPATION INFO RECORD : NAICS: {row["NAICS"]}, OCCUPATION TITLE: {row["OccupationTitle"]}, INDUSTRY GROUP: {row["IndustryGroup"]}, AREA NAME: {row["AreaName"]}, AREA CODE: {row["AreaCode"]}')
                        query = f"""
                            INSERT INTO {table_name} ({', '.join(occupational_new_records.columns)})
                            VALUES ({','.join(['?']*len(occupational_new_records.columns))})
                        """
                        
                        cursor.executemany(query, occupational_new_records.values.tolist())
                        cursor.commit()
                        
                    # Update the existing records in the table
                    # We need to handle the SCD's here
                    # We need to set the IsCurrent to 0 for the existing records
                    # We need to set the ExpirationDate to the current date for the existing records
                    # We need to insert the updated records as new records in the table
                    # We need to set the EffectiveDate to the current date for the updated records
                    # We need to set the IsCurrent to 1 for the updated records
                    # We need to set the ExpirationDate to '9999-12-31' for the updated records
                    if len(occupational_updated_records) > 0:
                        # Check if any columns have changed
                        added_records = []
                        for _, row in occupational_updated_records.iterrows():
                            # Need to check for state code, county, city, rural vs urban
                            
                            # There is no unique key in the occupational info table so we need to check all the columns
                            current_row = current_df[(current_df['NAICS'] == row['NAICS']) & (current_df['OccupationTitle'] == row['OccupationTitle']) & (current_df['AreaName'] == row['AreaName']) & (current_df['AreaCode'] == row['AreaCode'])]
                            if current_row.empty:
                                continue
                            else:
                                current_row = current_row.iloc[0]
                                changed_columns = []
                                for column in occupational_updated_records.columns:
                                    if row[column] != current_row[column]:
                                        changed_columns.append(column)
                                    
                                print(f'MATCHED OCCUPATION INFO RECORD : NAICS: {row["NAICS"]}, OCCUPATION TITLE: {row["OccupationTitle"]}, INDUSTRY GROUP: {row["IndustryGroup"]}, AREA NAME: {row["AreaName"]}, AREA CODE: {row["AreaCode"]}')
                                if len(changed_columns) > 0:
                                    # First Making the Current Bit 0 and Expiring the record
                                    print(f'UPDATED OCCUPATION INFO RECORD : NAICS: {row["NAICS"]}, OCCUPATION TITLE: {row["OccupationTitle"]}, INDUSTRY GROUP: {row["IndustryGroup"]}, AREA NAME: {row["AreaName"]}, AREA CODE: {row["AreaCode"]}')
                                    update_query = f"""
                                        UPDATE {table_name}
                                        SET IsCurrent = 0, ExpirationDate = GETDATE()
                                        WHERE NAICS = ?
                                    """
                                    cursor.execute(update_query, (row['NAICS'],))
                                    cursor.commit()
                                
                                    # Insert the updated record with new data
                                    insert_query = f"""
                                        INSERT INTO {table_name} ({', '.join(occupational_updated_records.columns)})
                                        VALUES ({','.join(['?']*len(occupational_updated_records.columns))})
                                    """
                                    cursor.execute(insert_query, tuple(row))
                                    cursor.commit()
                                
                                    added_records.append(row)
                                
                        occupational_updated_records = pd.DataFrame(added_records,columns=occupational_updated_records.columns)
                        print('Records Updated in OccupationalInfo Dimension: ',len(occupational_updated_records))
                
                if table_name == 'HospitalInfo':
                    return hospital_new_records,hospital_updated_records
                elif table_name == 'GeographicInfo':
                    return geographic_new_records,geographic_updated_records
                elif table_name == 'OccupationalInfo':
                    return occupational_new_records,occupational_updated_records 
                                
   
                    

def load_data_into_staging_tables(cursor: pyodbc.Cursor, table_name: str, df: pd.DataFrame) -> None:
    # Now we need to load the data into the staging tables
    # This will always be empty when loading the data
    
    # Delete existing records just in case to make sure there are no overlaps in the Staging table
    query = f"""
        DELETE FROM {table_name}
    """
    
    cursor.execute(query)
    cursor.commit()
    
    query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)})
        VALUES ({','.join(['?']*len(df.columns))})
    """
    
    cursor.executemany(query, df.values.tolist())
    cursor.commit()
    
    
def delete_data_from_table(cursor: pyodbc.Cursor, table_name: str) -> None:
    query = f"""
        DELETE FROM {table_name}
    """
    
    cursor.execute(query)
    cursor.commit()
    
    print(f'Deleted all records from {table_name} successfully!')
    
def load_data_into_database(cursor: pyodbc.Cursor, hospitalInfo_dim: pd.DataFrame,date_dim: pd.DataFrame, occupationInfo_dim: pd.DataFrame,geographic_info_dim,update: bool = False):
    # Loading the data into the database
    if not update:
        load_data_into_table(cursor, 'HospitalInfo', hospitalInfo_dim)
        load_data_into_table(cursor, 'DateDimension', date_dim)
        load_data_into_table(cursor, 'OccupationalInfo', occupationInfo_dim)
        load_data_into_table(cursor, 'GeographicInfo', geographic_info_dim)
    else:
        hospital_new_records,hospital_updated_records  = load_data_into_table(cursor, 'HospitalInfo', hospitalInfo_dim, True, 2)
        load_data_into_table(cursor, 'DateDimension', date_dim)
        occupational_new_records,occupational_updated_records = load_data_into_table(cursor, 'OccupationalInfo', occupationInfo_dim,True,2)
        geographic_new_records,geographic_updated_records = load_data_into_table(cursor, 'GeographicInfo', geographic_info_dim,True,2)
        return hospital_new_records,hospital_updated_records,occupational_new_records,occupational_updated_records,geographic_new_records,geographic_updated_records
    
    print('Loaded all the data into the database successfully!')

    
def delete_data_from_database(cursor: pyodbc.Cursor) -> None:
    delete_data_from_table(cursor, 'HospitalInfo')
    delete_data_from_table(cursor, 'DateDimension')
    delete_data_from_table(cursor, 'OccupationalInfo')
    delete_data_from_table(cursor, 'GeographicInfo')
    delete_data_from_table(cursor, 'FinancialMeasurement')
    delete_data_from_table(cursor, 'DemographicMeasurement')
    delete_data_from_table(cursor, 'HealthCareCost')
    delete_data_from_table(cursor, 'FinancialMeasurementStaging')
    delete_data_from_table(cursor, 'DemographicMeasurementStaging')
    delete_data_from_table(cursor, 'HealthCareCostStaging')
    
def generate_fact_tables(cursor: pyodbc.Cursor) -> None:
    
    delete_query = """
        DELETE FROM FinancialMeasurement
    """
    
    cursor.execute(delete_query)
    cursor.commit()
    
    query = """
        WITH InterDate AS (
            SELECT 
		        DateID,
		        HospitalName,
		        StateCode,
		        County,
		        City,
		        TotalIncome, 
		        Profit, 
		        ProfitMargin,
		        ROW_NUMBER() OVER (PARTITION BY HospitalName ORDER BY DateDimension.DateID DESC) AS RowNum
            FROM FinancialMeasurementStaging
            INNER JOIN DateDimension
            ON DateDimension.Year = FinancialMeasurementStaging.Year
        ),
        InterHospital AS (
            SELECT 
		        HospitalInfo.HospitalID,
		        DateID as Date_Key,
		        StateCode,
		        County,
		        City,
		        TotalIncome,
		        Profit,
		        ProfitMargin 
            FROM (SELECT DateID,HospitalName,StateCode,County,City,TotalIncome,Profit,ProfitMargin FROM InterDate WHERE RowNum = 1) as t1
            INNER JOIN HospitalInfo
            ON t1.HospitalName = HospitalInfo.Hospital_Name AND HospitalInfo.IsCurrent = 1
        )

        SELECT HospitalID,GeographicID,Date_Key,TotalIncome,Profit,ProfitMargin FROM InterHospital
        INNER JOIN GeographicInfo
        ON InterHospital.StateCode = GeographicInfo.State_Code
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    insert_query = """
        INSERT INTO FinancialMeasurement (HospitalID,GeographicID,Date_Key,TotalIncome,Profit,ProfitMargin)
        VALUES (?,?,?,?,?,?)
    """
    
    cursor.executemany(insert_query, results)
    cursor.commit()
    
    delete_query = """
        DELETE FROM DemographicMeasurement
    """
    
    cursor.execute(delete_query)
    cursor.commit()
    
    query = """
        WITH DemographicInter AS (
            SELECT 
	            DemographicID,
	            DemographicMeasurementStaging.Year,
	            DemographicMeasurementStaging.MeanHourlyWage,
	            DemographicMeasurementStaging.MeanWage,
	            DemographicMeasurementStaging.MedianAnnualWage,
	            DemographicMeasurementStaging.PCT25AnnualWage, 
	            DemographicMeasurementStaging.PCT25HourlyWage,
	            DemographicMeasurementStaging.PCT75AnnualWage,
	            DemographicMeasurementStaging.PCT75HourlyWage 
            FROM DemographicMeasurementStaging
            INNER JOIN OccupationalInfo
            ON OccupationalInfo.NAICS = DemographicMeasurementStaging.NAICS AND OccupationalInfo.OccupationTitle = DemographicMeasurementStaging.OccupationTitle AND OccupationalInfo.AreaName = DemographicMeasurementStaging.AreaName AND OccupationalInfo.AreaCode = DemographicMeasurementStaging.AreaCode
        ),
        DateDemographicInter AS (
            SELECT 
	            DemographicInter.DemographicID,
	            DateDimension.DateID,
	            DemographicInter.MeanHourlyWage,
	            DemographicInter.MeanWage,
	            DemographicInter.MedianAnnualWage,
	            DemographicInter.PCT25AnnualWage, 
	            DemographicInter.PCT25HourlyWage,
	            DemographicInter.PCT75AnnualWage,
	            DemographicInter.PCT75HourlyWage,
	            ROW_NUMBER() OVER (PARTITION BY DemographicID ORDER BY DateDimension.DateID DESC) AS RowNum 
        FROM DemographicInter
        INNER JOIN DateDimension
        ON DemographicInter.Year = DateDimension.Year
        )

        SELECT DemographicID,DateID,MeanHourlyWage,MeanWage,MedianAnnualWage,PCT25AnnualWage,PCT25HourlyWage,PCT75AnnualWage,PCT75HourlyWage FROM DateDemographicInter
        WHERE RowNum = 1
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    insert_query = """
        INSERT INTO DemographicMeasurement (DemographicID,DateID,MeanHourlyWage,MeanWage,MedianAnnualWage,PCT25AnnualWage,PCT25HourlyWage,PCT75AnnualWage,PCT75HourlyWage)
        VALUES (?,?,?,?,?,?,?,?,?)
    """
    
    cursor.executemany(insert_query, results)
    cursor.commit()
    
    delete_query = """
        DELETE FROM HealthCareCost
    """
    
    cursor.execute(delete_query)
    cursor.commit()
    
    query = """
            WITH HealthCareCostHospitalInter AS (
                SELECT 
                HospitalID,
                Year,
                Quarter,
                Month,
                Day,
                StateCode,
                County,City,MedicaidCharges,StandAloneCHIPCharges,TotalCosts,NetIncome,NetRevenueFromMedicaid,NetRevenueFromStandAloneCHIP,NetIncomeFromServiceToPatients, CostToChargeRatio 
            FROM HealthCareCostStaging
            INNER JOIN HospitalInfo
            ON HospitalInfo.Provider_CCN = HealthCareCostStaging.ProviderCCN
            ),
            HealthCareCostGeographicInter AS (
                SELECT 
                HospitalID,
                GeographicID,
                Year,
                Month,
                Quarter,
                MedicaidCharges,
                StandAloneCHIPCharges,
                TotalCosts,
                NetIncome,
                NetRevenueFromMedicaid,
                NetRevenueFromStandAloneCHIP,
                NetIncomeFromServiceToPatients,
                CostToChargeRatio 
            FROM HealthCareCostHospitalInter
            INNER JOIN GeographicInfo
            ON HealthCareCostHospitalInter.StateCode = GeographicInfo.State_Code
            ),
            HealthCareDateInter AS (
                SELECT 
                HospitalID,
                GeographicID,
                DateID,
                ROW_NUMBER() OVER (PARTITION BY HospitalID ORDER BY DateID DESC) as RowNumber,
                MedicaidCharges,
                StandAloneCHIPCharges,
                TotalCosts,
                NetIncome,
                NetRevenueFromMedicaid,
                NetRevenueFromStandAloneCHIP,
                NetIncomeFromServiceToPatients,
                CostToChargeRatio 
            FROM HealthCareCostGeographicInter
            INNER JOIN DateDimension
            ON DateDimension.Year = HealthCareCostGeographicInter.Year
            )

            SELECT HospitalID,GeographicID,DateID,MedicaidCharges,StandAloneCHIPCharges,TotalCosts,NetIncome,NetRevenueFromMedicaid,NetRevenueFromStandAloneCHIP,NetIncomeFromServiceToPatients, CostToChargeRatio FROM HealthCareDateInter
            WHERE RowNumber = 1
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    insert_query = """
        INSERT INTO HealthCareCost (HospitalID,GeographicID,DateID,MedicaidCharges,StandAloneCHIPCharges,TotalCosts,NetIncome,NetRevenueFromMedicaid,NetRevenueFromStandAloneCHIP,NetIncomeFromServiceToPatients,CostToChargeRatio)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """
    
    cursor.executemany(insert_query, results)
    cursor.commit()
    

