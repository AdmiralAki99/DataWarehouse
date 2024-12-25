from warehouse import *

if __name__ == '__main__':
   
    hospital_df_2020 = extract('./Hospital Info/CostReport_2020_Final.csv', HOSPITAL_SUBSET)
    demographic_df_2020 = extract('./Demographic Info/state_M2020_dl.xlsx', DEMOGRAPHIC_SUBSET)
    
    hospital_df_2020 = transform_hospital_data(hospital_df_2020)
    demographic_df_2020 = transform_demographic_data(demographic_df_2020,2020)
    
    hospital_dim_2020, date_dim_2020, occupational_info_dim_2020,geographic_info_dim_2020,financial_staging_table_2020,demographic_staging_table_2020,health_care_cost_staging_table_2020 = transform_data(hospital_df_2020, demographic_df_2020,2020)
    
    cursor = connect_to_warehouse()
    
    verify_tables_in_database(cursor)
    
    load_data_into_database(cursor, hospital_dim_2020, date_dim_2020, occupational_info_dim_2020,geographic_info_dim_2020)
    
    load_data_into_staging_tables(cursor, 'FinancialMeasurementStaging', financial_staging_table_2020)
    load_data_into_staging_tables(cursor, 'DemographicMeasurementStaging', demographic_staging_table_2020)
    load_data_into_staging_tables(cursor, 'HealthCareCostStaging', health_care_cost_staging_table_2020)
    
    generate_fact_tables(cursor)