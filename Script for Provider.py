import psycopg2
import pandas as pd
from datetime import datetime, timedelta

db_config = {                                                        # Database configuration
    'dbname': 'Changes',
    'user': 'postgres',
    'password': '05102021',
    'host': 'localhost',
    'port': 5432
}

def connect_to_database(config):                                     # Connect to the database  
    try:
        conn = psycopg2.connect(**config)
        print("Connection successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_data_for_date(conn, date):                                # Fetch data for a specific date
    query = f"""                    
    SELECT *,                               
           DATE(date_timestamp::timestamp) as date_only,        
           TO_CHAR(date_timestamp::timestamp, 'HH24:MI:SS') as time_only
    FROM store
    WHERE DATE(date_timestamp::timestamp) = '{date}'        
    """
    try:    
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def compare_data(df1, df2):                                         # function for Comparing data
    comparison = pd.merge(df1, df2, on='site_id', suffixes=('_old', '_new'))
    changes = comparison[
        (comparison['provider_primary_old'] != comparison['provider_primary_new']) |
        (comparison['provider_backup_old'] != comparison['provider_backup_new'])
    ]
    return changes

def create_alerts_table(conn):                                      # Create table for detected changes
    create_table_query = """
    CREATE TABLE IF NOT EXISTS alerts (
        id SERIAL PRIMARY KEY,
        site_id VARCHAR(255) NOT NULL,
        date_timestamp TIMESTAMP NOT NULL,
        date_old VARCHAR(255),
        provider_primary_old VARCHAR(255),
        provider_backup_old VARCHAR(255),
        date_new VARCHAR(255),
        provider_primary_new VARCHAR(255),
        provider_backup_new VARCHAR(255)
    )"""
    
    try:                        
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            print("Table 'alerts' created or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_changes_into_alerts(conn, changes):                      # Insert changes into the new table
    current_timestamp = datetime.now()
    try:
        with conn.cursor() as cursor:
            for index, row in changes.iterrows():
                insert_query = """
                INSERT INTO alerts (
                    site_id, 
                    date_timestamp,
                    date_old,
                    provider_primary_old, 
                    provider_backup_old, 
                    date_new, 
                    provider_primary_new, 
                    provider_backup_new
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    row['site_id'],
                    current_timestamp,
                    row['date_only_old'],                          # Adjusted based on column names in DataFrame
                    row['provider_primary_old'], 
                    row['provider_backup_old'], 
                    row['date_only_new'],                          # Adjusted based on column names in DataFrame
                    row['provider_primary_new'],
                    row['provider_backup_new']
                ))
            conn.commit()
            print("Data inserted into 'alerts' table successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")

def main():                                                         # Main function
    conn = connect_to_database(db_config)
    if conn:
        today_date = datetime.now().date()                          # Get current date
        yesterday_date = today_date - timedelta(days=1)             # Calculate previous date
        
        date1 = yesterday_date.strftime('%Y-%m-%d')                 # Convert dates to string format 'YYYY-MM-DD'
        date2 = today_date.strftime('%Y-%m-%d')

        df1 = fetch_data_for_date(conn, date1)
        df2 = fetch_data_for_date(conn, date2)

        if df1 is not None and df2 is not None:                                      
            differences = compare_data(df1, df2)                            
            if not differences.empty:                   
                print("Changes detected!:")
                # print(f"date2: {date2}")
                # print(differences)
                create_alerts_table(conn)
                insert_changes_into_alerts(conn, differences)
            else:
                print("No changes detected")
                # print(f"date2: {date2}")
                # print(f"date1: {date1}")
        conn.close()

if __name__ == "__main__":                                          # Run the main function
    main()  
