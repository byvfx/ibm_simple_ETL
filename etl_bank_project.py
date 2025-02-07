import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import sqlite3
import logging

# Global configuration variables
WIKIPEDIA_URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
EXCHANGE_RATES_CSV = 'exchange_rate.csv'
DATABASE_PATH = 'Banks.db'
CSV_OUTPUT_PATH = './Largest_banks_data.csv'

# Configure logging
logging.basicConfig(
    filename='code_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

def log_progress(message):
    """
    Log the progress of the code.
    """
    logging.info(message)

def extract():
    """
    Extract the tabular information from the Wikipedia page.
    Returns a dataframe with bank name and total assets in USD billions.
    """
    try:
        log_progress("Starting data extraction from Wikipedia")
        # Use global variable for URL
        url = WIKIPEDIA_URL
        
        # Make the request and create soup object
        log_progress("Making HTTP request to Wikipedia")
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all tables and locate the market capitalization table
        log_progress("Locating market capitalization table")
        tables = soup.find_all('table', {'class': 'wikitable'})
        target_table = None
        
        for table in tables:
            headers = table.find_all('th')
            for header in headers:
                if header.get_text() and 'Market cap' in header.get_text():
                    target_table = table
                    break
            if target_table:
                break
        
        if target_table:
            log_progress("Target table found - processing data")
            df = pd.read_html(io.StringIO(str(target_table)))[0]
            
            # Clean and rename columns
            log_progress(f"Available columns: {df.columns.tolist()}")
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(-1)
            
            df.columns = df.columns.str.strip()
            df_clean = df[['Bank name', 'Market cap (US$ billion)']].copy()
            df_clean.columns = ['Name', 'MC_USD_Billion']
            
            df_clean['MC_USD_Billion'] = df_clean['MC_USD_Billion'].astype(str).str.replace(',', '')
            df_clean['MC_USD_Billion'] = pd.to_numeric(df_clean['MC_USD_Billion'], errors='coerce')
            
            log_progress("Data extraction completed successfully")
            return df_clean
        else:
            log_progress("Error: Target table not found!")
            return None
            
    except Exception as e:
        log_progress(f"Error in data extraction: {str(e)}")
        return None

def transform(df):
    """
    Transform the dataframe by adding currency conversion columns.
    """
    try:
        if df is None:
            log_progress("Error: No dataframe to transform")
            return None
            
        log_progress("Starting data transformation")
        
        try:
            log_progress(f"Reading exchange rates from {EXCHANGE_RATES_CSV}")
            exchange_rates_df = pd.read_csv(EXCHANGE_RATES_CSV)
            
            required_currencies = {'EUR', 'GBP', 'INR'}
            available_currencies = set(exchange_rates_df['Currency'])
            
            if not required_currencies.issubset(available_currencies):
                missing = required_currencies - available_currencies
                log_progress(f"Error: Missing exchange rates for currencies: {missing}")
                return None
            
            exchange_rates = dict(zip(exchange_rates_df['Currency'], exchange_rates_df['Rate']))
            log_progress(f"Exchange rates loaded successfully: EUR={exchange_rates['EUR']}, GBP={exchange_rates['GBP']}, INR={exchange_rates['INR']}")
            
            df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * exchange_rates['GBP']).round(2)
            df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * exchange_rates['EUR']).round(2)
            df['MC_INR_Billion'] = (df['MC_USD_Billion'] * exchange_rates['INR']).round(2)
            
            log_progress("Data transformation completed successfully")
            return df
            
        except Exception as e:
            log_progress(f"Error reading exchange rates CSV: {str(e)}")
            return None
            
    except Exception as e:
        log_progress(f"Error in data transformation: {str(e)}")
        return None

def load_to_csv(df):
    """
    Load transformed data to CSV.
    """
    try:
        if df is None:
            log_progress("Error: No dataframe to save to CSV")
            return False
            
        log_progress(f"Starting CSV file export to {CSV_OUTPUT_PATH}")
        df.to_csv(CSV_OUTPUT_PATH, index=False)
        log_progress(f"Data successfully exported to {CSV_OUTPUT_PATH}")
        return True
        
    except Exception as e:
        log_progress(f"Error saving to CSV: {str(e)}")
        return False

def load_to_db(df):
    """
    Load transformed data to SQL database.
    """
    try:
        if df is None:
            log_progress("Error: No dataframe to load to database")
            return False
            
        log_progress("Starting database loading process")
        conn = sqlite3.connect(DATABASE_PATH)
        
        df.to_sql('Largest_banks', conn, if_exists='replace', index=False)
        log_progress(f"Data successfully loaded to {DATABASE_PATH}")
        
        conn.close()
        return True
        
    except Exception as e:
        log_progress(f"Error in database loading: {str(e)}")
        return False

def run_queries():
    """
    Run various queries on the database.
    """
    try:
        log_progress("Starting database queries")
        conn = sqlite3.connect(DATABASE_PATH)
        
        query1 = """
        SELECT 
            ROUND(AVG(MC_EUR_Billion), 2) as Average_MC_EUR,
            ROUND(MAX(MC_EUR_Billion), 2) as Max_MC_EUR,
            ROUND(MIN(MC_EUR_Billion), 2) as Min_MC_EUR,
            ROUND(MAX(MC_EUR_Billion) / MIN(MC_EUR_Billion), 2) as Max_to_Min_Ratio
        FROM Largest_banks;
        """
        
        query2 = """
        SELECT 
            ROUND(AVG(MC_EUR_Billion), 2) as Avg_EUR,
            ROUND(AVG(MC_USD_Billion), 2) as Avg_USD,
            ROUND(AVG(MC_GBP_Billion), 2) as Avg_GBP,
            ROUND(AVG(MC_INR_Billion), 2) as Avg_INR
        FROM Largest_banks;
        """
        
        query3 = """
        WITH max_cap AS (
            SELECT MAX(MC_EUR_Billion) as max_mc
            FROM Largest_banks
        )
        SELECT 
            Name,
            MC_EUR_Billion,
            ROUND(MC_EUR_Billion / max_mc * 100, 2) as Percent_of_Largest
        FROM Largest_banks, max_cap
        ORDER BY MC_EUR_Billion DESC;
        """
        
        results = {
            'Market Cap Analysis': pd.read_sql_query(query1, conn),
            'Average by Currency': pd.read_sql_query(query2, conn),
            'Bank Size Comparison': pd.read_sql_query(query3, conn)
        }
        
        for name, df in results.items():
            print(f"\n{name}:")
            print(df)
            
        log_progress("Database queries completed successfully")
        conn.close()
        return results
        
    except Exception as e:
        log_progress(f"Error in database queries: {str(e)}")
        return None

def verify_logs():
    """
    Verify log file contents.
    """
    try:
        with open('code_log.txt', 'r') as file:
            log_contents = file.read()
            print("\nLog file contents:")
            print(log_contents)
            return True
    except Exception as e:
        print(f"Error reading log file: {str(e)}")
        return False

def main():
    """
    Main function to execute all tasks in sequence.
    """
    log_progress("ETL Process Started")
    
    df = extract()
    
    if df is not None:
        df_transformed = transform(df)
        
        if df_transformed is not None:
            db_success = load_to_db(df_transformed)
            
            if db_success:
                run_queries()
            
            verify_logs()
            log_progress("ETL Process Completed Successfully")
        else:
            log_progress("ETL Process Failed at Transformation Stage")
    else:
        log_progress("ETL Process Failed at Extraction Stage")

if __name__ == "__main__":
    main()