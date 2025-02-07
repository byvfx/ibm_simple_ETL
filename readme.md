# Simple ETL Project

This project demonstrates a simple ETL process to extract data from a Wikipedia page, transform it with exchange rates, and load it into both a CSV file and an SQLite database.

## ETL Process Overview

### Extraction
- Downloads the Wikipedia page.
- Parses the HTML to locate the target table with market capitalization information.
- Example snippet:
```python
# Extract function snippet
def extract():
    # ...existing extraction code...
    response = requests.get(WIKIPEDIA_URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    # ...existing extraction code...
```

### Transformation
- Reads exchange rates from a CSV file.
- Converts bank market capitalizations from USD to GBP, EUR, and INR.
- Example snippet:
```python
# Transform function snippet
def transform(df):
    # ...existing transformation code...
    exchange_rates = dict(zip(exchange_rates_df['Currency'], exchange_rates_df['Rate']))
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * exchange_rates['GBP']).round(2)
    # ...existing transformation code...
```

### Loading
- Saves the transformed data into a CSV file.
- Loads the data into a SQLite database named `Banks.db`.
- SQL queries are run to analyze the data.
- Example snippet:
```python
# Main function snippet
def main():
    # ...existing main code...
    df = extract()
    if df is not None:
        df_transformed = transform(df)
        # ...existing loading code...
```

## Logging and Verification

- Logs the progress of each ETL step into `code_log.txt`.
- Verification function prints the content of the log file to track the process.

## Execution

To run the ETL process, execute:
```bash
python etl_bank_project.py
```
