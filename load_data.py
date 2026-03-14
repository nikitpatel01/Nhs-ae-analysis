# Load libraries panda for data tables , os and glob to find files on computer, sqlalchemy to connect to the database 
import pandas as pd
import os
import glob
from sqlalchemy import create_engine

# Store the path to where all the files are saved
folder_path = r'C:\Users\nikit\Downloads\Nhs project files'

# Find all excel files in the folder
files = glob.glob(os.path.join(folder_path, '*.xls*'))

all_data = []

# Loop through all the files and remove spaces from column names and adds a recording column to show which file data came from, appends to all_data
for file in files:
    df = pd.read_excel(file, sheet_name=0, skiprows=15) # Note skip first 15 rows for the headers in the file that do not have data
    df.columns = df.columns.str.strip()
    df['source_file'] = os.path.basename(file)
    all_data.append(df)

# Create a table by combining all files into one table
combined = pd.concat(all_data, ignore_index=True)

# Keep only the columns we need and rename them cleanly
combined = combined[['Code', 'System', 
                      'Total attendances', 
                      'Total Attendances < 4 hours', 
                      'source_file']]

combined.columns = ['trust_code', 'trust_name',
                    'total_attendances', 'within_4hrs', 'source_file']

# Drop rows where trust name is missing
combined = combined.dropna(subset=['trust_name'])

# Calculate the 4-hour compliance rate 
combined['compliance_pct'] = (
    combined['within_4hrs'] / combined['total_attendances'] * 100 # Creates a column to divide patients seen in 4 hours by total attendance, then converts to a % 
).round(2)

# Save to a SQLite database
engine = create_engine('sqlite:///nhs_ae.db')
combined.to_sql('ae_data', engine, if_exists='replace', index=False)

print(f'Done. {len(combined)} rows loaded into nhs_ae.db')


# Save as a CSV file to view in excel, and for visulaisation in PowerBI  
engine = create_engine('sqlite:///nhs_ae.db')
df = pd.read_sql('SELECT * FROM ae_data', engine)
df.to_csv('nhs_ae_clean.csv', index=False)
print('Exported.')

