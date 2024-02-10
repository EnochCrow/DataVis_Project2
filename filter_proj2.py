import pandas as pd
from io import StringIO

file_path = 'yearly_data\zbp17detail.txt'

def filter_data(in_filepath, out_filepath):
    # Read the CSV data into a string
    with open(in_filepath, 'r') as file:
        csv_data = file.read()

    #for line in csv_data:
    #    line.split("")
    # Remove quotes from the CSV data
    csv_data_cleaned = csv_data.replace('"', '')

    # Create a DataFrame from the cleaned CSV data
    # data = pd.read_csv(pd.compat.StringIO(csv_data_cleaned))
    # Read CSV-formatted text file into a pandas DataFrame, specifying the header row
    df = pd.read_csv(StringIO(csv_data_cleaned), delimiter=',', header=0)
    #df = pd.read_csv(in_filepath, delimiter='","', header=0, engine='python')

    # Define the values you want to filter on
    values_to_filter = [512131, 512132]

    # Replace 'column_name' with the actual column name in your dataset
    # column_name = 'naics'

    def clean_and_convert(column):
        # Remove special characters using regular expressions
        column = column.str.replace(r'[^0-9]', '', regex=True)
        # Convert the cleaned column to numeric type
        column = pd.to_numeric(column, errors='coerce')  # 'coerce' will turn invalid parsing into NaNs
        return column

    # Apply the function to the column you want to clean and convert
    df['naics'] = clean_and_convert(df['naics'])
    df['naics'] = df['naics'].astype('Int64')
    #df['zip'] = pd.to_numeric(df['zip'], errors='coerce')

    filtered_df = df[df['naics'].isin(values_to_filter)]

    columns_to_keep = ['zip','naics','est']
    filtered_df = filtered_df[columns_to_keep]
    filtered_df.to_csv(out_filepath)

def adding_coords(theater_file, coord_file, final_output):
    # theater_file = "zbp_filtered_12.csv"
    # coord_file = "us_zip_code_lat_long.csv"

    df1 = pd.read_csv(theater_file)
    df2 = pd.read_csv(coord_file)

    shared_column = 'zip'
    merged_df = pd.merge(df1, df2, on=shared_column, how='left')
    merged_df.to_csv(final_output, index=False)

in_first_half = "yearly_data\zbp"
in_second_half = "detail.txt"
out_first_half = "filtered_yearly_data\zbp_filtered_"
out_second_half = ".csv"
coord_file = "us_zip_code_lat_long.csv"
for i in range(3, 18):
    num = ""
    if i < 10: num = "0" + str(i)
    else: num = str(i)
    in_file_path = in_first_half + num + in_second_half
    out_file_path = out_first_half + num + out_second_half
    filter_data(in_file_path, out_file_path)

    if i >= 10: theater_file = "filtered_yearly_data\zbp_filtered_" + str(i) + ".csv"
    else: theater_file = "filtered_yearly_data\zbp_filtered_0" + str(i) + ".csv" 
    
    if i >= 10: output_file = "final_yearly_data\\theater_coords_" + str(i) + ".csv"
    else: output_file = "final_yearly_data\\theater_coords_0" + str(i) + ".csv"
    
    adding_coords(theater_file, coord_file, output_file)