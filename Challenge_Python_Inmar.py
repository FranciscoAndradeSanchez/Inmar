import pandas as pd
import re
import os
import sys 
import glob
import logging
from datetime import datetime

# Define the path for the processed files log
PROCESSED_FILES_LOG = 'processed_files.csv'


def load_processed_files():
    # Loading the set of processed files from a CSV log file.
    
    if os.path.exists(PROCESSED_FILES_LOG):
        processed_df = pd.read_csv(PROCESSED_FILES_LOG)
        return set(processed_df['file_name'])
    return set()


def save_processed_files(file_name):
    # Saving a new processed file name and the current date to the CSV log.
    
    data = {'file_name': [file_name], 'processed_date': [datetime.now().isoformat()]}
    new_record_df = pd.DataFrame(data)
    
    if os.path.exists(PROCESSED_FILES_LOG):
        processed_df = pd.read_csv(PROCESSED_FILES_LOG)
        updated_df = pd.concat([processed_df, new_record_df], ignore_index=True)
    else:
        updated_df = new_record_df
    
    updated_df.to_csv(PROCESSED_FILES_LOG, index=False)
    
    

def file_check_module(file_path):
    #  Module 1: File Check Module
    
    processed_files = load_processed_files()
    file_name = os.path.basename(file_path)
    
    # Checking If Is this a new file?    point 1  case 1
    if file_name in processed_files:
        logging.warning(f"Skipping '{file_name}': Already processed.")
        return False
    
    # Checking If Is the file empty?    point 1    case 1
    if os.path.getsize(file_path) == 0:
        logging.warning(f"Skipping '{file_name}': File is empty.")
        return False
        
    # Checking If Is the file extension .csv?    point 1   case  3
    if not file_path.endswith('.csv'):
        logging.warning(f"Skipping '{file_name}': Invalid file extension.")
        return False
    
    return True

def data_quality_check_module(df):
    #  Module 2: Data Quality Check Module - Performs standard data cleaning and validation checks.
    
    
    # Initializing columns for validation and metadata
    df['is_bad'] = False
    df['issue_type'] = ''

    # Checking the Data in the phone field can be validated for correct phone numbers.   point 2  case 1
    def clean_and_validate_phone(phone):
        if pd.isna(phone):
            return None
        phone = str(phone).replace('+', '').replace(' ', '')
        if len(phone) == 10 and phone.isdigit():
            return phone
        return 'invalid' # Using a distinct value to indicate invalid format

    df['phone_cleaned'] = df['phone'].apply(clean_and_validate_phone)
    
    phone_issue_mask = (df['phone_cleaned'] == 'invalid')
    df.loc[phone_issue_mask, 'is_bad'] = True
    df.loc[phone_issue_mask, 'issue_type'] += 'invalid_phone;'
    
    df['phone'] = df['phone_cleaned']
    df = df.drop(columns=['phone_cleaned'])

    # Checking for those fields ('name', 'phone', 'location') that should not be null, check for null values.
    required_fields = ['name', 'phone', 'location']
    for field in required_fields:
        null_mask = df[field].isnull()
        df.loc[null_mask, 'is_bad'] = True
        df.loc[null_mask, 'issue_type'] += f'null_{field};'

    # Checking for Descriptive fields like address, reviews_list can be cleaned.
    def clean_descriptive_field(text):
        if pd.isna(text):
            return None
        # Remove non-alphanumeric characters, except for spaces and commas.
        return re.sub(r'[^\w\s,]', '', str(text))
        
    df['address'] = df['address'].apply(clean_descriptive_field)
    df['reviews_list'] = df['reviews_list'].apply(clean_descriptive_field)
    
    return df



def write_output_files(df, output_dir, file_name_base):
    #  Writes clean, bad, and metadata files based on the processed DataFrame.
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    clean_records = df[~df['is_bad']].drop(columns=['is_bad', 'issue_type'])
    bad_records = df[df['is_bad']]
    
    # Write clean records
    clean_file_path = os.path.join(output_dir, f"{file_name_base}_cleaned_{current_timestamp}.out")
    clean_records.to_csv(clean_file_path, index=False)
    logging.info(f"Clean records written to: {clean_file_path}")


    # Write bad records and metadata
    if not bad_records.empty:
        bad_file_path = os.path.join(output_dir, f"{file_name_base}.bad")
        bad_records.to_csv(bad_file_path, index=False)
        logging.warning(f"Bad records written to: {bad_file_path}")    
       
       

def main(input_dir, output_dir):
    #  Main function to orchestrate the data pipeline.

    csv_files = glob.glob(os.path.join(input_dir, 'data_file_*.csv'))
    
    for file_path in csv_files:
        if file_check_module(file_path):
            file_name = os.path.basename(file_path)
            file_base_name = os.path.splitext(file_name)[0]
            logging.warning(f"\nProcessing '{file_name}'...")
            
            # Read every file
            try:
                df = pd.read_csv(file_path)
            except Exception as e:
                logging.warning(f"Error reading file '{file_name}': {e}. Skipping.")
                continue

            df = data_quality_check_module(df)            
            
            # Write output files
            write_output_files(df, output_dir, file_base_name)
            
            # Save the processed file to the log
            save_processed_files(file_name)
    
    logging.warning("\nData quality pipeline execution complete.")

if __name__ == '__main__':
    # You must pass the directories as command-line arguments.
    # something like this: python Challenge_Python_Inmar.py data output
    if len(sys.argv) > 2:
        main(sys.argv[1], sys.argv[2])
    else:
        logging.error("Please provide input and output directories as command-line arguments.")
        sys.exit(1)
