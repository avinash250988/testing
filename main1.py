from flask import Flask, render_template, request, send_file, make_response, json, session, jsonify
import re
from bs4 import BeautifulSoup
import pyodbc
import pandas as pd
import os
import io
import csv
from openpyxl import Workbook

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Needed for session management

# Replace these with your actual Azure SQL Database connection details
SERVER = 'avitesting.database.windows.net'
DATABASE = 'AdventureWorks2022'
USERNAME = 'avi_testing_login'
PASSWORD = 'Evry@2024'

def get_connection():
    return pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'UID={USERNAME};'
        f'PWD={PASSWORD};'
    )

def get_current_database():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        return cursor.fetchone()[0]

# Store the data in this variable to be able to download it later
data_for_download = {}

# Global variable to store search results
results = {}

def search_columns(column_names):
    global data_for_download, results  # Declare results as global
    data_for_download.clear()  # Clear previous data
    results.clear()  # Clear previous results
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all schemas
        cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA")
        schemas = [row.SCHEMA_NAME for row in cursor.fetchall()]
        results["Database Info"] = {"Schemas": schemas}
        
        for column_name in column_names:
            query = f"""
            SELECT 
                s.name AS schema_name,
                t.name AS table_name, 
                c.name AS column_name
            FROM sys.columns c
            JOIN sys.tables t ON c.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE c.name LIKE '%{column_name}%'
            """
            cursor.execute(query)
            tables = cursor.fetchall()
            
            for table in tables:
                schema_name = table.schema_name
                table_name = table.table_name
                column_name = table.column_name
                full_table_name = f"[{schema_name}].[{table_name}]"
                
                if full_table_name not in results:
                    results[full_table_name] = {}
                
                try:
                    data_query = f"SELECT TOP 100 [{column_name}] FROM {full_table_name}"
                    df = pd.read_sql(data_query, conn)
                    
                    results[full_table_name][column_name] = df.to_dict('records')
                    
                    # Store the data for downloading as a CSV
                    if full_table_name not in data_for_download:
                        data_for_download[full_table_name] = {}
                    data_for_download[full_table_name][column_name] = df
                    
                    if not results[full_table_name][column_name]:
                        results[full_table_name][column_name] = "No data available for this column."
                except Exception as e:
                    results[full_table_name][column_name] = f"Error querying data: {str(e)}"
                
                # Add table structure information
                try:
                    structure_query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
                    """
                    df_structure = pd.read_sql(structure_query, conn)
                    results[full_table_name]["Table Structure"] = df_structure.to_dict('records')
                except Exception as e:
                    results[full_table_name]["Table Structure"] = f"Error retrieving table structure: {str(e)}"
    
    return results

def extract_potential_column_names(html_content):
    # Use regex to find potential field names in the test script
    pattern = r'@id=\\"([^\\"]+)\\"'  # Updated pattern to handle escaped quotes
    potential_columns = re.findall(pattern, html_content)
    
    # Remove duplicates and sort
    potential_columns = sorted(set(potential_columns))
    
    return potential_columns

@app.route('/', methods=['GET', 'POST'])
def index():
    current_db = get_current_database()
    results = {}
    all_columns = []  # List of all column names
    column_data = []  # Data for each column

    if request.method == 'POST':
        html_content = request.form['html_content']
        potential_columns = extract_potential_column_names(html_content)
        
        if not results:
            results = search_columns(potential_columns)
        
        # Extract all columns and their data
        for table, columns in results.items():
            for column, data in columns.items():
                if column != "Table Structure" and table != "Database Info":  # Exclude unnecessary column
                    all_columns.append(f"{table}.{column}")
                    column_data.append([d[column] for d in data if isinstance(d, dict)])

        # Calculate the maximum length of column data
        max_length = max(len(col) for col in column_data) if column_data else 0

        if 'selected_tables' in request.form:
            selected_tables = request.form.getlist('selected_tables')
            filtered_results = {table: results[table] for table in selected_tables if table in results}
        else:
            filtered_results = results
        
        return render_template('results1.html', results=results, filtered_results=filtered_results, current_db=current_db, potential_columns=potential_columns, all_columns=all_columns, column_data=column_data, max_length=max_length)
    
    return render_template('index1.html', current_db=current_db)

@app.route('/add_to_bucket', methods=['POST'])
def add_to_bucket():
    selected_data = request.json.get('data', [])
    print("Received data:", selected_data)  # Log the received data
    
    # Clear existing bucket data
    session['bucket'] = []

    # Add new data to the bucket
    session['bucket'].extend(selected_data)

    print("Updated bucket:", session['bucket'])  # Log the updated bucket
    return jsonify(success=True)

@app.route('/download_bucket', methods=['GET'])
def download_bucket():
    bucket_data = session.get('bucket', [])
    print("Bucket data for download:", bucket_data)  # Log the bucket data
    
    # Prepare the output for CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    if not bucket_data:
        return "No data in bucket", 400

    # Extract headers and data
    headers = [column[0] for column in bucket_data]
    data = [column[1:] for column in bucket_data]

    # Write headers
    writer.writerow(headers)

    # Find the maximum length of data in any column
    max_rows = max(len(column) for column in data)
    
    # Write data row by row
    for row_index in range(max_rows):
        row = []
        for column in data:
            # Append the data if available, otherwise append an empty string
            row.append(column[row_index] if row_index < len(column) else "")
        writer.writerow(row)

    # Clear the bucket after generating the CSV
    session['bucket'] = []

    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=bucket_data.csv'
    response.headers['Content-Type'] = 'text/csv'
    return response

@app.route('/download', methods=['POST'])
def download_data():
    selected_tables = request.form.get('selected_tables')
    if selected_tables:
        selected_tables = json.loads(selected_tables)
        # Filter the data based on selected tables
        data_to_download = {table: data_for_download[table] for table in selected_tables if table in data_for_download}
    else:
        # If no tables are selected, download all data
        data_to_download = data_for_download

    # Create a BytesIO stream to hold the Excel file
    output = io.BytesIO()

    # Use pandas to write data to an Excel file with multiple sheets
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for table_name, columns in data_to_download.items():
            table_data = {}
            
            # Collect column data for each table
            for column_name, df in columns.items():
                # Ensure that the DataFrame for this column is correctly formatted
                table_data[column_name] = df[column_name].values
            
            # Create a DataFrame for the collected data
            if table_data:
                sanitized_table_name = sanitize_sheet_name(table_name)
                df_combined = pd.DataFrame(table_data)  # Create a DataFrame where columns are adjacent
                
                # Write the DataFrame to a sheet in the Excel file
                df_combined.to_excel(writer, sheet_name=sanitized_table_name[:31], index=False)

    # Move back to the beginning of the BytesIO buffer
    output.seek(0)

    # Send the Excel file as a response
    return send_file(output, download_name='data.xlsx', as_attachment=True)

def sanitize_sheet_name(sheet_name):
    # Replace invalid characters with an underscore and limit to 31 characters
    return re.sub(r'[\[\]\:\*\?\/\\]', '_', sheet_name)[:31]
    
@app.route('/download_selected_columns', methods=['POST'])
def download_selected_columns():
    selected_data = request.json.get('data', [])
    
    # Prepare the output for CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write data row by row
    for row in selected_data:
        writer.writerow(row)

    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=selected_columns.csv'
    response.headers['Content-Type'] = 'text/csv'
    return response

if __name__ == '__main__':
    app.run(debug=True)
