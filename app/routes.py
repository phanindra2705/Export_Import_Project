import snowflake.connector
import boto3.s3
from botocore.client import BaseClient
from flask import Flask, jsonify, request, render_template, redirect, url_for, flash, session
import pandas as pd
import time
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from app import (
    app,
    s3_bucket_name,
    snowflake_user,
    snowflake_password,
    snowflake_account,
    snowflake_warehouse,
    snowflake_database,
    snowflake_schema
)
from __init__ import (app, s3_bucket_name, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse,
                      snowflake_database, snowflake_schema)
from models import create_users_table, get_snowflake_tables, fetch_table_data, create_import_table
from utils import s3, upload_to_s3
from email_utils import send_email
# from . import app, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema
# from app import app, s3_bucket_name, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema


@app.route('/')
def index():
    return redirect(url_for('signup'))


@app.route('/s3_files')
def s3_files():
    response = s3.list_objects_v2(Bucket=s3_bucket_name)
    csv_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
    return jsonify(csv_files)


@app.route('/export', methods=['GET', 'POST'])
def export():
    if 'username' not in session:
        flash('Please sign in first.')
        return redirect(url_for('signin'))

    if request.method == 'POST':
        table_name = request.form['table_name']
        start_time = time.time()  # Record the start time
        table_data = fetch_table_data(table_name)
        if table_data is not None:
            csv_content = table_data.to_csv(index=False)
            now = datetime.now()
            formatted_date = now.strftime("%Y-%m-%d-%H-%M-%S")
            file_name = f"{table_name}_{formatted_date}.csv"
            if upload_to_s3(csv_content, file_name):
                end_time = time.time()  # Record the end time
                duration = end_time - start_time  # Calculate the duration
                success_message = f"File '{file_name}' exported and uploaded to S3 bucket '{s3_bucket_name}' in {duration:.2f} seconds."
                session['success_message'] = success_message

                # Send email notification
                recipient_email = session['user_email']  # Replace with actual session variable
                subject = "Data Export Notification"
                body = f"Dear {session['username']},\n\nYour data export operation for '{file_name}' has been completed successfully in {duration:.2f} seconds."

                send_email(subject, recipient_email, body)

                return redirect(url_for('dashboard'))
            else:
                flash("Error uploading file to S3")
                return redirect(url_for('export'))
        else:
            flash("Error fetching table data!")
            return redirect(url_for('export'))
    else:
        tables = get_snowflake_tables()
        return render_template('index_export.html', tables=tables)



# Assuming you have these imported and set up properly:
# s3, s3_bucket_name, create_import_table, send_email functions

@app.route('/import', methods=['GET', 'POST'])
def import_data():
    if 'username' not in session:
        flash('Please sign in first.')
        return redirect(url_for('signin'))

    if request.method == 'POST':
        start_time = time.time()  # Record the start time
        s3_file_key = request.form['s3_file_key']
        local_file_path = '/tmp/temp_file.csv'

        try:
            # Download the selected file from S3
            s3.download_file(s3_bucket_name, s3_file_key, local_file_path)

            # Read the CSV file into a DataFrame
            df = pd.read_csv(local_file_path)

            # Create table name with username and file identifier
            username = session['username']
            table_name = f"{s3_file_key.split('.')[0].replace('-', '_')}_{s3_bucket_name}"

            # Create table and insert data into Snowflake
            if create_import_table(username, table_name, df):
                end_time = time.time()  # Record the end time
                duration = end_time - start_time  # Calculate the duration
                session['success_message'] = f"Table '{table_name}' created and data inserted successfully to Snowflake in {duration:.2f} seconds."

                # Send email notification
                recipient_email = session.get('user_email')  # Use .get() to safely retrieve user_email
                if recipient_email:
                    subject = "Data Import Notification"
                    body = f"Dear {session['username']},\n\nYour data import operation for '{table_name}' has been completed successfully in {duration:.2f} seconds."
                    send_email(subject, recipient_email, body)

                return redirect(url_for('dashboard'))
            else:
                flash("Error importing data to Snowflake.")
                return redirect(url_for('import_data'))

        except Exception as e:
            flash(f"Error: {str(e)}")
            return redirect(url_for('import_data'))

    else:
        # List all CSV files in the S3 bucket
        response = s3.list_objects_v2(Bucket=s3_bucket_name)
        csv_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
        return render_template('index_import.html', csv_files=csv_files)


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password_hash = generate_password_hash(password)
        gmail = request.form['gmail']

        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        )
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM users WHERE username = %s", (username,))
        existing_username = cursor.fetchone()
        if existing_username:
            flash('Username already exists. Please choose a different username.')
            return redirect(url_for('signup'))
        cursor.execute("SELECT gmail FROM users WHERE gmail = %s", (gmail,))
        existing_gmail = cursor.fetchone()
        if existing_gmail:
            flash('Email already exists. Please use a different email.')
            return redirect(url_for('signup'))
        cursor.execute("INSERT INTO users (username, password_hash, gmail) VALUES (%s, %s, %s)",
                       (username, password_hash, gmail))
        conn.commit()
        cursor.close()
        conn.close()
        flash('Signup successful! Please sign in.')
        session['username'] = username
        session['user_email'] = gmail
        return redirect(url_for('signin'))
    return render_template('signup.html')


@app.route('/signin', methods=['GET', 'POST'])
def signin():
    if request.method == 'POST':
        login = request.form['login']
        password = request.form['password']

        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        )
        cursor = conn.cursor()
        cursor.execute("SELECT username, password_hash FROM users WHERE username = %s OR gmail = %s", (login, login))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result:
            stored_username, stored_password_hash = result
            if check_password_hash(stored_password_hash, password):
                session['username'] = stored_username
                conn = snowflake.connector.connect(
                    user=snowflake_user,
                    password=snowflake_password,
                    account=snowflake_account,
                    warehouse=snowflake_warehouse,
                    database=snowflake_database,
                    schema=snowflake_schema
                )
                cursor = conn.cursor()
                cursor.execute("SELECT gmail FROM users WHERE username = %s", (stored_username,))
                user_email = cursor.fetchone()[0]
                session['user_email'] = user_email
                cursor.close()
                conn.close()
                return redirect(url_for('dashboard'))
            else:
                flash('Incorrect username/email or password.')
                return redirect(url_for('signin'))
        else:
            flash('Incorrect username/email or password.')
            return redirect(url_for('signin'))
    return render_template('signin.html')


@app.route('/dashboard') #dashboard
def dashboard():
    if 'username' not in session:
        flash('Please sign in first.')
        return redirect(url_for('signin'))
    success_message = session.pop('success_message', None)
    return render_template('dashboard.html', username=session['username'], success_message=success_message)


@app.route('/logout')
def logout():
    session.pop('username', None)
    flash('You have been logged out.')
    return redirect(url_for('signin'))


@app.route('/create-users-table', methods=['POST'])
def create_users_table_api():
    try:
        create_users_table()
        return jsonify({'message': 'Users table created successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/import-table', methods=['POST'])
def import_table_api():
    data = request.get_json()
    username = data.get('username')
    table_name = data.get('table_name')
    df_data = data.get('data')

    if not username or not table_name or not df_data:
        return jsonify({'error': 'Missing required parameters'}), 400

    df = pd.DataFrame(df_data)

    if create_import_table(username, table_name, df):
        return jsonify({'message': f"Table '{username}_{table_name}' created successfully"}), 200
    else:
        return jsonify({'error': 'Failed to create table'}), 500

@app.route('/tables', methods=['GET'])
def get_snowflake_tables_api():
    try:
        tables = get_snowflake_tables()
        return jsonify({'tables': tables}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/fetch-table-data/<table_name>', methods=['GET'])
def fetch_table_data_api(table_name):
    try:
        df = fetch_table_data(table_name)
        data = df.to_dict(orient='records')
        return jsonify({'data': data}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)