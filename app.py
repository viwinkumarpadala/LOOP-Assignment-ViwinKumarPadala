#imported all the require libraries here

from flask import Flask, request, jsonify,send_file
from sqlalchemy import create_engine, text
import pandas as pd
import random
import string
import os
import threading
from datetime import datetime, timedelta,time
import pandas as pd
from sqlalchemy import create_engine, text
import pytz
from pathlib import Path
import uuid
import os
import threading
from flask import make_response

#Creating an instance of Flask Class
app = Flask(__name__)

#Connect to the database
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# Create a MySQL database connection
db_url = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# Establish a connection
connection = engine.connect()


#processing function
def taskfunction(store_current,business_hours,current_timestamp2,id):

    #get all the data and business hours of restaurants
    df_status_data = store_current
    df_business_hours = business_hours

    # Sort status_data by timestamp
    df_status_data = df_status_data.copy()
    df_status_data['timestamp_utc'] = pd.to_datetime(df_status_data['timestamp_utc'])
    df_status_data = df_status_data.sort_values(by='timestamp_utc')

    #assume that the max timestamp in the overall data as the current timestamp
    current_timestamp = pd.to_datetime(current_timestamp2)

    # Add a new column for local time
    local_timezone = pytz.timezone(df_status_data['timezone_str'].iloc[0])

    # Assuming the timestamp is in a DataFrame column named 'timestamp_utc'
    df_status_data['local_time'] = df_status_data['timestamp_utc'].dt.tz_convert(local_timezone)
    df_status_data['day'] = df_status_data['local_time'].dt.dayofweek
    df_status_data['check_time'] = df_status_data['local_time'].dt.strftime('%H:%M:%S')

    #convert the time to local time zone
    utc_time=current_timestamp

    # print(local_timezone)
    timezone_converted_curr_timestamp=utc_time.astimezone(local_timezone)

    #previous day info
    previous_day_timestamp=timezone_converted_curr_timestamp- timedelta(days=1)
    prev_day_index=previous_day_timestamp.weekday()

    # Create a DataFrame with all combinations of store_id and day_of_week
    all_combinations = pd.DataFrame([(store_id, day_of_week) for store_id in df_status_data['store_id'].unique() for day_of_week in range(7)], columns=['store_id', 'day'])

    # Merge with business_hours data to fill in missing data
    df_combined = all_combinations.merge(df_business_hours, how='left', on=['store_id', 'day'])

    # Fill missing values with default open and close times(Handling the missing data in busines hours csv file)
    df_combined['start_time_local'].fillna(pd.to_datetime('00:00:00').time(), inplace=True)
    df_combined['end_time_local'].fillna(pd.to_datetime('23:59:59').time(), inplace=True)

    print(df_combined)





    #Processing for the last day uptime and downtime

    # Assuming df_status_data is your DataFrame containing status data
    previous_day_data_status = df_status_data[df_status_data['local_time'].dt.date == previous_day_timestamp.date()]

   
    uptime_last_day = timedelta()
    downtime_last_day = timedelta()
    prev_day_start_time=df_combined[df_combined['day']==prev_day_index]['start_time_local'].values[0]
    prev_day_end_time=df_combined[df_combined['day']==prev_day_index]['end_time_local'].values[0]
    curr_time = datetime.strptime(str(prev_day_start_time), '%H:%M:%S').time()
    close_time= datetime.strptime(str(prev_day_end_time), '%H:%M:%S').time()

    if len(previous_day_data_status)>0:

        previous_day_data_status = previous_day_data_status.copy()
        previous_day_data_status.loc[:, 'check_time'] = pd.to_datetime(previous_day_data_status['check_time']).dt.time


        # Filter the data in business hours
        prev_day_start_time = datetime.strptime(str(prev_day_start_time), "%H:%M:%S").time()
        prev_day_end_time = datetime.strptime(str(prev_day_end_time), "%H:%M:%S").time()



        filtered_data = previous_day_data_status[
            (previous_day_data_status['check_time'] >= prev_day_start_time) &
            (previous_day_data_status['check_time'] <= prev_day_end_time)
        ]

        # print(filtered_data)


        status = 'active'
        curr_time = datetime.strptime(str(prev_day_start_time), '%H:%M:%S').time()
        close_time= datetime.strptime(str(prev_day_end_time), '%H:%M:%S').time()

        for i in range(len(filtered_data)):
            check_time = datetime.strptime(str(filtered_data.iloc[i]['check_time']), '%H:%M:%S').time()

            if filtered_data.iloc[i]['status'] == 'active' and status == 'active':
                uptime_last_day += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_time)
                curr_time = check_time
            elif filtered_data.iloc[i]['status'] == 'inactive' and status == 'inactive':
                downtime_last_day += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_time)
                curr_time = check_time
            elif filtered_data.iloc[i]['status'] == 'inactive' and status == 'active':
                downtime_last_day += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_time)
                curr_time = check_time
                status = 'inactive'
            elif filtered_data.iloc[i]['status'] == 'active' and status == 'inactive':
                uptime_last_day += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_time)
                curr_time = check_time
                status = 'active'

        if status=='active':
            uptime_last_day+= datetime.combine(datetime.today(), close_time)- datetime.combine(datetime.today(), curr_time)
        if status=='inactive':
            downtime_last_day+= datetime.combine(datetime.today(), close_time)- datetime.combine(datetime.today(), curr_time)

    else:
        uptime_last_day+= datetime.combine(datetime.today(), close_time)- datetime.combine(datetime.today(), curr_time)



    #processing for last week down time

    uptime_last_week = timedelta()
    downtime_last_week = timedelta()
   

    #Processing for each day in last 1 week
    for i in range(7):
        previous_day_of_week_timestamp=timezone_converted_curr_timestamp- timedelta(days=i+1)
        prev_day_of_week_index=previous_day_of_week_timestamp.weekday()
        previous_day_of_week_data_status = df_status_data[df_status_data['local_time'].dt.date == previous_day_of_week_timestamp.date()]
        prev_day_of_week_start_time=df_combined[df_combined['day']==prev_day_of_week_index]['start_time_local'].values[0]
        prev_day_of_week_end_time=df_combined[df_combined['day']==prev_day_of_week_index]['end_time_local'].values[0]
        curr_day_of_week_time = datetime.strptime(str(prev_day_of_week_start_time), '%H:%M:%S').time()
        close_day_of_week_time= datetime.strptime(str(prev_day_of_week_end_time), '%H:%M:%S').time()
        print(previous_day_of_week_timestamp)
        if len(previous_day_of_week_data_status)>0:

            previous_day_of_week_data_status = previous_day_of_week_data_status.copy()
            previous_day_of_week_data_status.loc[:, 'check_time'] = pd.to_datetime(previous_day_of_week_data_status['check_time']).dt.time


            # Filter the data in business hours
            prev_day_of_week_start_time = datetime.strptime(str(prev_day_of_week_start_time), "%H:%M:%S").time()
            prev_day_of_week_end_time = datetime.strptime(str(prev_day_of_week_end_time), "%H:%M:%S").time()



            filtered_data = previous_day_of_week_data_status[
                (previous_day_of_week_data_status['check_time'] >= prev_day_of_week_start_time) &
                (previous_day_of_week_data_status['check_time'] <= prev_day_of_week_end_time)
            ]

            
            if len(filtered_data)==0:
                curr_day_of_week_time = datetime.strptime(str(prev_day_of_week_start_time), '%H:%M:%S').time()
                close_day_of_week_time= datetime.strptime(str(prev_day_of_week_end_time), '%H:%M:%S').time()
                uptime_last_week += datetime.combine(datetime.today(), close_day_of_week_time) - datetime.combine(datetime.today(), curr_day_of_week_time)
                

            status = 'active'
            curr_day_of_week_time = datetime.strptime(str(prev_day_of_week_start_time), '%H:%M:%S').time()
            close_day_of_week_time= datetime.strptime(str(prev_day_of_week_end_time), '%H:%M:%S').time()

            for i in range(len(filtered_data)):
                check_time = datetime.strptime(str(filtered_data.iloc[i]['check_time']), '%H:%M:%S').time()

                if filtered_data.iloc[i]['status'] == 'active' and status == 'active':
                    uptime_last_week += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_day_of_week_time)
                    curr_day_of_week_time = check_time
                elif filtered_data.iloc[i]['status'] == 'inactive' and status == 'inactive':
                    downtime_last_week += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_day_of_week_time)
                    curr_day_of_week_time = check_time
                elif filtered_data.iloc[i]['status'] == 'inactive' and status == 'active':
                    downtime_last_week += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_day_of_week_time)
                    curr_day_of_week_time = check_time
                    status = 'inactive'
                elif filtered_data.iloc[i]['status'] == 'active' and status == 'inactive':
                    uptime_last_week += datetime.combine(datetime.today(), check_time) - datetime.combine(datetime.today(), curr_day_of_week_time)
                    curr_day_of_week_time = check_time
                    status = 'active'

            if status=='active':
                uptime_last_week+= datetime.combine(datetime.today(), close_day_of_week_time)- datetime.combine(datetime.today(), curr_day_of_week_time)
            if status=='inactive':
                downtime_last_week+= datetime.combine(datetime.today(), close_day_of_week_time)- datetime.combine(datetime.today(), curr_day_of_week_time)

        else:
            uptime_last_week+= datetime.combine(datetime.today(), close_day_of_week_time)- datetime.combine(datetime.today(), curr_time)
        
        print(f"uptime today: {uptime_last_week}")
        print(f"downtime today: {downtime_last_week}")


    #processing for last day

    uptime_last_hour=timedelta()
    downtime_last_hour=timedelta()

    utc_time=current_timestamp
    local_timezone = df_status_data['timezone_str'].iloc[0]

    # print(local_timezone)
    timezone_converted_curr_timestamp=utc_time.astimezone(local_timezone)
    # print(timezone_converted_curr_timestamp)
    last_hour_timestamp=timezone_converted_curr_timestamp- timedelta(hours=1)
    # print(last_hour_timestamp)
    last_hour_day_index=last_hour_timestamp.weekday()

    last_day_data_status = df_status_data[df_status_data['local_time'].dt.date == last_hour_timestamp.date()]
    last_day_start_time=df_combined[df_combined['day']==last_hour_day_index]['start_time_local'].values[0]
    last_day_end_time=df_combined[df_combined['day']==last_hour_day_index]['end_time_local'].values[0]
    curr_day_time = datetime.strptime(str(last_day_start_time), '%H:%M:%S').time()
    close_day_time= datetime.strptime(str(last_day_end_time), '%H:%M:%S').time()
    last_hour_start_time=last_hour_timestamp.strftime('%H:%M:%S')
    last_hour_end_time=timezone_converted_curr_timestamp.strftime('%H:%M:%S')

    print(f"last hour start time: {last_hour_start_time}")
    print(f"last hour end time: {last_hour_end_time}")

    filtered_data=pd.DataFrame()
    if len(last_day_data_status)>0:

        last_day_data_status = last_day_data_status.copy()
        last_day_data_status.loc[:, 'check_time'] = pd.to_datetime(last_day_data_status['check_time']).dt.time

        # Filter the data in business hours
        last_day_start_time = datetime.strptime(str(last_day_start_time), "%H:%M:%S").time()
        last_day_end_time = datetime.strptime(str(last_day_end_time), "%H:%M:%S").time()



        filtered_data = last_day_data_status[
            (last_day_data_status['check_time'] >= last_day_start_time) &
            (last_day_data_status['check_time'] <= last_day_end_time)
        ]

        print(filtered_data)
    # ...

    status = 'active'
    compare_time = datetime.strptime(last_hour_start_time, '%H:%M:%S').time()
    last_hour_end_time = datetime.strptime(last_hour_end_time, '%H:%M:%S').time()
    
    # print('hello')

    if len(filtered_data)==0:
        downtime_last_hour+= datetime.combine(datetime.today(), last_hour_end_time)- datetime.combine(datetime.today(), compare_time)
    else:
        for i in range(len(filtered_data)):
            check_time = datetime.strptime(str(filtered_data.iloc[i]['check_time']), '%H:%M:%S').time()
           
            # print('hello')

            if type(last_hour_end_time) == str:
                last_hour_end_time = datetime.strptime(last_hour_end_time, '%H:%M:%S').time()


            if(check_time>=compare_time):
                curr_day_time=check_time
                if status == 'active' and filtered_data.iloc[i]['status'] == 'active' and curr_day_time <= last_hour_end_time:
                    uptime_last_hour += datetime.combine(datetime.today(), curr_day_time) - datetime.combine(datetime.today(), compare_time)
                elif status == 'inactive' and filtered_data.iloc[i]['status'] == 'inactive' and curr_day_time <= last_hour_end_time:
                    downtime_last_hour += datetime.combine(datetime.today(), curr_day_time) - datetime.combine(datetime.today(), compare_time)
                elif status == 'active' and filtered_data.iloc[i]['status'] == 'inactive' and curr_day_time <= last_hour_end_time:
                    downtime_last_hour += datetime.combine(datetime.today(), curr_day_time) - datetime.combine(datetime.today(), compare_time)
                    status='inactive'
                elif status == 'inactive' and filtered_data.iloc[i]['status'] == 'active' and curr_day_time <= last_hour_end_time:
                    uptime_last_hour += datetime.combine(datetime.today(), curr_day_time) - datetime.combine(datetime.today(), compare_time)
                    status='active'
                compare_time=check_time
                
            if (
                filtered_data.iloc[i]['status'] == 'active'
                and status == 'active'
                and check_time < compare_time
            ):
                curr_day_time = check_time
            elif (
                filtered_data.iloc[i]['status'] == 'inactive'
                and status == 'inactive'
                and check_time < compare_time
            ):
                curr_day_time = check_time
            elif (
                filtered_data.iloc[i]['status'] == 'inactive'
                and status == 'active'
                and check_time < compare_time
            ):
                curr_day_time = check_time
                status = 'inactive'
            elif (
                filtered_data.iloc[i]['status'] == 'active'
                and status == 'inactive'
                and check_time < compare_time
            ):
                curr_day_time = check_time
                status = 'active'
        
        if status=='active':
            uptime_last_hour+=datetime.combine(datetime.today(), last_hour_end_time)- datetime.combine(datetime.today(), compare_time)
        else:
            downtime_last_hour+=datetime.combine(datetime.today(), last_hour_end_time)- datetime.combine(datetime.today(), compare_time)
    
        
   
    def timedelta_to_hours(td):
        return td.total_seconds() / 3600.0
    def timedelta_to_minutes(td):
        return td.total_seconds() / 60.0

    uptime_last_week_hours = timedelta_to_hours(uptime_last_week)
    downtime_last_week_hours = timedelta_to_hours(downtime_last_week)
    uptime_last_day_hours = timedelta_to_hours(uptime_last_day)
    downtime_last_day_hours = timedelta_to_hours(downtime_last_day)
    uptime_last_hour_minutes = timedelta_to_minutes(uptime_last_hour)
    downtime_last_hour_minutes = timedelta_to_minutes(downtime_last_hour)

    print(f"Uptime last week: {uptime_last_week_hours:.3f} hours")
    print(f"Downtime last week: {downtime_last_week_hours:.3f} hours")
    print(f"Uptime last day: {uptime_last_day_hours:.3f} hours")
    print(f"Downtime last day: {downtime_last_day_hours:.3f} hours")
    print(f"Uptime last hour: {uptime_last_hour_minutes}")
    print(f"Downtime last hour: {downtime_last_hour_minutes}")

    # csv_directory = "./reports/"
    # os.makedirs(csv_directory, exist_ok=True)

    

    csv_file_path = f"./reports/{id}.csv"
    store_id = df_status_data['store_id'].iloc[0]

    # Check if the file exists
    if os.path.exists(csv_file_path):
        try:
            existing_results_df = pd.read_csv(csv_file_path)
            if existing_results_df.empty:
                # If the file is empty, create a new DataFrame with columns
                existing_results_df = pd.DataFrame(columns=['store_id', 'uptime_last_hour', 'uptime_last_day',
                                                            'uptime_last_week', 'downtime_last_hour',
                                                            'downtime_last_day', 'downtime_last_week'])
        except pd.errors.EmptyDataError:
            # If the file doesn't exist, create a new DataFrame with columns
            existing_results_df = pd.DataFrame(columns=['store_id', 'uptime_last_hour', 'uptime_last_day',
                                                        'uptime_last_week', 'downtime_last_hour',
                                                        'downtime_last_day', 'downtime_last_week'])
    else:
        # If the file doesn't exist, create a new DataFrame with columns
        existing_results_df = pd.DataFrame(columns=['store_id', 'uptime_last_hour', 'uptime_last_day',
                                                    'uptime_last_week', 'downtime_last_hour',
                                                    'downtime_last_day', 'downtime_last_week'])

    # Continue with the rest of your code
    # ...

    if store_id in existing_results_df['store_id'].values:
        # Update the existing row
        existing_results_df.loc[existing_results_df['store_id'] == store_id, 'uptime_last_hour'] = round(uptime_last_hour_minutes, 2)
        existing_results_df.loc[existing_results_df['store_id'] == store_id, 'uptime_last_day'] = round(uptime_last_day_hours, 2)
        existing_results_df.loc[existing_results_df['store_id'] == store_id, 'uptime_last_week'] = round(uptime_last_week_hours, 2)
        existing_results_df.loc[existing_results_df['store_id'] == store_id, 'downtime_last_hour'] = round(downtime_last_hour_minutes, 2)
        existing_results_df.loc[existing_results_df['store_id'] == store_id, 'downtime_last_day'] = round(downtime_last_day_hours, 2)
        existing_results_df.loc[existing_results_df['store_id'] == store_id, 'downtime_last_week'] = round(downtime_last_week_hours, 2)
    else:
        # Append a new row
        new_row = [store_id, round(uptime_last_hour_minutes, 2), round(uptime_last_day_hours, 2),
                round(uptime_last_week_hours, 2), round(downtime_last_hour_minutes, 2),
                round(downtime_last_day_hours, 2), round(downtime_last_week_hours, 2)]
        existing_results_df = pd.concat([existing_results_df, pd.DataFrame([new_row], columns=existing_results_df.columns)], ignore_index=True)

    # Save the updated DataFrame to the CSV file
    existing_results_df.to_csv(csv_file_path, index=False)


#Global variable to store if a process is still running or complete
processing_info = {}

#Processing all the store ids information
def process_all_stores(filename):
    # all_stores_query = text("SELECT DISTINCT store_id FROM status_data")
    status_data_query = text("SELECT sd.store_id, sd.status, sd.timestamp_utc, COALESCE(td.timezone_str, 'America/Chicago') AS timezone_str FROM status_data sd LEFT JOIN timezones td ON sd.store_id = td.store_id WHERE sd.store_id='8419537941919820732'")
    business_hours_query = text("SELECT * FROM business_hours")

    # df_all_stores = pd.read_sql(all_stores_query, con=connection)
    df_status_data = pd.read_sql(status_data_query, con=connection)
    df_business_hours = pd.read_sql(business_hours_query, con=connection)
    current_timestamp = df_status_data['timestamp_utc'].max()
    print(current_timestamp)
    print(filename)

    # Group df_status_data by store_id
    grouped_data = df_status_data.groupby('store_id')

    for store_id, group_data in grouped_data:
        # Process each group_data here
        taskfunction(group_data, df_business_hours, current_timestamp,filename)
    processing_info[filename]="Complete"

REPORTS_DIR = "reports"

# Create the reports directory if it doesn't exist
Path(REPORTS_DIR).mkdir(parents=True, exist_ok=True)


# Generate a random string as the report_id
def generate_report_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))


#'/' route for testing if the server is working or not
@app.route('/')
def hello():
    return "Hello world"


# '/trigger_report' route for triggering the processing function
@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    # Generate a unique report ID
    report_id = str(uuid.uuid4())
    processing_info[report_id] = "Running"

    # Start a new thread to process all stores
    thread = threading.Thread(target=process_all_stores, args=(report_id,))
    thread.start()

    return jsonify({"report_id": report_id})

    

#'/get_report' route for getting the report using the report id
@app.route('/get_report', methods=['GET'])
def get_report():
    # Get the report_id from the request
    report_id = request.args.get('report_id')
    
    #Checking if the thread is completed or not, if it is completed then we are moving forward and returning the csv.
    #Also Test is a csv file that was completed running previously and can be used for reference of the output purpose.
    if report_id == "Test" or processing_info.get(report_id) == "Complete":
        report_path = os.path.join(REPORTS_DIR, f"{report_id}.csv")
        if os.path.exists(report_path):
            response = make_response(send_file(report_path, as_attachment=True))
            response.headers['Content-Type'] = 'application/csv'  
            response.headers['status'] = 'Complete'
            return response
        else:
            return jsonify({"status": "Report not found"})
    elif processing_info.get(report_id) == "Running":
        return jsonify({"status": "Running"})
    else:
        return jsonify({"status": "Report not found"})



if __name__ == '__main__':
    app.run(debug=True)