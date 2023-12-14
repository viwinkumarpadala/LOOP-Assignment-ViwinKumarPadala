# Store_Monitoring_System

## Technologies used: 
Flask web framework, Mysql database

## Routes:
### Trigger report route
http://127.0.0.1:5000/trigger_report

Result: will give a report id that will be randomly generated.
### Get report route
http://127.0.0.1:5000/get_report?report_id=id

Result:
1) If the report exists and it is still running the function for generating the reports, then it will return the status as "Running".
2) If the report exists and it has completed running the function for generating reports the then it will return the status as "Complete" along with the csv file.
3) If the file doesn't exist, then return the status as "Report not found".

## Working:
1) First create the database tables for the three csv files that we are using for the process by running a python script called  "dbcreate.py".
2) Now we have all the data required, stored in our Mysql database.
3) Now run the file "app.py" in the python virtual environment.
4) Now to trigger the function we can use this route : http://127.0.0.1:5000/trigger_report
5) Now to get the reports of the id, pass the id as a parameter and get the result using this route : http://127.0.0.1:5000/get_report?report_id=id


## Process explanation:
1) First we are getting all the stores data using query
"SELECT sd.store_id, sd.status, sd.timestamp_utc, COALESCE(td.timezone_str, 'America/Chicago') AS timezone_str FROM status_data sd LEFT JOIN timezones td ON sd.store_id = td.store_id".
2) This query will have all the stores statuses data along with the misiing values of timezones replaced with the value 'America/Chicago' as their timezone.
3) Now we are grouping the data by using store_id as a key, and processing the data for each store id using a for loop.
4) while processing the data, first we are sorting the data so that we have the data in a sorted value based on the timestamp.
5) We are then changing the timestamos based on their timezones and adding new columns for storing their business hours.
6) Now we have all the data required for processing.
7) First we will process the last day uptime and downtime.
8) when the store will be opened the status is considered to be active, and after the store is closed the status is considered as inactive.
9) Filter the data during the business hours.
10) In between the start time and end time, we are always considering the status of the  current timestamp and previous ones, and considering the uptime and downtime accordingly.
11) if the previous status is active, and the current status is active, then the uptime is added, so the time difference between them is added to calculate the uptime.
12) if the previous status is active, and the current status is inactive, then the downtime is added, so the time difference between them is added to calculate the downtime and the previous status is changed to inactive.
13) if the previous status is inactive, and the current status is inactive, then the downtime is added, so the time difference between them is added to calculate the downtime.
14)  if the previous status is inactive, and the current status is active, then the upwntime is added, so the time difference between them is added to calculate the uptime and the previous status is changed to active.
15)  Thus we will have the uptime and downtime in last day  by this process.
16)  For last 1 week we will be processing the data similarly for the last 7 days.
17)  Fot the last 1 hour, first we are calculating the status of the restaurant in the last 1 hr and then doing the similar process.
18)  Thus we will have all the required data in the csv file format.

## Points to be considered:
1) We have optimized the code by using a single database read, and grouping the data based on the store_ids and processing them instead of reading data for each store in the database individually.
2) The code is well structured and we are handling all the edge cases like missing data values in status data, business data, timezones data etc.
3) Created a functioning trigger, poll architecture using flask web framework, database reads and CSV output.
4) Calculated the uptimes and downtimes using a well defined logic.

## Assumptions made:
1) If the business hours data is missing then it is considered to be active 24/7.
2) If the timezone is missing it was considered as "America/Chicago" for their timezone.
3) If the status_data for a particular day is entirely missing, it is considered as active for that day. 

## Note: Store the required database credentials in the .env file before you will start working with it.

# Thank You
