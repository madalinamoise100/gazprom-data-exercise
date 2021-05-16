# gazprom-data-exercise

-> List of libraries necessary:

pandas
Flask
sqlalchemy
ntpath
glob
numpy

-> Data ingestion script [data_ingestion.py]

This script is used to perform the data ingestion.

It creates the necessary tables using PostgreSQL and SQLAlchemy:

1. a table that holds meter data about all the meters
1. a table that holds file data about all the files
1. a table that holds meter readings

It looks in the 'sample_data' folder to find files and ingests the data into the relevant tables.

If if finds a file it has not seen before, it adds it to the database. If the file is already present in the database, it does not read it again.

Before adding a file to the database, it checks if it is in the right format (header, footer).

If it finds a meter it has not seen before, it adds it to the meter table.

If it finds an entry for a meter recorded at a timestamp already present in the table, it checks the timestamp of the existing entry and the timestamp of the current entry.

If the current entry was recorded after the existing one, the table is updates.

If the current entry was recorded before the existing one, then the table is already up-to-date and nothing is done.

-> Flask application

To run the application:

1. run app.py
2. in browser: http://localhost:1337/app/[endpoint]

This is a simple API with the following endpoints:

app/meters:

  returns json data which included the total number of meters and a list of meter ids

app/meter/[meter_id]

  returns json data which included total consumption recorded for this meter and all entries present in the database for this MeterReading

app/files

  returns json data which included the total number of files and a list of file ids and creation times

app/last_file

  returns json data which includes the name of the most recent file and when it was created
