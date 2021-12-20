
# Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
The aim of this project is to create a Postgres database with tables designed to optimize queries on song play analysis and build an ETL pipeline using Python.
The built ETL pipeline should transfers data from files in two local directories into these tables (Fact & Dimension tables) in Postgres

## Tools Used

Python & Sql

## Modules

1. Create tables  - create_tables.py  
2. ETL Pipeline   - etl.py

## Execution steps

* Run the create_tables.py in Command window
* After successful run, execute etl.py
* Query the database and check for the values in the table by making use of queries in test.ipynb

## Tables & Schema

### Fact table

1.songplays - records in log data associated with song plays i.e. records with page NextSong Columns

### Dimension table

1.users - users in the app

2.songs - songs in music database

3.artists - artists in music database

4.time - timestamps of records in songplays broken down into specific units


 
