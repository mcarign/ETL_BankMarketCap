# ETL_BankMarketCap
Script that webscrapes from a webpage from 2023, performs etl and outputs the database.

An ETL script for the top 10 banks in 2023. It web scrapes a table from 2023, extracts the table containing the bank name and USD market cap, then transforms the USD to GBP, EUR, and INR using the appropriate conversion rate, and finally outputs the database displaying bank name, and USD, GBP, EUR, INR market cap.
