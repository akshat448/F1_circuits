# Formula 1 circuit Project
## Introduction
This was a Python-based project that crawls data from Wikipedia using Apache Airflow, It was mostly an introductory project to using docker, PostgreSQL, and Apache Airflow and creating a Tableau dashboard for visualization.

## Data collection
Data is fetched from the Wikipedia page of List of Formula 1 Circuits(https://en.wikipedia.org/wiki/List_of_Formula_One_circuits) using beautiful soup for scraping the tabular data.

## Data Preprocessing/Cleaning
The scraped data is saved into a CSV file which is further used to clean the data by removing keys(e.g., "*", "nbsp", "[a]") found within the table. Then a new CSV table is created with the cleaned data.

Using the Geopy library on the city and country of the circuits, we created a new column for the latitudes and longitudes which are later used to create the tableau dashboard to displace the locations of the circuits on a world map.
Further, the countries are divided into their respective continents and a final CSV file is saved.

## Docker
A custom image is crafted using the "apache/airflow:slim-2.8.3" base image. Necessary libraries are installed within this Docker image to facilitate seamless integration with the project, 
this docker file was later used in docker-compose. The compose file contained services required to run airflow (webserver, PostgreSQL, and scheduler).

## Airflow
Created multiple Directed Acyclic Graphs (DAGs) which facilitate the extraction of data from the Wikipedia page, subsequent cleaning and transformation of the data, and the finalization of the processed data into a CSV file.




