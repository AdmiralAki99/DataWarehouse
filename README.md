# Data Warehouse for Hospital Costs Analysis Post-COVID

## Project Overview

This project aims to analyze the impact of COVID-19 on hospital costs. It leverages publicly available data from the Centers for Medicare & Medicaid Services (CMS) for hospital cost information and the U.S. Bureau of Labor Statistics for employment data to see the trends in both post covid.

## Architecture Design

<figure>
  <img src="./Final Diagram.png" alt="Alt text" />
  <figcaption><strong>Figure 1: Data Warehouse Design Diagram</strong></figcaption>
</figure>

## Data Sources

1. **CMS Hospital Costs Data:** Provides detailed information on hospital costs (Medicaid,CHIP, Hospital Information, etc.).
2. **U.S. Bureau of Labor Statistics Employment Data:** Offers insights into the labor market and employment trends.

## Features

- **ETL Automation:** The ETL process is fully automated to streamline data ingestion and processing. Structured into abstracted function for easier use and integration into a pipeline and also for maintainability and feature updates in the future.
- **Data Cleaning & Transformation:** The data pipeline includes creating a subset, cleaning the data, and transforming the data into dimensional tables.
- **Analytics and Insights:** Post-transformation data is analyzed using Tableau to create graphs which are then used to gain insights from the KPI's stored in the fact tables.

## Technologies Used

- **Database:** SQL Server
- **ETL Tools:** Pyodbc, Pandas, Numpy
- **Programming Languages:** Python, SQL
- **Data Visualization:** Tableau