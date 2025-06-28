# ETL DEMO

This project demonstrates a comprehensive data warehousing and analytics solution, covering the entire process from building a data warehouse to generating actionable insights.

The project is inspired by [DataWithBaraa’s](https://github.com/DataWithBaraa/sql-data-warehouse-project) SQL Data Warehouse Project.

## Objective

import data from two source system (ERP and CRM) provided a sCSV files. Cleanse and resllve data quality. Combine both sources into single, user friendly data model
crm
sales_details: transactional recors about sales and orers
cust_info: customer information
prd_info: current and history product information

erp: extra details about the customer

Repository Structure

## Data Source Overview

CRM Data
sales_details: Transactional records about sales and orders.

cust_info: Customer information.

prd_info: Current and historical product information.

ERP Data
Additional details about customers to enrich CRM data.

## Repository Structure

```
project-root/
├── src/ # ETL scripts and pipeline code
│ ├── main.py # Main entry point for running ETL
│ ├── etl/ # Extraction, transformation, and loading modules
│ └── utils/ # Utility scripts (e.g., logging config)
│
├── data/ # Raw and processed CSV files
│
├── tests/ # Unit and integration tests for ETL modules
│
├── etl.log # Log file generated during ETL runs
│
└── README.md # Project overview and instructions
```

# How to Run

Clone the repository:

bash
Copy
Edit
git clone https://github.com/yourusername/etl-demo.git
Prepare your CSV source files and place them in the data/ folder.

Run the ETL pipeline:

css
Copy
Edit
python src/main.py
Check etl.log for detailed logs.

## Logging

Info level and above are logged to etl.log.

Debug and above are output to the console for real-time monitoring.
