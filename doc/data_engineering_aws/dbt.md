What is DBT?
DBT allows you to write SQL SELECT statements to transform raw data into clean, usable models. It compiles these queries into executable SQL, manages dependencies between models, and runs them in the correct order on your data warehouse (e.g., Snowflake, BigQuery, Redshift). Key features include:

Modularity: Break transformations into reusable SQL models.
Version Control: Manage data transformations like code using Git.
Testing: Validate data quality with built-in and custom tests.
Documentation: Automatically generate documentation for your data models.
Orchestration: Schedule and run transformations with DBT Cloud or other schedulers.

Core Concepts
- Models: SQL files (*.sql) that define transformations. Each model is a SELECT query that creates a table or view in your data warehouse.
- Sources: source macro is designed to reference tables or views that already exist in your data warehouse and were not created by dbt itself, usually loaded by ELT/ETL pipelines.
- Seeds: Static CSV files loaded into the warehouse as tables (e.g., for lookup tables). `dbt seed --target <db>` (e.g dbt seed --target duckdb) to insert data into database. 
- Tests: Assertions to validate data integrity (e.g., uniqueness, non-null checks).
- Macros: Reusable Jinja templates for writing DRY (Don’t Repeat Yourself) SQL code.
- Snapshots: Capture historical changes in mutable data by creating slowly changing dimension (SCD) tables.dbt snapshots are designed to implement Slowly Changing Dimension (SCD) Type 2

- Packages: Reusable DBT code from the community or your organization.
- Profiles: Configuration files (`profiles.yml`) that define how DBT connects to your data warehouse.
Resource-specific configurations are applicable to only one dbt resource type rather than multiple resource types. You can define these settings in the project file (dbt_project.yml), a property file (models/properties.yml for models, similarly for other resources), or within the resource’s file using the {{ config() }} macro.
`dbt init my_project`

- dbt_project.yml (`~/.dbt/profiles.yml`): Core configuration file specifying project settings, model paths, and targets.
- models/: Contains SQL files for transformations (e.g., models/staging/stg_customers.sql). there is a schema file to map source and to document model
- tests/: Custom SQL tests to validate models.
- macros/: Jinja templates for reusable logic.
- seeds/: CSV files for static data.
- snapshots/: SQL files for capturing historical data changes.
- analyses/: Ad-hoc SQL queries that don’t materialize in the warehouse.

`dbt seed` : loads CSV files from the seeds/ folder into the warehouse.
`dbt run` : Executes models to create tables/views in your warehouse.
`dbt debug`        # Test the connection
`dbt test`: Runs tests defined in your project (e.g., uniqueness, not_null).
`dbt build `:        Runs models + tests + snapshots
` dbt docs serve`.


## Instalation
- dbt-core: `pip install dbt-core `
`pip install dbt-core dbt-<adapter>`
Replace <adapter> with your data warehouse (e.g., dbt-snowflake,dbt-duckdb, dbt-bigquery, dbt-postgres).

- If use duckdb: 
export PATH="/Users/u249637/.duckdb/cli/latest:$PATH"
    `python -m pip install dbt-duckdb` 

/Users/u249637/.duckdb/cli/latest/duckdb
- if use postgresql: 

## set up project
python3 -m venv dbt_venv
source dbt_venv/bin/activate
make sure you install dbt adaptor
`dbt init <project-name>`

## examples_tutorial
https://github.com/Data-Engineer-Camp/dbt-dimensional-modelling/tree/main
oad Only New or Changed Data (Incremental Loads) , use `{{ config(materialized='incremental') }}`
https://mbvyn.medium.com/understanding-dbt-incremental-materialization-c32318364241