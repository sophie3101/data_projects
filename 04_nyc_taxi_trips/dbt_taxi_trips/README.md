Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
NOte: the green trips and yello trips have 2 different column names, tpep_pickup_datetime for yellow vs lpep_pickup_datetime for green. so I run glue craw and create table (use this for dbt because if i rerun dag , it costs so much)
dbt seed
`dbt compile` -> generate sql 
`dbt run` to build all modelas
dbt run --select dbt_taxi_trips.staging.stg_yellow_trips
dbt list

create packages.yml and put 

`packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
`
then run dbt deps

dbt test 

dbt docs generate
dbt docs serve  --port 8081 # because port 8080 is used by airflow
