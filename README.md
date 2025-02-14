Welcome to DBT Mart's Demo Project!

## Overview
This project is designed to transform raw data into meaningful insights using dbt (data build tool). The primary goal is to create and manage datamarts that provide a consolidated view of data for various business needs.

## Project Structure
datamarts_dbt_project/ ├── models/ │ ├── marts/ │ │ ├── finance/ │ │ │ └── finance_mart.sql │ │ ├── sales/ │ │ │ └── sales_mart.sql │ ├── staging/ │ │ ├── finance/ │ │ │ └── stg_finance.sql │ │ ├── sales/ │ │ │ └── stg_sales.sql ├── snapshots/ ├── tests/ ├── macros/ ├── analyses/ ├── seeds/ ├── dbt_project.yml └── README.md

Try running the following commands:
- dbt run --vars '{"buss_dt": "2025-02-25", "run_id": "9999"}'
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
