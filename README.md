# Data Pipeline Airflow

In this project my goal is to demonstrate the process of automation that is required to create a data warehouse in AWS Redshift.

The dataset files and database design is based on the project desribed in <a href="https://github.com/masoodqq/postgress_python_script" target="_blank"> postgress python script</a>

 <h3> Data Pipeline steps </h3>
 <ol>
 <li>Extract data from multiple S3 locations.</li>
<li>Load the data into Redshift cluster.</li>
<li>Transform the data into a star schema.</li>
<li>Perform data validation and data quality checks.</li>
</ol>
<hr style="width:50%;text-align:left;margin-left:0">

Following is a image of DAG implementing above steps.

<img src="/dag.png" alt="ERD" style="max-width:100%;">
