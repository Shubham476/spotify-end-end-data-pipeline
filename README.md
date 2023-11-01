# spotify-end-end-data-pipeline
### Intoduction
In this project, we will build an ETL(Extract, Transform, Load) pipeline using spontify API on AWS. The pipeline will retrive data from spontify API, transform it to desire format and load it into an AWS data storage and will perform analysis on data. 

### Architecture
![Architecture Diagram](https://github.com/Shubham476/spotify-end-end-data-pipeline/blob/main/spotify-etl-architecture-diagram.png)

### Services Used
1. **S3 (Simple Storage Service):** Amazon S3 or Amazon Simple Storage Service is a service offered by Amazon Web Services (AWS) that provides object storage through a web service interface. S3 is commonly used for data backup, archiving, hosting static websites, and as a data storage solution for various applications. It offers features like versioning, data replication, and access control to manage and protect your stored data.

2. **AWS Lambda:** AWS Lambda is an event-driven, serverless computing platform provided by Amazon as a part of Amazon Web Services. It is designed to enable developers to run code without provisioning or managing servers. It executes code in response to events and automatically manages the computing resources required by that code.

3. **Cloud Watch:** Amazon CloudWatch is a monitoring and observability service provided by Amazon Web Services (AWS). It is designed to collect and track metrics, collect and monitor log files, and set alarms.

4. **Glue Crawler:** A crawler can crawl multiple data stores in a single run. Upon completion, the crawler creates or updates one or more tables in your Data Catalog. Extract, transform, and load (ETL) jobs that you define in AWS Glue use these Data Catalog tables as sources and targets.

5. **Data Catalog:** The AWS Glue Data Catalog is a centralized metadata repository for all your data assets across various data sources. It provides a unified interface to store and query information about data formats, schemas, and sources. When an AWS Glue ETL job runs, it uses this catalog to understand information about the data and ensure that it is transformed correctly.

6. **AWS Athena:** Amazon Athena is a serverless, interactive analytics service built on open-source frameworks, supporting open-table and file formats. Athena provides a simplified, flexible way to analyze petabytes of data where it lives. Analyze data or build applications from an Amazon Simple Storage Service (S3) data lake and 30 data sources, including on-premises data sources or other cloud systems using SQL or Python.
