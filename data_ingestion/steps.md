1. Run data_ingestion.py to get new parquet file in S3.
2. Run Glue crawler to update schema if necessary.
3. Athena now has all history in lifter table.
4. An Iceberg table has been created in openpowerlifting schema 
4. v_lifter pulls only the last day from the lifter table.
5. v_lifter_iceberg_delta pulls only the days after the latest iceberg ingestion.
6. insert_into_lifter_iceberg will insert content from v_lifter_iceberg_delta into lifter_iceberg

1. Run data_ingestion.py every day checking if file is already downloaded.
    Blocker: The hash method of checking data integrity does not seem to work. 
    Solution: Consider just setting up a weekly download with a check to see if data updated or not once in Athena.
2. Files should be added weekly.
3. Run Glue crawler when file is added to S3 bucket.
4. Import data into lifter_iceberg also.
5. This might be SNS/SQS.