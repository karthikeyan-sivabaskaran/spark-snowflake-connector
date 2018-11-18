Using spark snowflake connector, this sample program will read/write the data from snowflake using snowflake-spark connector and also used Utils.runquery to directly run the commands in snowflake.

Dependencies used:

org.apache.spark:spark-core_2.11:2.3.0
org.apache.spark:spark-sql_2.11:2.3.0
org.apache.spark:spark-hive_2.11:2.3.0
net.snowflake:snowflake-jdbc:3.5.4
net.snowflake:spark-snowflake_2.11:2.3.2
org.apache.hadoop:hadoop-aws:2.7.1
org.apache.httpcomponents:httpclient:4.3.6
org.apache.httpcomponents:httpcore:4.3.3
com.fasterxml.jackson.core:jackson-annotations:2.9.6
com.amazonaws:aws-java-sdk:1.10.39