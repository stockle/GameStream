# TODO
## Spark
 - [x] Create PySpark script
 - [x] Connector from PySpark to Cassandra
 - [x] Execute first SparkSQL statement to join `gameplay_events` with `purchase_events`
 - [ ] Debug `Exception: Java gateway process exited before sending its port number`
 - [ ] Put spark on it's own ec2
 - [ ] point flask app master job to spark and retrieve query (if possible)

## Kafka
 - [x] Create two more nodes
 - [x] Run the Consumer scripts on each of those nodes
  - Two each for `gameplay_events` and `purchase_events`

## Website
 - [x] Deploy Flask to Ec2
 - [x] Point Ec2 to DNS

## Data
 - [x] Bucket ages into age brackets instead of actual age
 - [x] Determine a data model that supports age brackets in the database and when presenting the data
  - Age should be semi-anonimized before it gets to the database

## Repo
 - [x] Break out scripts into their respective folders
 - [ ] Finish README in every top-level directory
