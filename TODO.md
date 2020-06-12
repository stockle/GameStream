# TODO
## Spark
 - [ ] Create PySpark script
  - [ ] Connector from PySpark to Cassandra
    - [ ] Execute first SparkSQL statement to join `gameplay_events` with `purchase_events`

## Kafka
 - [ ] Create two more nodes
 - [ ] Run the Consumer scripts on each of those nodes
  - Two each for `gameplay_events` and `purchase_events`

## Data
 - [ ] Bucket ages into age brackets instead of actual age
  - [ ] Determine a data model that supports age brackets in the database and when presenting the data
   - Age should be semi-anonimized before it gets to the database

## Repo
 - [ ] Break out scripts into their respective folders
