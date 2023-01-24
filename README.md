# bulker
bulker sends SMS to list of abonents

To create database schema use sql/database_schema.sql<br />
To add minimal test data to tables use sql/database_testdata.sql</br>

To create rabbitmq environment, do:</br>
```
rabbitmqctl add_user bulker bulker
rabbitmqctl add_vhost bulker
rabbitmqctl set_permissions -p bulker bulker ".*" ".*" ".*"
```
then build cmd/rmqinit into bin/rmqinit<br />
and then run 
```
bin/rmqinit ./config/config.yaml
```
it will create exchange and all needed queues with bindings

To run bulker do:<br />
build bulker into bin/bulker<br />
run 
```
bin/bulker config/config.yaml
```
check log file...<br />
check tables bulk, bulk_stat and bulk_msisdn records

