# bulker
bulker sends SMS to list of abonents

To create database schema use sql/database_schema.sql
Add minimal test data to tables via sql/database_testdata.sql

To create rabbitmq environment, do:
rabbitmqctl add_user bulker bulker
rabbitmqctl add_vhost bulker
rabbitmqctl set_permissions -p bulker bulker ".*" ".*" ".*"

build cmd/rmqinit into bin/rmqinit
run bin/rmqinit ./config/config.yaml

build bulker into bin/bulker
run bin/bulker config/config.yaml
check log file...
check tables bulk, bulk_stat and bulk_msisdn records
