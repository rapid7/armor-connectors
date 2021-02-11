# presto-armor-connector

## Developing with the connector

You can develop and debug your presto connector locally. Follow these instructions below to debug.

1. Clone the prestodb repository.

https://github.com/prestodb/presto

2. Build the prestodb project

```
./mvnw clean install -DskipTests
```

3. Load modules from prestodb into your IDE.

These are required in your IDE to get a good idea of how it works.
 * presto-main
 * presto-common
 
This repository which is NOT part of prestoDB.
 * presto-armor-connector
 
3. Find and identify PrestoServer, this will be your main class to run.

4. To ensure armor is setup first create an armor.properties file and class it in presto-main/etc/catalog/armor.properties

```
#
# WARNING
# ^^^^^^^
# This configuration file is for development only and should NOT be used be
# used in production. For example configuration, see the Presto documentation.
#

connector.name=armor
armor.store.type=s3
armor.store.location=mybucket
#armor.store.type=file
#armor.store.location=/home/alee/nike/armor
```

5. Modify the presto-main/etc/config.properties

```
#
# WARNING
# ^^^^^^^
# This configuration file is for development only and should NOT be used be
# used in production. For example configuration, see the Presto documentation.
#

# sample nodeId to provide consistency across test runs
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.environment=test
http-server.http.port=8080

discovery-server.enabled=true
discovery.uri=http://localhost:8080

exchange.http-client.max-connections=1000
exchange.http-client.max-connections-per-server=1000
exchange.http-client.connect-timeout=1m
exchange.http-client.idle-timeout=1m

scheduler.http-client.max-connections=1000
scheduler.http-client.max-connections-per-server=1000
scheduler.http-client.connect-timeout=1m
scheduler.http-client.idle-timeout=1m

query.client.timeout=5m
query.min-expire-age=30m

plugin.bundles=\
  ../presto-blackhole/pom.xml,\
  ../presto-memory/pom.xml,\
  ../presto-jmx/pom.xml,\
  ../presto-raptor/pom.xml,\
  ../presto-hive-hadoop2/pom.xml,\
  ../presto-example-http/pom.xml,\
  ../presto-kafka/pom.xml, \
  ../presto-tpch/pom.xml, \
  ../presto-local-file/pom.xml, \
  ../presto-mysql/pom.xml,\
  ../presto-sqlserver/pom.xml, \
  ../presto-postgresql/pom.xml, \
  ../presto-tpcds/pom.xml, \
  ../presto-pinot/pom.xml, \
  ../presto-i18n-functions/pom.xml,\
  ../presto-function-namespace-managers/pom.xml,\
  ../presto-druid/pom.xml, \
  ../../mydirectory/presto-armor-connector/pom.xml

presto.version=testversion
node-scheduler.include-coordinator=true

```

6. Pass these VM argumnets into presto main

```
-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx16G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true
```

7. You are all set and ready to go, launch it from your IDE.

Note: If you are testing against a S3 store, then ensure your IDE's session has AWS access to whichever S3 bucket.

8. To interface with the server, use the CLI.

a) cd to presto/presto-cli/target
b) ./presto-cli-$VERSION-SNAPSHOT-executable.jar

You should see the armor catalog setup. Have fun!

