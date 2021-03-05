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

4. Create an armor.properties file and class it in presto-main/etc/catalog/armor.properties

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

5. Add the path to presto-armor-connector's pom file to presto-main/etc/config.properties

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

6. Pass these VM arguments when running presto main

```
-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx16G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true
```

7. You will either need to delete etc/function-namespace/example.properties or start MySQL.

**8. In etc/log.properties, set com.facebook.presto to DEBUG otherwise you may just get see an error with no stacktrace.**

9. You are all set and ready to go, launch it from your IDE.

Note: If you are testing against a S3 store, then ensure your IDE's session has AWS access to whichever S3 bucket.

10. To interface with the server, use the CLI.

a) cd to presto/presto-cli/target
b) ./presto-cli-$VERSION-SNAPSHOT-executable.jar

You should see the armor catalog setup.

## Armor properties

In order to use the presto connector you'll need to define your armor settings. The armor settings you can need to set are

a) connector.name: The name of the connector. It can be any value like "armor".
b) armor.store.type: The type of store, current options are "file" and "s3"
c) armor.store.location: The root directory of the store. For s3 this would be the bucket.
d) armor.store.connections (optional): This only applies to s3 types, but this will define how many underlying s3 connections to use. Default is 50
e) armor.default-interval-stragety (optional): Setup a default interval stragety. Defaults to "none", in which case there is no hidden filtering by interval. If you wish have all queries filter by an interval like "single" then set this otherwise all queries' where clause would need to pass an interval where predicate.
f) armor.attempt-predicate-pushdown (optional): Attempt to execute predicate pushdowns, default is set to "true".

```
connector.name=armor
armor.store.type=s3
armor.store.location=armor-bucket
```

## Armor Session properties

You can set certain properties at runtime to change the behaivor of the armor queries. The two session properties you can set are.

1) attempt_predicate_pushdown: Enable/Disable predicate pushdown. Default is true.
2) default_interval_stragety: Set the interval stragety. Default is none.

## Deploying the connector to prestodb

NOTE: These instructions are based on deploying into prestodb using AWS EMR clusters. For the most part it should work in the same manner but there may be slight differences.

1. Compile and build the plugin. Ensure the version of Java is compatible with the version of Java that presto will be running.
2. Collect not only the jar but also all the libraries the connector has a dependency on. Most important to notice is to make sure the zstd library version matches the version of zstd that the presto cluster is using. This library (if it does have it) would have an api comptablity if different versions of zstd were introduced.
3. Build an armor properties file. See armor properties file section above for more details.
4. Deploy the property file to /etc/presto/conf.dist/catalog/ on the master presto node.
5. All the jars from step to deploy and place them in the /usr/lib/presto/plugin/armor/
6. Start the cluster.



