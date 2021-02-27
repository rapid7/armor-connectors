package com.rapid7.presto.armor;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.read.fast.FastArmorBlockReader;
import com.rapid7.armor.read.fast.FastArmorReader;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.FileReadStore;
import com.rapid7.armor.store.ReadStore;
import com.rapid7.armor.store.S3ReadStore;

public class ArmorClient {
  private ReadStore readStore;
  private Cache<String, List<ColumnId>> columnIdCache = CacheBuilder.newBuilder()
     .maximumSize(2000)
     .expireAfterWrite(10, TimeUnit.MINUTES).build();

  @Inject
  public ArmorClient(ArmorConfig config) {
    if (config.getStoreType().equals("file")) {
      readStore = new FileReadStore(Paths.get(config.getStoreLocation()));
    } else if (config.getStoreType().equals("s3")) {
      ClientConfiguration cc = new ClientConfiguration();
      cc.setMaxConnections(50);
      cc.withMaxErrorRetry(5);
      cc.withTcpKeepAlive(true);

      AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
      builder.setClientConfiguration(cc);
      if (Regions.getCurrentRegion() != null) {
        builder.setRegion(Regions.getCurrentRegion().getName());
      } else
        builder.setRegion(Regions.US_EAST_1.getName());
      readStore = new S3ReadStore(builder.build(), config.getStoreLocation());
    } else
      throw new RuntimeException("The store type " + config.getStoreType() + " is not supported yet");
  }

  public List<ColumnId> getColumnIds(String org, String tableName) {
    String key = org + "_" + tableName;
    List<ColumnId> columns = columnIdCache.getIfPresent(key);
    if (columns != null)
      return columns;
    columns = readStore.getColumnIds(org, tableName);
    if (columns.isEmpty())
      return columns;
    else {
      columnIdCache.put(key, columns);
      return columns;
    }
  }

  public Collection<String> getTables(String org) {
    return readStore.getTables(org);
  }

  public List<String> getSchemas() {
    return readStore.getTenants();
  }

  public List<ShardId> getShardIds(String org, String tableName, Interval interval) throws IOException {
    return readStore.findShardIds(org, tableName, interval);
  }

  public List<ShardId> getShardIds(String org, String tableName, Interval interval, InstantPredicate intervalStart) throws IOException {
    return readStore.findShardIds(org, tableName, interval, intervalStart);
  }

  public List<ShardId> getShardIds(String org, String tableName, StringPredicate interval, InstantPredicate intervalStart) throws IOException {
    return readStore.findShardIds(org, tableName, interval, intervalStart);
  }

  public long count(int shardNum, String tenant, String table, Interval interval, Instant timestamp) {
    ShardMetadata metadata = readStore.getShardMetadata(tenant, table, interval, timestamp, shardNum);
    if (metadata == null)
      return 0l;
    return metadata.getColumnMetadata().get(0).getNumRows();
  }

  public Map<String, FastArmorBlockReader> getFastReaders(int shardNum, String tenant, String table, Interval interval, Instant timestamp, List<ColumnHandle> columns) throws IOException {
    FastArmorReader armorReader = new FastArmorReader(readStore);
    HashMap<String, FastArmorBlockReader> readers = new HashMap<>();
    String valueInterval = interval.name();
    for (ColumnHandle column : columns) {
      ArmorColumnHandle armorHandle = (ArmorColumnHandle) column;
      if (ArmorConstants.INTERVAL.equals(armorHandle.getName())) {
        readers.put(armorHandle.getName(), armorReader.getFixedValueColumn(tenant, table, interval, timestamp, shardNum, valueInterval));
      } else if (ArmorConstants.INTERVAL_START.equals(armorHandle.getName())) {
        Instant valueIntervalStart = Instant.parse(interval.getIntervalStart(timestamp));
        readers.put(armorHandle.getName(), armorReader.getFixedValueColumn(tenant, table, interval, timestamp, shardNum, valueIntervalStart.toEpochMilli()));
      } else {
        readers.put(armorHandle.getName(), armorReader.getColumn(tenant, table, interval, timestamp, armorHandle.getName(), shardNum));
      }
    }
    return readers;
  }
}
