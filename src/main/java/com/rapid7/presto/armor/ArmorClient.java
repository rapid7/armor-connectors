package com.rapid7.presto.armor;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;

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
      cc.setMaxConnections(config.getStoreConnections());
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
      return columns.stream().collect(Collectors.toList());
    columns = Collections.unmodifiableList(readStore.getColumnIds(org, tableName));
    if (!columns.isEmpty())
      columnIdCache.put(key, columns);
    return columns.stream().collect(Collectors.toList());
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

  public List<ShardId> getShardIds(String org, String tableName, Interval interval, InstantPredicate intervalStartPredicate) {
    return readStore.findShardIds(org, tableName, interval, intervalStartPredicate);
  }

  public List<ShardId> getShardIds(String org, String tableName, StringPredicate interval, InstantPredicate intervalStart) {
    return readStore.findShardIds(org, tableName, interval, intervalStart);
  }

  public long count(ShardId shardId) {
    ShardMetadata metadata = readStore.getShardMetadata(shardId);
    if (metadata == null)
      return 0l;
    return metadata.getColumnMetadata().get(0).getNumRows();
  }

  public Map<String, FastArmorBlockReader> getFastReaders(ShardId shardId, List<ColumnHandle> columns) throws IOException {
    FastArmorReader armorReader = new FastArmorReader(readStore);
    HashMap<String, FastArmorBlockReader> readers = new HashMap<>();
    for (ColumnHandle column : columns) {
      ArmorColumnHandle armorHandle = (ArmorColumnHandle) column;
      if (ArmorConstants.INTERVAL.equals(armorHandle.getName())) {
        readers.put(armorHandle.getName(), armorReader.getFixedValueColumn(shardId, shardId.getInterval()));
      } else if (ArmorConstants.INTERVAL_START.equals(armorHandle.getName())) {
        Instant valueIntervalStart = Instant.parse(shardId.getIntervalStart());
        readers.put(armorHandle.getName(), armorReader.getFixedValueColumn(shardId, packDateTimeWithZone(valueIntervalStart.toEpochMilli(), "UTC")));
      } else {
        readers.put(armorHandle.getName(), armorReader.getColumn(shardId, armorHandle.getName()));
      }
    }
    return readers;
  }
}
