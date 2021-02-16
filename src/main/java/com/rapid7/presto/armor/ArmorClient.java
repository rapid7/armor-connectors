package com.rapid7.presto.armor;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.read.fast.FastArmorBlockReader;
import com.rapid7.armor.read.fast.FastArmorReader;
import com.rapid7.armor.read.fast.NullArmorBlockReader;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.FileReadStore;
import com.rapid7.armor.store.ReadStore;
import com.rapid7.armor.store.S3ReadStore;

public class ArmorClient {
    private ReadStore readStore;

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

    public List<ColumnId> getColumnIds(String org, String tableName, Interval interval, Instant timestamp) throws IOException {
        return readStore.getColumnIds(org, tableName, interval, timestamp);
    }

    public Collection<String> getTables(String org) throws IOException {
        return readStore.getTables(org);
    }

    public List<String> getSchemas() {
        return readStore.getTenants();
    }

    public List<ShardId> getShardIds(String org, String tableName, Interval interval, Instant timestamp) throws IOException {
        return readStore.findShardIds(org, tableName, interval, timestamp);
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
        String valueIntervalStart = interval.getIntervalStart(timestamp);
        for (ColumnHandle column : columns) {
            ArmorColumnHandle armorHandle = (ArmorColumnHandle) column;
            if ("__interval".equals(armorHandle.getName())) {
                readers.put(armorHandle.getName(), armorReader.getFixedValueColumn(tenant, table, interval, timestamp, shardNum, valueInterval));
            } else if ("__intervalStart".equals(armorHandle.getName())) {
                readers.put(armorHandle.getName(), armorReader.getFixedValueColumn(tenant, table, interval, timestamp, shardNum, valueIntervalStart));
            } else {
                readers.put(armorHandle.getName(), armorReader.getColumn(tenant, table, interval, timestamp, armorHandle.getName(), shardNum));
            }
        }
        return readers;
    }
}
