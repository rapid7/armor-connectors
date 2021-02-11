package com.rapid7.presto.armor;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.rapid7.armor.read.fast.FastArmorBlockReader;
import com.rapid7.armor.read.fast.FastArmorReader;
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

    public List<ColumnId> getColumnIds(String org, String tableName) throws IOException {
        return readStore.getColumnIds(org, tableName);
    }

    public Collection<String> getTables(String org) throws IOException {
        return readStore.getTables(org);
    }
    
    public List<String> getSchemas() {
        return readStore.getTenants();
    }

    public List<ShardId> getShardIds(String org, String tableName) throws IOException {
        return readStore.findShardIds(org, tableName);
    }

    public Map<String, FastArmorBlockReader> getFastReaders(int shardNum, String org, String table, List<ColumnHandle> columns) throws IOException {
        FastArmorReader armorReader = new FastArmorReader(readStore);
        HashMap<String, FastArmorBlockReader> readers = new HashMap<>();
        for (ColumnHandle column : columns) {
            ArmorColumnHandle armorHandle = (ArmorColumnHandle) column;
            readers.put(armorHandle.getName(), armorReader.getColumn(org, table, armorHandle.getName(), shardNum));
        }
        return readers;
    }
}
