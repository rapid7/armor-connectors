/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rapid7.presto.armor;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.shard.ShardId;

import javax.inject.Inject;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ArmorSplitManager
        implements ConnectorSplitManager
{
	private ArmorClient armorClient;
    @Inject
    public ArmorSplitManager(ArmorClient armorClient)
    {
        this.armorClient = requireNonNull(armorClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        ArmorTableLayoutHandle layoutHandle = (ArmorTableLayoutHandle) layout;
        ArmorTableHandle tableHandle = layoutHandle.getTable();
        
        String org = tableHandle.getSchema();
        String table = tableHandle.getTableName();
        AtomicReference<String> intervalAtomic = new AtomicReference<>();
        AtomicReference<String> timestampAtomic = new AtomicReference<>();
        layoutHandle.getTupleDomain().getColumnDomains().ifPresent(
            columnDomains -> {
              for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : columnDomains) {
                ArmorColumnHandle columnHandle = (ArmorColumnHandle) columnDomain.getColumn();
                if ("__interval".equals(columnHandle.getName())) {
                  intervalAtomic.set((String) columnDomain.getDomain().getNullableSingleValue());
                }
                if ("__timestamp".equals(columnHandle.getName())) {
                  timestampAtomic.set((String) columnDomain.getDomain().getNullableSingleValue());
                }
              }
            }
        );

        String interval = intervalAtomic.get();
        if (interval == null) {
          interval = Interval.SINGLE.getInterval();
        }
        String timestamp = timestampAtomic.get();
        if (timestamp == null) {
          timestamp = Instant.now().toString();
        }

        try {
	        List<ShardId> shards = armorClient.getShardIds(
	            org,
                table,
                Interval.toInterval(interval),
                Instant.parse(timestamp)
            );
	        List<ArmorSplit> splits = shards.stream()
	                .map(shard -> new ArmorSplit(shard.getShardNum()))
	                .collect(toImmutableList());
	
	        return new FixedSplitSource(splits);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }
}
