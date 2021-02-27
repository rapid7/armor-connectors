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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.Operator;

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
        AtomicReference<StringPredicate> intervalAtomic = new AtomicReference<>();
        AtomicReference<InstantPredicate> intervalStartAtomic = new AtomicReference<>();
        layoutHandle.getTupleDomain().getColumnDomains().ifPresent(
            columnDomains -> {
              for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : columnDomains) {
                ArmorColumnHandle columnHandle = (ArmorColumnHandle) columnDomain.getColumn();
                if (ArmorConstants.INTERVAL.equals(columnHandle.getName())) {
                  Domain domain = columnDomain.getDomain();
                  StringPredicate stringPredicate = ArmorDomainUtil.intervalPredicate(domain);
                  intervalAtomic.set(stringPredicate);;
                }
                if (ArmorConstants.INTERVAL_START.equals(columnHandle.getName())) {
                  Domain domain = columnDomain.getDomain();
                  InstantPredicate instantPredicate = ArmorDomainUtil.startIntervalPredicate(domain);
                  intervalStartAtomic.set(instantPredicate);;
                }
            }
          }
        );

        StringPredicate interval = intervalAtomic.get();
        InstantPredicate intervalStart = intervalStartAtomic.get();
        try {
         List<ShardId> shards = null;
         if (interval != null) {
             if (interval.getOperator() == Operator.EQUALS) {
                 if (intervalStart == null)
                     shards = armorClient.getShardIds(org, table, Interval.toInterval(interval.getValue()));
                 else
                     shards = armorClient.getShardIds(org, table, Interval.toInterval(interval.getValue()), intervalStart);
             } else {
                 shards = armorClient.getShardIds(org, table, interval, intervalStart);
             }
         } else {
             shards = armorClient.getShardIds(org, table, (Interval) null, intervalStart);
         }
	        List<ArmorSplit> splits = shards.stream()
	           .map(shard -> new ArmorSplit(shard.getShardNum(), shard.getInterval(), shard.getIntervalStart()))
	           .collect(toImmutableList());
	        return new FixedSplitSource(splits);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }
}
