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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Marker.Bound;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
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

import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
        AtomicReference<StringPredicate> intervalAtomic = new AtomicReference<>();
        AtomicReference<InstantPredicate> intervalStartAtomic = new AtomicReference<>();
        layoutHandle.getTupleDomain().getColumnDomains().ifPresent(
            columnDomains -> {
              for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : columnDomains) {
                ArmorColumnHandle columnHandle = (ArmorColumnHandle) columnDomain.getColumn();
                if (ArmorConstants.INTERVAL.equals(columnHandle.getName())) {
                  Domain domain = columnDomain.getDomain();
                  StringPredicate stringPredicate = intervalPredicate(domain);
                  intervalAtomic.set(stringPredicate);;
                }
                if (ArmorConstants.INTERVAL_START.equals(columnHandle.getName())) {
                  Domain domain = columnDomain.getDomain();
                  InstantPredicate instantPredicate = startIntervalPredicate(domain);
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
             shards = armorClient.getShardIds(org, table, Interval.SINGLE, intervalStart);
         }
	        List<ArmorSplit> splits = shards.stream()
	           .map(shard -> new ArmorSplit(shard.getShardNum()))
	           .collect(toImmutableList());
	        return new FixedSplitSource(splits);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }
    
    private String valueToString(Object value) {
        if (value != null) {
            if (value instanceof Slice)
              return ((Slice)value).toStringUtf8();
            else if (value instanceof String)
              return (String) value;
        }
        return null;
    }
    
    private StringPredicate intervalPredicate(Domain predicate) {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.size() > 1) {
                // In clause style predicate
                List<String> filters = new ArrayList<>();
                for (Range range : ranges) {
                   String interval = valueToString(range.getSingleValue());
                   filters.add(interval);
                }
                return new StringPredicate(ArmorConstants.INTERVAL, Operator.IN, filters);
            } else {
                // Greater, less, between etc. NOTE: Equals not supported in presto on timestmp.
                Range range = ranges.iterator().next();                
                Operator operator = null;
                Marker highMarker = range.getHigh();
                Marker lowMarker = range.getLow();
                // For < and <=
                if (highMarker.getValueBlock().isPresent() && !lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE )
                        operator = Operator.LESS_THAN;
                    else if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.LESS_THAN_EQUAL;
                    String interval = valueToString(highMarker.getValue());
                    return new StringPredicate(ArmorConstants.INTERVAL, operator, interval);
                }
                // For > and >=
                if (lowMarker.getValueBlock().isPresent() && !highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.EXACTLY )
                        operator = Operator.GREATER_THAN_EQUAL;
                    else if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.GREATER_THAN;
                    String interval = valueToString(lowMarker.getValue());
                    return new StringPredicate(ArmorConstants.INTERVAL, operator, interval);
                }
                // For = or between
                if (highMarker.getValueBlock().isPresent() && highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.EXACTLY) {
                       // Between and equal are the same only way to determine the difference is if the values are different.
                       String highValue = valueToString(highMarker.getValue());
                       String lowValue = valueToString(lowMarker.getValue());
                       if (highValue.equals(lowValue)) {
                         return new StringPredicate(ArmorConstants.INTERVAL, Operator.EQUALS, highValue);
                       } else {
                         return new StringPredicate(
                             ArmorConstants.INTERVAL,
                             Operator.BETWEEN,
                             Arrays.asList(highValue, lowValue));
                       }
                    }
                }
            }
        }
        throw new RuntimeException("Unable to resolve predicate into armor predicate");
    }
    
    
    private InstantPredicate startIntervalPredicate(Domain predicate) {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.size() > 1) {
                // In clause style predicate
                List<Instant> filters = new ArrayList<>();
                for (Range range : ranges) {
                   Long time = (Long) range.getSingleValue();
                   filters.add(Instant.ofEpochMilli(time));
                }
                return new InstantPredicate(ArmorConstants.INTERVAL_START, Operator.IN, filters);
            } else {
                // Greater, less, between etc. NOTE: Equals not supported in presto on timestmp.
                Range range = ranges.iterator().next();                
                Operator operator = null;
                Marker highMarker = range.getHigh();
                Marker lowMarker = range.getLow();
                // For < and <=
                if (highMarker.getValueBlock().isPresent() && !lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE )
                        operator = Operator.LESS_THAN;
                    else if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.LESS_THAN_EQUAL;
                    long time = (Long) highMarker.getValue();
                    return new InstantPredicate(ArmorConstants.INTERVAL_START, operator, Arrays.asList(Instant.ofEpochMilli(time)));
                }
                // For > and >=
                if (lowMarker.getValueBlock().isPresent() && !highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.EXACTLY )
                        operator = Operator.GREATER_THAN_EQUAL;
                    else if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.GREATER_THAN;
                    long time = (Long) lowMarker.getValue();
                    return new InstantPredicate(ArmorConstants.INTERVAL_START, operator, Arrays.asList(Instant.ofEpochMilli(time)));
                }
                // For = or between
                if (highMarker.getValueBlock().isPresent() && highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.EXACTLY) {
                       // Between and equal are the same only way to determine the difference is if the values are different.
                       long highValue = (Long) highMarker.getValue();
                       long lowValue = (Long) lowMarker.getValue();
                       if (highValue == lowValue) {
                         return new InstantPredicate(ArmorConstants.INTERVAL_START, Operator.EQUALS, Arrays.asList(Instant.ofEpochMilli(highValue)));
                       } else {
                         return new InstantPredicate(
                             ArmorConstants.INTERVAL_START,
                             Operator.BETWEEN,
                             Arrays.asList(Instant.ofEpochMilli(lowValue), Instant.ofEpochMilli(highValue)));
                       }
                    }
                }
            }
        }
        throw new RuntimeException("Unable to resolve predicate into armor predicate");
    }
}
