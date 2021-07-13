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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.DictionaryReader;
import com.rapid7.armor.read.fast.FastArmorBlockReader;
import com.rapid7.armor.read.predicate.ColumnMetadataPredicateUtils;
import com.rapid7.armor.read.predicate.NumericPredicate;
import com.rapid7.armor.read.predicate.Predicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.Operator;

import javax.inject.Inject;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArmorPageSourceProvider implements ConnectorPageSourceProvider
{
    private static final Logger LOG = Logger.get(ArmorPageSourceProvider.class);

    private ArmorClient armorClient;

    @Inject
    public ArmorPageSourceProvider(ArmorClient armorClient)
    {
        this.armorClient = armorClient;
    }

    @Override
    public ConnectorPageSource createPageSource(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorSplit split,
        ConnectorTableLayoutHandle layout,
        List<ColumnHandle> columns,
        SplitContext splitContext)
    {
        requireNonNull(split, "split is null");
        requireNonNull(layout, "layout is null");
        ArmorTableLayoutHandle layoutHandle = (ArmorTableLayoutHandle) layout;
        String tenant = layoutHandle.getTable().getSchema();
        ArmorSplit armorSplit = (ArmorSplit) split;

        if (columns.isEmpty()) {
            return new ArmorCountQueryPageSource(armorClient, session, layoutHandle, armorSplit);
        }

        String table = layoutHandle.getTable().getTableName();
        try {
            ShardId shardId = ShardId.buildShardId(
                tenant,
                table,
                Interval.toInterval(armorSplit.getInterval()),
                Instant.parse(armorSplit.getIntervalStart()),
                armorSplit.getShard());
            // Determine if there are any predicates we can use. This will allow to have the fast readers load the entity and value dictionary.
            // NOTE: Pushdowns are only provided if its AND or single predicate situations. ORs returns ALL
            Map<String, Predicate<?>> pushDownPredicates = buildPushdownPredicates(layoutHandle);

            Map<String, FastArmorBlockReader> readers = armorClient.getFastReaders(shardId, columns);
            if (pushDownPredicates != null && !pushDownPredicates.isEmpty()) {
                boolean allPredicates = true;
                for (Map.Entry<String, Predicate<?>> entries : pushDownPredicates.entrySet()) {
                    String name = entries.getKey();
                    Predicate<?> predicate = entries.getValue();
                    FastArmorBlockReader reader = readers.get(name);
                    if (predicate instanceof StringPredicate) {
                        DictionaryReader dictionary = reader.valueDictionary();
                        Boolean predicateResult = dictionary.evaulatePredicate((StringPredicate) predicate);
                        if (predicateResult == null) {
                            // This can't be evaluated by the string column thus invalidate pushdown predicates attempt.
                            allPredicates = true; // If previous predicate was set to true, then simplate
                            break;
                        }
                        if (!predicateResult) {
                            allPredicates = false;
                            break;
                        }
                    } else if (predicate instanceof NumericPredicate) {
                        NumericPredicate<? extends Number> numPredicate = (NumericPredicate<?>) predicate;
                        double testValue = numPredicate.getValue().doubleValue();
                        ColumnMetadata metadata = reader.metadata();
                        if (metadata.getMaxValue() == null || metadata.getMinValue() == null) {
                            LOG.warn("Detected no metadata min/max value for numeric. This wasn't expected for column " + name);
                            allPredicates = true;
                            break;
                        }
                        if (predicate.getOperator() == Operator.EQUALS) {
                            if (!ColumnMetadataPredicateUtils.columnMayContain(testValue, metadata.getMinValue(), metadata.getMaxValue())) {
                                allPredicates = false;
                                break;
                            }    
                        } else if (predicate.getOperator() == Operator.NOT_EQUALS) {
                            if (ColumnMetadataPredicateUtils.columnMayContain(testValue, metadata.getMinValue(), metadata.getMaxValue())) {
                                allPredicates = false;
                                break;
                            }    
                        } else if (predicate.getOperator() == Operator.GREATER_THAN || predicate.getOperator() == Operator.GREATER_THAN_EQUAL) {
                            if (!ColumnMetadataPredicateUtils.columnMayHaveValueGreaterThan(testValue, metadata.getMaxValue())) {
                                allPredicates = false;
                                break;
                            }    
                        } else if (predicate.getOperator() == Operator.LESS_THAN || predicate.getOperator() == Operator.LESS_THAN_EQUAL) {
                            if (!ColumnMetadataPredicateUtils.columnMayHaveValueLessThan(testValue, metadata.getMaxValue())) {
                                allPredicates = false;
                                break;
                            }
                        } else if (predicate.getOperator() == Operator.IN) {
                            boolean noneMatch = true;
                            for (Number num : numPredicate.getValues()) {
                               if (ColumnMetadataPredicateUtils.columnMayContain(num.doubleValue(), metadata.getMinValue(), metadata.getMaxValue())) {
                                   noneMatch = false;
                                   break;
                               }
                            }
                            if (noneMatch) {
                              allPredicates = false;
                              break;
                            }
                        } else if (predicate.getOperator() == Operator.BETWEEN) {
                           double testMin = numPredicate.getValues().get(0).doubleValue();
                           double testMax = numPredicate.getValues().get(1).doubleValue();
                           if (!ColumnMetadataPredicateUtils.columnMayHaveValueLessThan(testMin, metadata.getMaxValue()) &&
                               !ColumnMetadataPredicateUtils.columnMayHaveValueGreaterThan(testMax, metadata.getMinValue())) {
                               allPredicates = false;
                               break;
                           }
                        }
                    }
                }
                if (!allPredicates)
                    return emptyConnectorPageSource();
            }
            return new ArmorPageSource(new ArmorBlockReader(readers), session, columns);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Predicate<?>> buildPushdownPredicates(ArmorTableLayoutHandle tableHandle) {
        Optional<List<ColumnDomain<ColumnHandle>>> option = tableHandle.getTupleDomain().getColumnDomains();
        if (!option.isPresent())
            return null;
        Map<String, Predicate<?>> predicates = new HashMap<>();
        for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : option.get()) {
            ArmorColumnHandle columnHandle = (ArmorColumnHandle) columnDomain.getColumn();
            String name = columnHandle.getName();
            // Skip these special columns, they should have been pushed down earlier.
            if (name.equals(ArmorConstants.INTERVAL) || name.equals(ArmorConstants.INTERVAL_START))
                continue;

            Type type = columnHandle.getType();
            if (type instanceof VarcharType) {
                StringPredicate stringPred = ArmorDomainUtil.columnStringPredicate(name, columnDomain.getDomain());
                predicates.put(name, stringPred);
            } else if (type instanceof BigintType || type instanceof IntegerType) {
                NumericPredicate<? extends Number> numPredicate = ArmorDomainUtil.columnNumericPredicate(name, columnDomain.getDomain());
                predicates.put(name, numPredicate);
            }
        }
        return predicates;
    }

    private ConnectorPageSource emptyConnectorPageSource()
    {
        return new ConnectorPageSource() {
            @Override
            public long getCompletedBytes()
            {
                return 0;
            }
            @Override
            public long getCompletedPositions()
            {
                return 0;
            }
            @Override
            public long getReadTimeNanos()
            {
                return 0;
            }
            @Override
            public boolean isFinished()
            {
                return true;
            }
            @Override
            public Page getNextPage()
            {
                return null;
            }
            @Override
            public long getSystemMemoryUsage()
            {
                return 0;
            }
            @Override
            public void close() {}
        };
    }
}
