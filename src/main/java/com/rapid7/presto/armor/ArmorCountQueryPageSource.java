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


import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.shard.ShardId;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import java.time.Instant;

public class ArmorCountQueryPageSource
        implements ConnectorPageSource
{
    private final long readTimeNanos;
    private long count = 0;

    public ArmorCountQueryPageSource(ArmorClient client, ConnectorSession session, ArmorTableLayoutHandle table, ArmorSplit split)
    {
        requireNonNull(client, "client is null");
        requireNonNull(session, "session is null");
        requireNonNull(table, "table is null");
        requireNonNull(split, "split is null");

        long start = System.nanoTime();
        String tenant = table.getTable().getSchema();
        String tableName = table.getTable().getTableName();
        ShardId shardId = ShardId.buildShardId(tenant, tableName, Interval.toInterval(split.getInterval()), Instant.parse(split.getIntervalStart()), split.getShard());
        count = client.count(shardId);
        readTimeNanos = System.nanoTime() - start;
    }

    @Override
    public boolean isFinished()
    {
        return true;
    }

    @Override
    public Page getNextPage()
    {
        return new Page((int) count);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return count;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
    }
}
