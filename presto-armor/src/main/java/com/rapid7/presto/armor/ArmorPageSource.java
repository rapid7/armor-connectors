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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;

public class ArmorPageSource implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(ArmorPageSource.class);
    private ArmorTableHandle table;
    private final List<ColumnHandle> columns;
    private long completedBytes;
    private long readTimeNanos;
    private long completedPositions;
    private ArmorBlockReader armorBlockReader;
    private boolean closed = false;
    private Executor executor = Executors.newFixedThreadPool(10);
    public ArmorPageSource(
            ArmorBlockReader armorBlockReader,
            ConnectorSession session,
            ArmorTableHandle table,
            List<ColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        this.armorBlockReader = armorBlockReader;
        this.columns = ImmutableList.copyOf(columns);
        this.table = table;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public Page getNextPage()
    {
        if (armorBlockReader.batchSize() <= 0) {
            close();
            return null;
        }
        CompletionService<Void> threadPool = new ExecutorCompletionService<>(executor);
        long mark = System.nanoTime();
        Block[] blocks = new Block[columns.size()];
        for (int i = 0; i < blocks.length; i++) {
            final ArmorColumnHandle column = (ArmorColumnHandle) columns.get(i);
            final int blockIndex = i;
            threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    blocks[blockIndex] = armorBlockReader.read(column.getType(), column.getName());
                    return null;
                }
            });
        }

        for (int i = 0; i < blocks.length; i++) {
            try {
                threadPool.take().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }    
        Page page = new Page(blocks);
        completedBytes += page.getSizeInBytes();
        completedPositions += page.getPositionCount();
        readTimeNanos = System.nanoTime() - mark;
        return page;
    }
}
