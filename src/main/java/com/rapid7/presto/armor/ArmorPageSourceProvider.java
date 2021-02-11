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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.rapid7.armor.read.fast.FastArmorBlockReader;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ArmorPageSourceProvider
        implements ConnectorPageSourceProvider
{
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
        String org = layoutHandle.getTable().getSchema();
        ArmorSplit armorSplit = (ArmorSplit) split;
 
        try {
	        Map<String, FastArmorBlockReader> readers = armorClient.getFastReaders(armorSplit.getShard(), org, layoutHandle.getTable().getTableName(), columns);        
	        return new ArmorPageSource(new ArmorBlockReader(readers), session, layoutHandle.getTable(), columns);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }
}
