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
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.rapid7.armor.schema.ColumnId;

public class ArmorMetadata
        implements ConnectorMetadata
{
    private ArmorClient armorClient;
   
    @Inject
    public ArmorMetadata(TypeManager typeManager, ArmorClient armorClient)
    {
        this.armorClient = requireNonNull(armorClient, "armorClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return armorClient.getSchemas();
    }

    @Override
    public ArmorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        return new ArmorTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
        ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ArmorTableHandle handle = (ArmorTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ArmorTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
    	   // Grab convert the table and build 
        ArmorTableHandle handle = (ArmorTableHandle) table;
        try {
        	String org = handle.getSchema();
	        List<ColumnMetadata> columns = armorClient.getColumnIds(org, handle.getTableName()).stream()
	            .map(column -> toColumnMetaData(column))
	            .collect(toImmutableList());
	        return new ConnectorTableMetadata(handle.toSchemaTableName(), columns);
        } catch (Exception ioe) {
        	ioe.printStackTrace();
        	throw new RuntimeException(ioe);
        }
    }
    
    private ColumnMetadata toColumnMetaData(ColumnId columnId) {
    	return new ColumnMetadata(columnId.getName(), toColumnType(columnId));
    }

    private Type toColumnType(ColumnId column) {
    	switch (column.dataType()) {
    		case LONG:
    			return BigintType.BIGINT;
    		case DOUBLE:
    			return DoubleType.DOUBLE;
    		case BOOLEAN:
    			return BooleanType.BOOLEAN;
    		case FLOAT:
    			return RealType.REAL;
    		case INTEGER:
    			return IntegerType.INTEGER;
    		case STRING:
    			return VarcharType.VARCHAR;
    		default:
    			break;
    		}
    	return null;
    }
    
    private ColumnHandle toColumnHandle(ColumnId column)
    {
        return new ArmorColumnHandle(column.getName(), toColumnType(column));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
    	try {
	    	String org = schemaName.get();
	    	return armorClient.getTables(org).stream().map(tableName -> new SchemaTableName(org, tableName)).collect(toImmutableList());
    	} catch (IOException e) {
    		throw new RuntimeException(e);
    	}
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArmorTableHandle armorTable = (ArmorTableHandle) tableHandle;
        String tableName = armorTable.getTableName();
        String org = armorTable.getSchema();
        try {
        	return armorClient.getColumnIds(org, tableName).stream().collect(toImmutableMap(ColumnId::getName, column -> toColumnHandle(column)));
        } catch (Exception ioe) {
        	ioe.printStackTrace();
        }
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ArmorColumnHandle handle = (ArmorColumnHandle) columnHandle;
        return new ColumnMetadata(handle.getName(), handle.getType());
    }
    
    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
    	requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, fromSchemaTableName(tableName));
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }
    
    public static ArmorTableHandle fromSchemaTableName(SchemaTableName schemaTableName)
    {
        return new ArmorTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}
