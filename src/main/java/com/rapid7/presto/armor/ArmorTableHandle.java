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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class ArmorTableHandle implements ConnectorTableHandle
{
    private final String schema;
    private final String tableName;
 
    @JsonCreator
    public ArmorTableHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("tableName") String tableName)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.tableName = tableName;
    }
    
    @JsonProperty
    public String getTableName() {
    	return tableName;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ArmorTableHandle other = (ArmorTableHandle) obj;
        return Objects.equals(this.getSchema(), other.getSchema()) &&
                Objects.equals(this.getTableName(), other.getTableName());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schema", getSchema())
                .add("tableName", getTableName())
                .toString();
    }
    
    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schema, tableName);
    }
}
