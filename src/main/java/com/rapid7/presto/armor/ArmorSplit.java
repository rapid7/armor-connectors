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

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.List;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ArmorSplit
        implements ConnectorSplit
{
    private final int shard;
    private final String interval;
    private final String intervalStart;
 
    @JsonCreator
    public ArmorSplit(@JsonProperty("shard") int shard, @JsonProperty("interval") String interval, @JsonProperty("intervalStart") String intervalStart)
    {
        this.shard = shard;
        this.interval = interval;
        this.intervalStart = intervalStart;
    }

    @JsonProperty
    public int getShard()
    {
        return shard;
    }
    
    @JsonProperty
    public String getInterval() {
        return interval;
    }
    
    @JsonProperty
    public String getIntervalStart() {
        return intervalStart;
    }

    
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(shard)
                .addValue(interval)
                .addValue(intervalStart)
                .toString();
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates) {
        return null;
    }
}
