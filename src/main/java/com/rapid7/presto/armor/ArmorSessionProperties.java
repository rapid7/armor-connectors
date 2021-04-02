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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

public class ArmorSessionProperties
{
    private static final String ATTEMPT_PUSHDOWN = "attempt_predicate_pushdown";
    private static final String DEFAULT_INTERVAL_STRAGETY = "default_interval_stragety";
    private static final String LIST_TENANT_CACHED = "list_tenant_cached";
    
    private final List<PropertyMetadata<?>> sessionProperties;

    public static boolean isAttemptPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(ATTEMPT_PUSHDOWN, Boolean.class);
    }
    
    public static String defaultIntervalStragety(ConnectorSession session)
    {
        return session.getProperty(DEFAULT_INTERVAL_STRAGETY, String.class);
    }
    
    public static boolean isListTenantsCachedEnabled(ConnectorSession session)
    {
        return session.getProperty(LIST_TENANT_CACHED, Boolean.class);
    }

    @Inject
    public ArmorSessionProperties(ArmorConfig armorConfig)
    {
        sessionProperties = ImmutableList.of(
            booleanProperty(
                 ATTEMPT_PUSHDOWN,
                 "Enable predicate pushdown if possible",
                 armorConfig.isAttemptPushdownEnabled(),
                 false),
            stringProperty(
                 DEFAULT_INTERVAL_STRAGETY,
                 "Set the default interval stragety",
                 armorConfig.getDefaultIntervalStragety(),
                 false),
            booleanProperty(
                 LIST_TENANT_CACHED,
                 "Use cache to list all tenants",
                 true,
                 false)
            );
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
