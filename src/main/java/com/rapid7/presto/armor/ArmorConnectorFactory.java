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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ArmorConnectorFactory
        implements ConnectorFactory
{
    ArmorConnectorFactory() {}

    @Override
    public String getName()
    {
        return "armor";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ArmorHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");

        try {
        	// Entry point for creating bindings, bootstrap is used to help manage the Guice stuff.
        	// Main initialization of instances will come from the custom module {@link ArmorConnectorModule}
            Bootstrap app = new Bootstrap(
                    new JsonModule(), // standard module copied from other connectors.
                    new ArmorConnectorModule(), // Armor module
                	// Add more bindings if you want access to more Presto specific classes.
                    binder -> {
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());  // access to the presto datatypes if we need it.
                    });

            // Copied over seems required.
            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(ArmorConnector.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
