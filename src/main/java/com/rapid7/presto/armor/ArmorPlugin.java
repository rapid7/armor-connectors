package com.rapid7.presto.armor;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;

public class ArmorPlugin implements Plugin
{
    private final ConnectorFactory connectorFactory;

    public ArmorPlugin()
    {
        connectorFactory = new ArmorConnectorFactory();
    }

    @VisibleForTesting
    ArmorPlugin(ArmorConnectorFactory factory)
    {
        connectorFactory = requireNonNull(factory, "factory is null");
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(connectorFactory);
    }
}
