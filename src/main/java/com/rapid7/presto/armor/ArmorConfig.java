package com.rapid7.presto.armor;

import javax.validation.constraints.NotNull;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class ArmorConfig {
    private String storeType;
    private String storeLocation;
    private int storeConnections = 50;
    private String defaultIntervalStragety = "none";
    private boolean pushdown;
    
    public int getStoreConnections() {
        return storeConnections;
    }
    
    @Config("armor.store.connections")
    @ConfigDescription("The number of connections to the store (if possible)")
    public ArmorConfig setStoreConnections(int storeConnections)
    {
        this.storeConnections = storeConnections;
        return this;
    }
    
    @NotNull
    public String getDefaultIntervalStragety() {
        return defaultIntervalStragety;
    }
    
    @Config("armor.default-interval-stragety")
    @ConfigDescription("Set the default interval stragety, can be set to any interval or \"none\"")
    public ArmorConfig setDefaultIntervalStragety(String defaultIntervalStragety)
    {
        this.defaultIntervalStragety = defaultIntervalStragety;
        return this;
    }
    
    @NotNull
    public String getStoreType()
    {
        return storeType;
    }

    @Config("armor.store.type")
    @ConfigDescription("The store type for armor files")
    public ArmorConfig setStoreType(String storeType)
    {
        this.storeType = storeType;
        return this;
    }
    
    @NotNull
    public String getStoreLocation()
    {
        return storeLocation;
    }

    @Config("armor.store.location")
    @ConfigDescription("The base location for the store")
    public ArmorConfig setStoreLocation(String storeLocation)
    {
        this.storeLocation = storeLocation;
        return this;
    }
    
    public boolean isAttemptPushdownEnabled()
    {
        return pushdown;
    }

    @Config("armor.attempt-predicate-pushdown")
    @ConfigDescription("Attempt predicate pushdown")
    public ArmorConfig setAttemptPushdownEnabled(boolean pushdown)
    {
        this.pushdown = pushdown;
        return this;
    }
}
