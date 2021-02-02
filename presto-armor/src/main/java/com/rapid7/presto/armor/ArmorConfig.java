package com.rapid7.presto.armor;

import javax.validation.constraints.NotNull;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class ArmorConfig {
    private String storeType;
    private String storeLocation;

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
}
