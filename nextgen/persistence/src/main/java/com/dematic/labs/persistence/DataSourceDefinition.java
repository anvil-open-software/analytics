package com.dematic.labs.persistence;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

public abstract class DataSourceDefinition {
    protected Map properties = new HashMap<>();

    @NotNull
    public Map getProperties() {

        return properties;
    }
}
