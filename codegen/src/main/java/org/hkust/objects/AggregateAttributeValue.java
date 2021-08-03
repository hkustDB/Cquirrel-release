package org.hkust.objects;

import static java.util.Objects.requireNonNull;

public class AggregateAttributeValue implements Value {
    private final String type;
    private final String name;
    private final Class<?> varType;
    private final Class<?> storeType;

    public AggregateAttributeValue(String type, String name, Class<?> varType, Class<?> storeType) {
        requireNonNull(type);
        requireNonNull(name);
        requireNonNull(varType);
        requireNonNull(storeType);
        this.type = type;
        this.name = name;
        this.varType = varType;
        this.storeType = storeType;

    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Class<?> getVarType() {
        return varType;
    }

    public Class<?> getStoreType() {
        return storeType;
    }
}
