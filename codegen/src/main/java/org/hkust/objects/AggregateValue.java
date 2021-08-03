package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;

import static java.util.Objects.requireNonNull;

public class AggregateValue implements Value {
    private final String name;
    private final Value value;
    private final Operator aggregation;
    private final Class<?> valueType;

    public AggregateValue(String name, final Value value, Operator aggregation, Class<?> valueType) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        this.name = name;
//        requireNonNull(value);
        this.value = value;
        requireNonNull(aggregation);
        this.aggregation = aggregation;
        requireNonNull(valueType);
        this.valueType = valueType;

    }

    public String getName() {
        return name;
    }

    public Operator getAggregation() {
        return aggregation;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    public Value getValue() {
        return value;
    }
}
