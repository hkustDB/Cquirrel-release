package org.hkust.objects;

import com.google.common.collect.ImmutableSet;
import org.hkust.schema.Attribute;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunction extends ProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> outputKey;

    private final List<org.hkust.objects.AggregateValue> aggregateValues;
    private final List<SelectCondition> aggregateSelectCondition;

    public AggregateProcessFunction(String name, List<String> thisKey, List<String> outputKey, List<AggregateValue> aggregateValues,
                                    //Operator aggregation, Class valueType,
                                    List<SelectCondition> aggregateSelectCondition) {
        super(name, thisKey, outputKey);
        this.name = name;
        this.thisKey = thisKey;
        this.outputKey = outputKey;
        this.aggregateSelectCondition = aggregateSelectCondition;

        requireNonNull(aggregateValues);
        this.aggregateValues = aggregateValues;
    }

    @Override
    public Set<Attribute> getAttributeSet(RelationSchema schema) {
        Set<Attribute> result = new HashSet<>();

        aggregateValues.forEach(aggregateValue -> {
            Value value = aggregateValue.getValue();
            if (value instanceof Expression) {
                Expression expression = (Expression) value;
                addExpressionAttributes(expression, result, schema);
            } else if (value instanceof AttributeValue) {
                addIfAttributeValue(result, value, schema);
            }
        });

        return ImmutableSet.copyOf(result);
    }

    private void addExpressionAttributes(Expression expression, Set<Attribute> attributes, RelationSchema schema) {
        expression.getValues().forEach(val -> {
            addIfAttributeValue(attributes, val, schema);
            if (val instanceof Expression) {
                addExpressionAttributes((Expression) val, attributes, schema);
            }
        });
    }

    @Override
    public String getName() {
        return name;
    }

    @Nullable
    public List<String> getThisKey() {
        return thisKey;
    }

    @Nullable
    public List<String> getOutputKey() {
        return outputKey;
    }

    public List<org.hkust.objects.AggregateValue> getAggregateValues() {
        return aggregateValues;
    }

    @Nullable
    public List<SelectCondition> getAggregateSelectCondition() {
        return aggregateSelectCondition;
    }

    @Override
    public String toString() {
        return "AggregateProcessFunction{" +
                "name='" + name + '\'' +
                ", thisKey=" + thisKey +
                ", nextKey=" + outputKey +
                ", computation=" + aggregateValues +
                '}';
    }
}
