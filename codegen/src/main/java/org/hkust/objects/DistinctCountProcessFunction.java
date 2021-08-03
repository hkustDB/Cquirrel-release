package org.hkust.objects;

import com.google.common.collect.ImmutableSet;
import org.hkust.schema.Attribute;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DistinctCountProcessFunction extends AggregateProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> outputKey;

    private final List<String> distinctKey;

    private final List<AggregateValue> aggregateValues;
    private final List<SelectCondition> aggregateSelectCondition;

    public DistinctCountProcessFunction(String name, List<String> thisKey, List<String> outputKey, List<AggregateValue> aggregateValues,
                                        //Operator aggregation, Class valueType,
                                        List<SelectCondition> aggregateSelectCondition) {
        super(name, thisKey, outputKey, aggregateValues, aggregateSelectCondition);
        this.name = name;
        this.thisKey = thisKey;
        this.outputKey = outputKey;
        this.aggregateSelectCondition = aggregateSelectCondition;
        requireNonNull(aggregateValues);
        this.aggregateValues = aggregateValues;
        distinctKey = this.aggregateValues.stream().map((x) ->
            ((AttributeValue) x.getValue()).getColumnName()
        ).collect(Collectors.toList());
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

    public List<String> getDistinctKey() {
        return distinctKey;
    }

    public List<AggregateValue> getAggregateValues() {
        return aggregateValues;
    }

    @Nullable
    public List<SelectCondition> getAggregateSelectCondition() {
        return aggregateSelectCondition;
    }

    @Override
    public String toString() {
        return "DistinctCountProcessFunction{" +
                "name='" + name + '\'' +
                ", distinctKey=" + distinctKey +
                ", thisKey=" + thisKey +
                ", nextKey=" + outputKey +
                ", computation=" + aggregateValues +
                '}';
    }
}
