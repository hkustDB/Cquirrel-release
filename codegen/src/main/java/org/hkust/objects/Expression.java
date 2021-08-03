package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;
import org.hkust.schema.Relation;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Expression implements Value {
    private final List<Value> values;
    private final Operator operator;

    public Expression(List<Value> values, Operator operator) {
        CheckerUtils.checkNullOrEmpty(values, "values");
        requireNonNull(operator);
        validate(values.size(), operator);
        this.values = values;
        this.operator = operator;
    }

    public List<Value> getValues() {
        return values;
    }

    public Operator getOperator() {
        return operator;
    }

    private void validate(int numOfValues, Operator operator) {
        if (numOfValues == 0) {
            throw new IllegalArgumentException("Expression must have at least 1 value");
        } else if (numOfValues == 1 && operator != Operator.NOT) {
            throw new IllegalArgumentException("Expression with 1 value can only have ! as the operator");
        } else if (numOfValues > 2) {
            if (operator != Operator.AND && operator != Operator.OR && operator != Operator.CASE) {
                throw new IllegalArgumentException("Expression with more than 2 values can only have && or || as the operator");
            }
        }
    }

    public boolean hasOtherRelationAttributes(Relation relation) {
        List<Value> values = this.getValues();
        for (Value v : values) {
            if (v instanceof  Expression) {
                return ((Expression) v).hasOtherRelationAttributes(relation);
            }
            if (v instanceof AttributeValue) {
                if (! ((AttributeValue) v).getRelation().equals(relation)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Expression{" +
                "values=" + values +
                ", operator=" + operator +
                '}';
    }
}
