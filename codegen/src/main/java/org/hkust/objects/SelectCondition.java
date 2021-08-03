package org.hkust.objects;

import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;

public class SelectCondition {
    private final Expression expression;
    //This is the operator between each select condition (AND/OR) e.g. condition1 AND condition2
    @Nullable("if there is only 1 select condition")
    private final Operator operator;

    public SelectCondition(final Expression expression, final Operator operator) {
        requireNonNull(expression);
        int size = expression.getValues().size();
        if (size != 2) {
            throw new RuntimeException("There must be exactly 2 values in the expression of a select condition");
        }
        this.expression = expression;
        this.operator = operator;
    }

    public Expression getExpression() {
        return expression;
    }

    @Nullable
    public Operator getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        return "SelectCondition{" +
                "expression=" + expression +
                ", operator=" + operator +
                '}';
    }
}
