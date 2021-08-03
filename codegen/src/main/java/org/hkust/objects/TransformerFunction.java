package org.hkust.objects;

import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TransformerFunction {
    private final String name ;
    //private final List<String> outputKey;
    private final Expression expr;

    public TransformerFunction(String name, Expression expr) {
        this.name = name;
        //this.outputKey = outputKey;
        requireNonNull(expr);
        this.expr = expr;
    }

    public String getName() {
        return name;
    }

//    public List<String> getOutputKey() {
//        return outputKey;
//    }

    public Expression getExpr() {
        return expr;
    }

    @Nullable

    @Override
    public String toString() {
        return "AggregateProcessFunction{" +
                "name='" + name + '\'' +
                ", computation=" + expr +
                '}';
    }
}
