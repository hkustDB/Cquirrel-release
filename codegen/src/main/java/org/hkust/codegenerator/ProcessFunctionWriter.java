package org.hkust.codegenerator;

import org.hkust.objects.*;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.hkust.schema.Schema;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.hkust.objects.Type.getStringConversionMethod;

abstract class ProcessFunctionWriter implements ClassWriter {
    private final RelationSchema relationSchema;

    ProcessFunctionWriter(RelationSchema relationSchema) {
        this.relationSchema = relationSchema;
    }

    protected String keyListToCode(@Nullable List<String> keyList) {
        StringBuilder code = new StringBuilder();
        code.append("Array(");
        if (keyList != null) {
            for (int i = 0; i < keyList.size(); i++) {
                code.append("\"");
                code.append(keyList.get(i).toUpperCase());
                code.append("\"");
                if (i != keyList.size() - 1) {
                    code.append(",");
                }
            }
        }
        code.append(")");

        return code.toString();
    }

    protected void expressionToCode(final Expression expression, StringBuilder code) {
        List<Value> values = expression.getValues();
        int size = values.size();
        if (expression.getOperator().equals(Operator.CASE)) {
            caseIfCode(expression, code);
            return;
        }
        if (expression.getOperator().equals(Operator.LIKE)) {
            likeToCode(expression, code);
            return;
        }
        if (expression.getOperator().equals(Operator.NOT_LIKE)) {
            notLikeToCode(expression, code);
            return;
        }
        for (int i = 0; i < size; i++) {
            Value value = values.get(i);
            if (value instanceof Expression) {
                code.append("(");
                expressionToCode((Expression) value, code);
                code.append(")");
            } else {
                valueToCode(value, code);
            }
            if (i != size - 1) {
                code.append(expression.getOperator().getValue());
            }
        }
    }

    protected void valueToCode(Value value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        //Note: expression can have an expression as one of its values, currently it is not being handled
        if (value instanceof ConstantValue) {
            constantValueToCode((ConstantValue) value, code);
        } else if (value instanceof AttributeValue) {
            attributeValueToCode((AttributeValue) value, code);
        } else if (value instanceof AggregateAttributeValue) {
            aggregationAttributeToCode((AggregateAttributeValue) value, code);
        } else if (value instanceof Expression) {
            expressionToCode((Expression) value, code);
        } else {
            throw new RuntimeException("Unknown type of value, expecting either ConstantValue or AttributeValue");
        }
    }

    protected void constantValueToCode(ConstantValue value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        Class<?> type = value.getType();
        if (type.equals(Type.getClass("date"))) {
            //Needed so generated code parses the date
            code.append("format.parse(\"").append(value.getValue()).append("\")");
        } else if (type.equals(Type.getClass("string"))) {
            code.append("\"").append(value.getValue()).append("\"");
        } else {
            code.append(value.getValue());
        }
    }

    protected void attributeValueToCode(AttributeValue value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        final String columnName = value.getColumnName();
        final Class<?> type = requireNonNull(relationSchema.getColumnAttributeByRawName(value.getRelation(), columnName.toLowerCase())).getType();
        code.append("value(\"")
                .append(columnName.toUpperCase())
                .append("\")")
                .append(".asInstanceOf[")
                .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                .append("]");
    }

    protected String getAttributeValueString(AttributeValue value) {
        requireNonNull(value);
        final String columnName = value.getColumnName();
        final Class<?> type = requireNonNull(relationSchema.getColumnAttributeByRawName(value.getRelation(), columnName.toLowerCase())).getType();
        StringBuilder sb = new StringBuilder();
        sb.append("value(\"")
                .append(columnName.toUpperCase())
                .append("\")")
                .append(".asInstanceOf[")
                .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                .append("]");
        return sb.toString();
    }

    protected void aggregationAttributeToCode(AggregateAttributeValue aggregateAttributeValue, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(aggregateAttributeValue);
        Class<?> type = aggregateAttributeValue.getStoreType();
        code.append("value(\"")
                .append(aggregateAttributeValue.getName().toUpperCase())
                .append("\")")
                .append(".asInstanceOf[")
                .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                .append("].")
                .append(getStringConversionMethod(aggregateAttributeValue.getVarType()));
    }

    private int getSubstringCountFromString(String str, String c) {
        Pattern pattern = Pattern.compile(c);
        Matcher matcher = pattern.matcher(str);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private void likeToCode(Expression expression, StringBuilder code) {
        if (expression.getValues().size() != 2) {
            throw new RuntimeException("Expecting 2 values with LIKE as operator for express, got: " + expression.getValues().size());
        }
        if (!(expression.getValues().get(0) instanceof AttributeValue)) {
            throw new RuntimeException("The first value in like expression should be attribute value!");
        }
        if (!(expression.getValues().get(1) instanceof ConstantValue)) {
            throw new RuntimeException("The second value in like expression should be constant value!");
        }

        AttributeValue attributeValue = (AttributeValue) expression.getValues().get(0);
        ConstantValue constantValue = (ConstantValue) expression.getValues().get(1);
        String pattern = constantValue.getValue();

        attributeValueToCode(attributeValue, code);

        int percentOccurrNum = getSubstringCountFromString(pattern, "%");
        if (percentOccurrNum == 0) {
            code.append(".equals(\"").append(pattern).append("\")");
        } else if (percentOccurrNum == 1) {
            if (pattern.startsWith("%")) {
                code.append(".endsWith(\"").append(pattern, 1, pattern.length()).append("\")");
            } else if (pattern.endsWith("%")) {
                code.append(".startsWith(\"").append(pattern, 0, pattern.length() - 1).append("\")");
            } else {
                throw new RuntimeException("This \"" + pattern + "\" has not supported yet.");
            }
        } else if (percentOccurrNum == 2) {
            if (pattern.startsWith("%") && pattern.endsWith("%")) {
                code.append(".contains(\"").append(pattern, 1, pattern.length() - 1).append("\")");
            } else {
                throw new RuntimeException("This \"" + pattern + "\" has not supported yet.");
            }
        } else if (percentOccurrNum == 3) {
            if (pattern.startsWith("%") && pattern.endsWith("%")) {
                String first = pattern.split("%")[1];
                String second = pattern.split("%")[2];
                code.append(".matches(\".*").append(first).append(".*").append(second).append(".*\")");
            } else {
                throw new RuntimeException("This \"" + pattern + "\" has not supported yet.");
            }
        } else {
            throw new RuntimeException("More than two % in like expression is not supported now!");
        }
    }

    private void notLikeToCode(Expression expression, StringBuilder code) {
        code.append("!");
        likeToCode(expression, code);
    }

    private void caseIfCode(Expression expression, StringBuilder code) {
        if (expression.getValues().size() != 3) {
            throw new RuntimeException("Expecting exactly 3 values with case if as operator for Expression, got: " + expression.getValues().size());
        }

        ifStatementCode(code, expression.getValues().get(0), expression.getValues().get(1), expression.getValues().get(2));
        /*for (Value value : expression.getValues()) {
            if (value instanceof Expression) {
                Expression exp = (Expression) value;
                if (exp.getValues().size() != 2) {
                    throw new RuntimeException("Expecting exactly 2 values in the expression of the if condition of a case if, got: " + exp.getValues().size());
                }
                if (exp.getValues().get(0) instanceof Expression) {
                    ifStatementCode(code, exp, 0, 1);
                } else {
                    ifStatementCode(code, exp, 1, 0);
                }
            } else if (value instanceof ConstantValue) {
                ConstantValue defaultReturn = (ConstantValue) value;
                code.append(" else ");
                constantValueToCode(defaultReturn, code);
            } else {
                throw new RuntimeException("Expecting only expressions and 1 constant value for case if, got: " + value);
            }
        }*/
    }

    //private void ifStatementCode(StringBuilder code, Expression exp, int i, int i2) {
    private void ifStatementCode(StringBuilder code, Value condition, Value then_value, Value else_value) {
        code.append("if(");
        valueToCode(condition, code);
        code.append(") ");
        valueToCode(then_value, code);
        code.append("\nelse ");
        valueToCode(else_value, code);
        code.append("\n");
        /*Expression exp1 = (Expression) exp.getValues().get(i);
        code.append("if(");
        valueToCode(exp1.getValues().get(0), code);
        code.append(exp1.getOperator().getValue());
        valueToCode(exp1.getValues().get(1), code);
        code.append(")");
        code.append(" ");
        valueToCode(exp.getValues().get(i2), code);
        code.append("\n");*/
    }

    protected List<String> optimizeKey(List<String> rpfNextKeys) {
        if (rpfNextKeys == null || rpfNextKeys.size() < 1) {
            return new ArrayList<>();
        }
        List<String> nextKeys = new ArrayList<>(rpfNextKeys);
        List<String> result = new ArrayList<>();
        Map<Relation, Schema> allSchemas = relationSchema.getAllSchemas();
        for (Map.Entry<Relation, Schema> entry : allSchemas.entrySet()) {
            Schema schema = entry.getValue();
            List<Attribute> primaryKeys = schema.getPrimaryKey();
            List<String> pkNames = primaryKeys.stream().map(Attribute::getName).collect(toList());
            boolean primaryKeysFound = nextKeys.containsAll(pkNames);
            if (primaryKeysFound) {
                result.addAll(pkNames);
                Set<Schema> childSchemas = relationSchema.getAllChildSchemas(pkNames);
                for (Schema cs : childSchemas) {
                    nextKeys.removeIf(nk -> relationSchema.getColumnAttribute(cs.getRelation(), nk) != null);
                }
            }
        }
        result.addAll(nextKeys);
        return result;
    }
}
