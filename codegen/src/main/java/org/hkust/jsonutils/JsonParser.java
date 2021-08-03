package org.hkust.jsonutils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.hkust.objects.*;
import org.hkust.schema.Relation;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.hkust.objects.Operator.CASE;
import static org.hkust.objects.Operator.COUNT_DISTINCT;

@SuppressWarnings("unchecked")
public class JsonParser {
    private static final Gson gson = new Gson();

    public static Node parse(final String jsonPath) throws Exception {
        String jsonString = new String(Files.readAllBytes(Paths.get(jsonPath)));
        Map<String, Object> map = gson.fromJson(jsonString, new TypeToken<Map<String, Object>>() {
        }.getType());

        Map<Relation, Relation> joinStructure = makeJoinStructure((List<Map<String, String>>) map.get("join_structure"));

        List<Map<String, Object>> rpfMap = (List<Map<String, Object>>) map.get("RelationProcessFunction");
        List<RelationProcessFunction> rpfs = makeRelationProcessFunctions(rpfMap);

        List<Map<String, Object>> apfMap = (List<Map<String, Object>>) map.get("AggregateProcessFunction");
        List<AggregateProcessFunction> apfs = makeAggregateProcessFunctions(apfMap);

        Map<String, Object> transMap = (Map<String, Object>) map.getOrDefault("transformer", null);
        TransformerFunction transformerFunction = makeTransformerFunction(transMap);

        return new Node(rpfs, apfs, joinStructure, transformerFunction);
    }

    private static TransformerFunction makeTransformerFunction(Map<String, Object> transMap) {
        if (transMap == null) return null;
        Map<String, Object> exprMap = (Map<String, Object>) transMap.get("expr");
        String name = (String) transMap.get("alias");
        Expression expr = (Expression) makeValue(exprMap);
        return new TransformerFunction(name, expr);
    }

    private static List<RelationProcessFunction> makeRelationProcessFunctions(List<Map<String, Object>> rpfList) {
        List<RelationProcessFunction> result = new ArrayList<>();
        rpfList.forEach(rpf -> {
            Map<String, Object> scMap = (Map<String, Object>) rpf.get("select_conditions");
            if (scMap != null) {
                List<Expression> scExpressions = makeSelectConditionsExpressions((List<Map<String, Object>>) scMap.get("values"));
                String operator = (String) scMap.get("operator");
                List<SelectCondition> selectConditions = makeSelectConditions(operator == null ? null : Operator.getOperator(operator), scExpressions);
                result.add(makeRelationProcessFunction(rpf, selectConditions));
            } else {
                result.add(makeRelationProcessFunction(rpf, null));
            }
        });

        return result;
    }

    @VisibleForTesting
    static RelationProcessFunction makeRelationProcessFunction(Map<String, Object> rpfMap, List<SelectCondition> selectConditions) {

        return new RelationProcessFunction(
                (String) rpfMap.get("name"),
                (String) rpfMap.get("relation"),
                (List<String>) rpfMap.get("this_key"),
                (List<String>) rpfMap.get("next_key"),
                //Data type coming from json is a double
                ((Double) rpfMap.get("child_nodes")).intValue(),
                (boolean) rpfMap.get("is_Root"),
                (boolean) rpfMap.get("is_Last"),
                (Map<String, String>) rpfMap.get("rename_attribute"),
                selectConditions,
                (String) rpfMap.get("id")
        );
    }

    @Nullable
    private static Map<Relation, Relation> makeJoinStructure(List<Map<String, String>> joinStructure) {
        if (joinStructure == null) {
            return null;
        }

        Map<Relation, Relation> structure = new HashMap<>();
        joinStructure.forEach(js -> structure.put(Relation.getRelation(js.get("primary")), Relation.getRelation(js.get("foreign"))));

        return structure;
    }

    private static List<AggregateProcessFunction> makeAggregateProcessFunctions(List<Map<String, Object>> apfList) {
        List<AggregateProcessFunction> result = new ArrayList<>();
        apfList.forEach(apf -> {
            List<Map<String, Object>> agMap = (List<Map<String, Object>>) apf.get("AggregateValue");
            List<AggregateValue> aggregateValues = makeAggregateValues(agMap);
            if (!testIfDistinctCount(aggregateValues)) {
                result.add(makeAggregateProcessFunction(apf, aggregateValues));
            } else {
                result.add(makeDistinctProcessFunction(apf, aggregateValues));
            }
        });

        return result;
    }

    private static Boolean testIfDistinctCount(List<AggregateValue> aggregateValues) {
        for (AggregateValue i : aggregateValues) {
            if (i.getAggregation() == COUNT_DISTINCT) return true;
        }
        return false;
    }

    @VisibleForTesting
    static AggregateProcessFunction makeAggregateProcessFunction(Map<String, Object> apfMap, List<AggregateValue> aggregateValues) {
        List<SelectCondition> outputSelectConditions = getOutputSelectConditions(apfMap);
        return new AggregateProcessFunction(
                (String) apfMap.get("name"),
                (List<String>) apfMap.get("this_key"),
                (List<String>) apfMap.get("output_key"),
                //Currently this assumes that there is exactly 1 AggregateValue, we may have more than one
                aggregateValues,
                outputSelectConditions);
    }

    static DistinctCountProcessFunction makeDistinctProcessFunction(Map<String, Object> apfMap, List<AggregateValue> aggregateValues) {
        List<SelectCondition> outputSelectConditions = getOutputSelectConditions(apfMap);
        return new DistinctCountProcessFunction(
                (String) apfMap.get("name"),
                (List<String>) apfMap.get("this_key"),
                (List<String>) apfMap.get("output_key"),
                //Currently this assumes that there is exactly 1 AggregateValue, we may have more than one
                aggregateValues,
                outputSelectConditions);
    }

    @Nullable
    private static List<SelectCondition> getOutputSelectConditions(Map<String, Object> apfMap) {
//        Map<String, Object> outputSelectConditionMap = (Map<String, Object>) apfMap.get("OutputSelectCondition");
        List<Map<String, Object>> values = (List<Map<String, Object>>) apfMap.get("OutputSelectCondition");
        List<SelectCondition> outputSelectConditions = null;
        if (values != null && !values.isEmpty()) {
            List<Expression> expressions = makeSelectConditionsExpressions(values);
            outputSelectConditions = makeSelectConditions(null, expressions);
        }
        return outputSelectConditions;
    }

    private static List<AggregateValue> makeAggregateValues(List<Map<String, Object>> aggregateValues) {
        List<AggregateValue> result = new ArrayList<>();
        for (Map<String, Object> aggValue : aggregateValues) {
            Value agv;
            agv = makeValue((Map<String, Object>) aggValue.get("value"));
            result.add(makeAggregateValue(aggValue, agv));
        }

        return result;
    }

    @VisibleForTesting
    static AggregateValue makeAggregateValue(Map<String, Object> avMap, Value value) {
        Operator aggregation = Operator.getOperator((String) avMap.get("aggregation"));
        Class<?> value_type = Type.getClass((String) avMap.get("value_type"));
        if (value_type == null) {
            throw new IllegalArgumentException("Non supported value type!");
        }
        String aggregateName = (String) avMap.get("name");
        return new AggregateValue(aggregateName, value, aggregation, value_type);
    }

    @VisibleForTesting
    static List<SelectCondition> makeSelectConditions(Operator nextOperator, List<Expression> expressions) {
        if (expressions == null) return null;
        List<SelectCondition> selectConditions = new ArrayList<>();
        for (Expression expression : expressions) {
            SelectCondition sc = new SelectCondition(expression, nextOperator);
            selectConditions.add(sc);
        }

        return selectConditions;
    }

    @VisibleForTesting
    static Expression makeExpression(Map<String, Object> value) {
        if (value == null || value.isEmpty()) return null;
        Operator operator = Operator.getOperator((String) value.get("operator"));
        if (operator != CASE) {
            Value left = makeValue((Map<String, Object>) value.get("left_field"));
            Value right = makeValue((Map<String, Object>) value.get("right_field"));
            return new Expression(Arrays.asList(left, right), operator);
        } else {
            Value condition = makeValue((Map<String, Object>) value.get("if_condition"));
            Value then_value = makeValue((Map<String, Object>) value.get("then_value"));
            Value else_value = makeValue((Map<String, Object>) value.get("else_value"));
            return new Expression(Arrays.asList(condition, then_value, else_value), operator);
        }
    }

    @VisibleForTesting
    @Nullable
    static List<Expression> makeSelectConditionsExpressions(List<Map<String, Object>> values) {
        if (values == null || values.isEmpty()) return null;

        List<Expression> expressions = new ArrayList<>();
        for (Map<String, Object> value : values) {
            expressions.add(makeExpression(value));
        }

        return expressions;
    }

    @VisibleForTesting
    static Value makeAggregateValueExpression(List<Map<String, Object>> valuesList, String operator) {
        List<Value> values = new ArrayList<>();
        for (Map<String, Object> value : valuesList) {
            values.add(makeValue(value));
        }

        return new Expression(values, Operator.getOperator(operator.toLowerCase()));
    }

    @VisibleForTesting
    private static Value makeValue(Map<String, Object> field) {
        String type = (String) field.get("type");
        Value value;
        if (type == null) {
            return null;
        }
        else if (type.equals("attribute")) {
            String name = (String) field.get("name");
            Relation relation = Relation.getRelation((String) field.get("relation"));
            value = new AttributeValue(relation, name);
        } else if (type.equals("constant")) {
            value = new ConstantValue(field.get("value").toString(), (String) field.get("var_type"));
        } else if (type.equals("expression")) {
            return makeExpression(field);
        } else if (type.equals("aggregate_attribute")) {
            return new AggregateAttributeValue(type, (String) field.get("name"), Type.getClass((String) field.get("var_type")), Type.getClass((String) field.get("store_type")));
        } else {
            throw new RuntimeException("Unknown field type " + type);
        }

        return value;
    }
}
