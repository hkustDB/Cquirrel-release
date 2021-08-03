package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.*;
import org.hkust.schema.RelationSchema;

import java.util.List;

class AggregateProcessFunctionWriter extends ProcessFunctionWriter {
    private final PicoWriter writer = new PicoWriter();
    private final AggregateProcessFunction aggregateProcessFunction;
    private final String aggregateType;
    private final String className;
    private final RelationSchema relationSchema;
    private final boolean hasMultipleAggregation;

    AggregateProcessFunctionWriter(final AggregateProcessFunction aggregateProcessFunction, RelationSchema schema) {
        super(schema);
        this.relationSchema = schema;
        this.aggregateProcessFunction = aggregateProcessFunction;
        // If only one aggregation exists
        if (aggregateProcessFunction.getAggregateValues().size() == 1) {
            Class<?> type = aggregateProcessFunction.getAggregateValues().get(0).getValueType();
            aggregateType = type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName();
            hasMultipleAggregation = false;
        } else {
            int a = aggregateProcessFunction.getAggregateValues().size();
            aggregateType = "QMultipleAggregateType";
            hasMultipleAggregation = true;
        }
        className = getProcessFunctionClassName(aggregateProcessFunction.getName());
    }

    @Override
    public String write(String filePath) throws Exception {
        addImports(writer);
        addConstructorAndOpenClass(writer);
        if (hasMultipleAggregation) {
            addMultipleAggregateFunction(writer);
        } else {
            addAggregateFunction(writer);
        }
        addAdditionFunction(writer);
        addSubtractionFunction(writer);
        List<SelectCondition> aggregateSelectConditions = aggregateProcessFunction.getAggregateSelectCondition();
        if (aggregateSelectConditions != null && !aggregateSelectConditions.isEmpty()) {
            addIsOutputValidFunction(writer, aggregateSelectConditions);
        }
        if (hasMultipleAggregation) {
            addMultipleInitStateFunction(writer);
            addGetAggregateMethod(writer);
        } else {
            addInitStateFunction(writer);
        }
        closeClass(writer);
        writeClassFile(className, filePath, writer.toString());

        if (hasMultipleAggregation) {
            new MultipleAggregateTypeWriter(aggregateProcessFunction).write(filePath);
        }

        return className;
    }

    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import org.apache.flink.api.common.state.ValueStateDescriptor");
        writer.writeln("import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}");
        writer.writeln("import org.hkust.BasedProcessFunctions.AggregateProcessFunction");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        //TODO: apply the next_key optimization on thiskey, remember: next_key is now output_key and requires no such optimizations
        List<AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        String code = "class " +
                className +
                " extends AggregateProcessFunction[Any, " +
                aggregateType +
                "](\"" +
                className +
                "\", " +
                keyListToCode(optimizeKey(aggregateProcessFunction.getThisKey())) +
                ", " +
                keyListToCode(aggregateProcessFunction.getOutputKey()) +
                "," +
                " aggregateName = \"" +
                (aggregateValues.size() == 1 ? aggregateValues.get(0).getName() : "_multiple_") + "\"" +
                ", deltaOutput = true" +
                ") {";
        writer.writeln_r(code);
    }

    @VisibleForTesting
    void addAggregateFunction(final PicoWriter writer) throws Exception {
        writer.writeln_r("override def aggregate(value: Payload): " + aggregateType + " = {");
        List<AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        for (AggregateValue aggregateValue : aggregateValues) {
            StringBuilder code = new StringBuilder();
            Value type = aggregateValue.getValue();
            if (type.getClass() == Expression.class) { //type.equals("expression")) {
                Expression expression = (Expression) aggregateValue.getValue();
                expressionToCode(expression, code);
                writer.writeln(code.toString());
            } else if (type.getClass() == AttributeValue.class) {//equals("attribute")) {
                AttributeValue attributeValue = (AttributeValue) aggregateValue.getValue();
                attributeValueToCode(attributeValue, code);
                writer.writeln(code.toString());
            } else if (type.getClass() == ConstantValue.class) {//type.equals("constant")) {
                constantValueToCode((ConstantValue) aggregateValue.getValue(), code);
                writer.writeln(code.toString());
            } else {
                throw new RuntimeException("Unknown type for AggregateValue, got: " + type);
            }
        }
        writer.writeln_l("}");
    }

    @VisibleForTesting
    void addMultipleAggregateFunction(final PicoWriter writer) throws Exception {
        writer.writeln_r("override def aggregate(value: Payload): " + aggregateType + " = {");
        List<AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        StringBuilder multipleAggregateName = new StringBuilder();
        multipleAggregateName.append(aggregateType).append("(");
        for (AggregateValue aggregateValue : aggregateValues) {
            StringBuilder code = new StringBuilder();
            code.append("val ").append(aggregateValue.getName()).append(" = ");
            multipleAggregateName.append(aggregateValue.getName()).append(", ");
            Value type = aggregateValue.getValue();
            if (type == null) {  // when the operator is COUNT, then type is null.
                if (aggregateValue.getAggregation().equals(Operator.COUNT)) {
                    // TODO fix the COUNT(*) condition.
                    code.append("1");
                    writer.writeln(code.toString());
                } else {
                    throw new RuntimeException("null type for AggregateValue!");
                }
            } else {
                if (type.getClass() == Expression.class) { //type.equals("expression")) {
                    Expression expression = (Expression) aggregateValue.getValue();
                    expressionToCode(expression, code);
                    writer.writeln(code.toString());
                } else if (type.getClass() == AttributeValue.class) {//equals("attribute")) {
                    AttributeValue attributeValue = (AttributeValue) aggregateValue.getValue();
                    attributeValueToCode(attributeValue, code);
                    writer.writeln(code.toString());
                } else if (type.getClass() == ConstantValue.class) {//type.equals("constant")) {
                    constantValueToCode((ConstantValue) aggregateValue.getValue(), code);
                    writer.writeln(code.toString());
                } else {
                    throw new RuntimeException("Unknown type for AggregateValue, got: " + type);
                }
            }
        }
        multipleAggregateName.append("1)");
        writer.writeln(multipleAggregateName.toString());
        writer.writeln_l("}");
    }


    void addIsOutputValidFunction(final PicoWriter writer, List<SelectCondition> aggregateSelectConditions) throws Exception {
        if (aggregateSelectConditions.size() != 1) {
            throw new RuntimeException("Currently only 1 aggregate select condition is supported");
        }
        SelectCondition condition = aggregateSelectConditions.get(0);
        StringBuilder code = new StringBuilder();
        code.append("override def isOutputValid(value: Payload): Boolean = {");
        code.append("if(");
        expressionToCode(condition.getExpression(), code);
        writer.write(code.toString());
        writer.writeln_r("){");
        writer.write("true");
        writer.writeln_lr("}else{");
        writer.write("false");
        writer.writeln_l("}");
        writer.writeln_l("}");
    }

    @VisibleForTesting
    void addAdditionFunction(final PicoWriter writer) {
        writer.writeln("override def addition(value1: " + aggregateType + ", value2: " + aggregateType + "): " + aggregateType + " = value1 + value2");
    }

    @VisibleForTesting
    void addSubtractionFunction(final PicoWriter writer) {
        writer.writeln("override def subtraction(value1: " + aggregateType + ", value2: " + aggregateType + "): " + aggregateType + " = value1 - value2");
    }

    @VisibleForTesting
    void addInitStateFunction(final PicoWriter writer) {
        writer.writeln_r("override def initstate(): Unit = {");
        writer.writeln("val valueDescriptor = TypeInformation.of(new TypeHint[" + aggregateType + "](){})");
        writer.writeln("val aliveDescriptor : ValueStateDescriptor[" + aggregateType + "] = new ValueStateDescriptor[" + aggregateType + "](\"" + className + "\"+\"Alive\", valueDescriptor)");
        writer.writeln("alive = getRuntimeContext.getState(aliveDescriptor)");
        writer.writeln_r("}");
        Class<?> type = Type.getClass(aggregateType);
        writer.writeln("override val init_value: " + aggregateType + (type != null && type.equals(Integer.class) ? " = 0" : " = 0.0"));
    }

    @VisibleForTesting
    void addMultipleInitStateFunction(final PicoWriter writer) {
        writer.writeln_r("override def initstate(): Unit = {");
        writer.writeln("val valueDescriptor = TypeInformation.of(new TypeHint[" + aggregateType + "](){})");
        writer.writeln("val aliveDescriptor : ValueStateDescriptor[" + aggregateType + "] = new ValueStateDescriptor[" + aggregateType + "](\"" + className + "\"+\"Alive\", valueDescriptor)");
        writer.writeln("alive = getRuntimeContext.getState(aliveDescriptor)");
        writer.writeln_l("}");

        Class<?> type = Type.getClass(aggregateType);

        StringBuilder sb = new StringBuilder();
        sb.append("override val init_value: ").append(aggregateType).append(" = ").append(aggregateType).append("(");
        for (AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            if (aggregateValue.getValueType().getSimpleName().equals("Double")) {
                sb.append("0.0, ");
            }
            if (aggregateValue.getValueType().getSimpleName().equals("Integer")) {
                sb.append("0, ");
            }
        }
        sb.append("0)");
        writer.writeln(sb.toString());
    }

    @VisibleForTesting
    void addGetAggregateMethod(final PicoWriter writer) {
        writer.writeln_r("override def getAggregate(value : " + aggregateType + ") : (Array[Any], Array[String]) = {");
        StringBuilder sb = new StringBuilder();
        sb.append("(Array(");
        for (AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            sb.append("value.").append(aggregateValue.getName().toUpperCase()).append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("),");
        writer.writeln(sb.toString());
        sb.setLength(0);

        sb.append("Array(");
        for (AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            sb.append("\"").append(aggregateValue.getName().toUpperCase()).append("\", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("))");
        writer.writeln(sb.toString());
        writer.writeln_l("}");
    }

}
