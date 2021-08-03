package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.*;
import org.hkust.schema.RelationSchema;

import java.util.List;

class CountDistinctProcessFunctionWriter extends ProcessFunctionWriter {
    private final PicoWriter writer = new PicoWriter();
    private final DistinctCountProcessFunction aggregateProcessFunction;
    private final String aggregateType;
    private final String className;
    private final RelationSchema relationSchema;
    private final boolean hasMultipleAggregation;

    CountDistinctProcessFunctionWriter(final DistinctCountProcessFunction aggregateProcessFunction, RelationSchema schema) {
        super(schema);
        this.relationSchema = schema;
        this.aggregateProcessFunction = aggregateProcessFunction;
        aggregateType = "Int";
        hasMultipleAggregation = false;
        className = getProcessFunctionClassName(aggregateProcessFunction.getName());
    }

    @Override
    public String write(String filePath) throws Exception {
        addImports(writer);
        addConstructorAndOpenClass(writer);
        List<SelectCondition> aggregateSelectConditions = aggregateProcessFunction.getAggregateSelectCondition();
        if (aggregateSelectConditions != null && !aggregateSelectConditions.isEmpty()) {
            addIsOutputValidFunction(writer, aggregateSelectConditions);
        }
        closeClass(writer);
        writeClassFile(className, filePath, writer.toString());
        return className;
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


    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import org.hkust.BasedProcessFunctions.CountDistinctAggregation");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        //TODO: apply the next_key optimization on thiskey, remember: next_key is now output_key and requires no such optimizations
        List<AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        String code = "class " +
                className +
                " extends CountDistinctAggregation[Any]" +
                "(" +
                keyListToCode(aggregateProcessFunction.getDistinctKey()) +
                ", \"" +
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


}
