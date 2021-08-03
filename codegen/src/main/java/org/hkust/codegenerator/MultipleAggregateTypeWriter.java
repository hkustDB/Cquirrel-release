package org.hkust.codegenerator;

import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.AggregateProcessFunction;
import org.hkust.objects.AggregateValue;
import org.hkust.objects.Operator;

import java.util.stream.Collectors;

public class MultipleAggregateTypeWriter implements ClassWriter {
    private final String className;
    private final PicoWriter writer = new PicoWriter();
    private final AggregateProcessFunction aggregateProcessFunction;
    private boolean hasCountOrder;

    MultipleAggregateTypeWriter(AggregateProcessFunction apf) {
        this.className = "QMultipleAggregateType";
        this.aggregateProcessFunction = apf;

    }


    public String write(String filePath) throws Exception {
        addCaseClassParameters(writer);
        addOpenBrace(writer);
        addAdditionMethod(writer);
        addSubtractionMethod(writer);
        addToStringMethod(writer);
        addCloseBrace(writer);
        writeClassFile(this.className, filePath, writer.toString());
        return this.className;
    }

    @Override
    public void addImports(PicoWriter writer) {

    }

    @Override
    public void addConstructorAndOpenClass(PicoWriter writer) {

    }


    void addCaseClassParameters(final PicoWriter writer) throws Exception {
        writer.writeln_r("case class " + this.className + "(");
        for (AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            String aggregateValueName = aggregateValue.getName().toUpperCase();
            String aggregateValueType = aggregateValue.getValueType().getSimpleName();
            if (aggregateValueType.equals("Integer")) {
                aggregateValueType = "Int";
            }
            writer.writeln(aggregateValueName + " : " + aggregateValueType + ", ");
        }
        writer.writeln("cnt : Int)");
    }


    void addAdditionMethod(final PicoWriter writer) throws Exception {
        writer.writeln_r("def +(that : " + this.className + ") : " + this.className + " = {");
        writer.writeln_r(this.className + "(");
        for (AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            String aggregateValueName = aggregateValue.getName().toUpperCase();
            if (aggregateValue.getAggregation() == Operator.AVG) {
                writer.writeln(aggregateValuesAverageOperations(aggregateValueName, "+"));
            } else {
                writer.writeln(aggregateValuesFirstLevelOperations(aggregateValueName, "+"));
            }
        }
        writer.writeln("this.cnt + that.cnt");
        writer.writeln_l(")");
        writer.writeln_l("}");
    }

    void addSubtractionMethod(final PicoWriter writer) throws Exception {
        writer.writeln_r("def -(that : " + this.className + ") : " + this.className + " = {");
        writer.writeln_r(this.className + "(");
        for (AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            String aggregateValueName = aggregateValue.getName().toUpperCase();
            if (aggregateValue.getAggregation() == Operator.AVG) {
                writer.writeln(aggregateValuesAverageOperations(aggregateValueName, "-"));
            } else {
                writer.writeln(aggregateValuesFirstLevelOperations(aggregateValueName, "-"));
            }
        }
        writer.writeln("this.cnt - that.cnt");
        writer.writeln_l(")");
        writer.writeln_l("}");
    }

    void addToStringMethod(final PicoWriter writer) throws Exception {
        writer.writeln_r("override def toString : String = {");
        String joinedStr = aggregateProcessFunction.getAggregateValues()
                .stream().map(aggregateValue -> {
                    if (aggregateValue.getValueType().getSimpleName().equals("Double")) {
                        return "\"%f\".format(" + aggregateValue.getName().toUpperCase() + ")";
                    } else {
                        return "\"%s\".format(" + aggregateValue.getName().toUpperCase() + ")";
                    }
                })
                .collect(Collectors.joining(" + \"|\" + "));
        writer.writeln(joinedStr);
        writer.writeln_l("}");
    }

    void addOpenBrace(final PicoWriter writer) throws Exception {
        writer.writeln_lr("{");
    }

    void addCloseBrace(final PicoWriter writer) throws Exception {
        writer.writeln_l("}");
    }

    String aggregateValuesFirstLevelOperations(String aggregateValueName, String op) {
        StringBuilder sb = new StringBuilder();
        sb.append("this.")
                .append(aggregateValueName)
                .append(" ")
                .append(op)
                .append(" that.")
                .append(aggregateValueName)
                .append(", ");
        return sb.toString();
    }

    String aggregateValuesAverageOperations(String aggregateValueName, String op) {
        StringBuilder sb = new StringBuilder();
        sb.append("(this.")
                .append(aggregateValueName)
                .append(" * this.cnt ")
                .append(op)
                .append(" that.")
                .append(aggregateValueName)
                .append(" * that.cnt) / (this.cnt ")
                .append(op)
                .append(" that.cnt),");
        return sb.toString();
    }
}
