package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import org.ainslec.picocog.PicoWriter;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.objects.Expression;
import org.hkust.objects.RelationProcessFunction;
import org.hkust.objects.SelectCondition;
import org.hkust.schema.RelationSchema;

import java.util.List;

class RelationProcessFunctionWriter extends ProcessFunctionWriter {
    private final String className;
    private final PicoWriter writer = new PicoWriter();
    private final RelationProcessFunction relationProcessFunction;
    private final String RELATION_PROCESS_FUNCTION_IMPORT;
    private final RelationSchema relationSchema;

    RelationProcessFunctionWriter(final RelationProcessFunction relationProcessFunction, RelationSchema relationSchema) {
        super(relationSchema);
        this.relationSchema = relationSchema;
        this.relationProcessFunction = relationProcessFunction;
        this.className = getProcessFunctionClassName(relationProcessFunction.getName());
        this.RELATION_PROCESS_FUNCTION_IMPORT = (relationProcessFunction.isLeaf() ? "RelationFKProcessFunction" : "RelationFKCoProcessFunction");
    }

    @Override
    public String write(final String filePath) throws Exception {
        CheckerUtils.checkNullOrEmpty(filePath, "filePath");
        addImports(writer);
        addConstructorAndOpenClass(writer);
        addIsValidFunction(relationProcessFunction, writer);
//        addIsValidSelfFunction(relationProcessFunction, writer);
//        addIsValidOtherFunction(relationProcessFunction, writer);
        closeClass(writer);
        writeClassFile(className, filePath, writer.toString());

        return className;
    }

    @Deprecated
    @VisibleForTesting
    void addIsValidFunction(List<SelectCondition> selectConditions, final PicoWriter writer) throws Exception {
        writer.writeln_r("override def isValid(value: Payload): Boolean = {");
        if (selectConditions == null) {
            writer.writeln_r("true");
        } else {
            StringBuilder ifCondition = new StringBuilder();
            ifCondition.append("if(");
            SelectCondition condition;
            for (int i = 0; i < selectConditions.size(); i++) {
                condition = selectConditions.get(i);
                expressionToCode(condition.getExpression(), ifCondition);
                if (i < selectConditions.size() - 1) {
                    //operator that binds each select condition
                    ifCondition.append(condition.getOperator().getValue());
                }
            }

            writer.write(ifCondition.toString());
            writer.writeln_r("){");
            writer.write("true");
            writer.writeln_lr("}else{");
            writer.write("false");
            writer.writeln_l("}");
        }
        writer.writeln_l("}");
    }

    @VisibleForTesting
    void addIsValidFunction(RelationProcessFunction rpf, final PicoWriter writer) throws Exception {
        writer.writeln_r("override def isValid(value: Payload): Boolean = {");
        List<SelectCondition> selectConditions = rpf.getSelectConditions();
        boolean expHasOtherRelationAttributes = false;
        if (selectConditions == null) {
            writer.writeln_r("true");
        } else {
            StringBuilder ifCondition = new StringBuilder();
            ifCondition.append("if(");
            SelectCondition condition;
            for (int i = 0; i < selectConditions.size(); i++) {
                condition = selectConditions.get(i);
                Expression exp = condition.getExpression();
                if (exp.hasOtherRelationAttributes(rpf.getRelation())) {
                    expHasOtherRelationAttributes = true;
                    continue;
                }
                ifCondition.append(" (");
                expressionToCode(condition.getExpression(), ifCondition);
                ifCondition.append(") &&");
            }
            ifCondition.delete(ifCondition.length() - 2, ifCondition.length());

            writer.write(ifCondition.toString());
            writer.writeln_r("){");
            writer.write("true");
            writer.writeln_lr("}else{");
            writer.write("false");
            writer.writeln_l("}");
        }
        writer.writeln_l("}");

        if (expHasOtherRelationAttributes) {
            addIsOutputValidFunction(rpf, writer);
        }
    }

    @VisibleForTesting
    void addIsOutputValidFunction(RelationProcessFunction rpf, final PicoWriter writer) throws Exception {
        writer.writeln_r("override def isOutputValid(value: Payload): Boolean = {");
        List<SelectCondition> selectConditions = rpf.getSelectConditions();
        if (selectConditions == null) {
            writer.writeln_r("true");
        } else {
            StringBuilder ifCondition = new StringBuilder();
            ifCondition.append("if(");
            SelectCondition condition;
            for (int i = 0; i < selectConditions.size(); i++) {
                condition = selectConditions.get(i);
                Expression exp = condition.getExpression();
                if (!exp.hasOtherRelationAttributes(rpf.getRelation())) {
                    continue;
                }
                ifCondition.append(" (");
                expressionToCode(condition.getExpression(), ifCondition);
                ifCondition.append(") &&");
            }
            ifCondition.delete(ifCondition.length() - 2, ifCondition.length());

            writer.write(ifCondition.toString());
            writer.writeln_r("){");
            writer.write("true");
            writer.writeln_lr("}else{");
            writer.write("false");
            writer.writeln_l("}");
        }
        writer.writeln_l("}");
    }

    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import scala.math.Ordered.orderingToOrdered");
        writer.writeln("import org.hkust.BasedProcessFunctions." + RELATION_PROCESS_FUNCTION_IMPORT);
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import java.util.Date");
        writer.writeln("import scala.util.matching.Regex");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        //TODO: if a next key is the PK of a relation, then do not include any other column names of that relation or its children
        boolean isLeaf = relationProcessFunction.isLeaf();
        String code = "class " + className + " extends "
                + RELATION_PROCESS_FUNCTION_IMPORT + "[Any](\""
                + relationProcessFunction.getRelation().getValue() + "\","
                + (isLeaf ? "" : relationProcessFunction.getChildNodes() + ",")
                + keyListToCode(relationProcessFunction.getThisKey()) + ","
                + keyListToCode(optimizeKey(relationProcessFunction.getNextKey())) + ","
                + relationProcessFunction.isRoot()
                + (isLeaf ?
                ") {" :
                ", " + relationProcessFunction.isLast() + ") {");
        writer.writeln(code);
    }
}
