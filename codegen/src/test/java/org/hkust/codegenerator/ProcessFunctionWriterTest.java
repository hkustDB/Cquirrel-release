package org.hkust.codegenerator;

import org.hkust.objects.*;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProcessFunctionWriterTest {

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Mock
    private RelationProcessFunction relationProcessFunction;

    @Mock
    private Relation relation;

    @Mock
    private RelationSchema schema;

    @Before
    public void initialization() {
        MockitoAnnotations.openMocks(this);
    }

    private ProcessFunctionWriter processFunctionWriter;

    @Test
    public void expressionToCodeTest() throws Exception {
        List<Value> values = new ArrayList<>();
        //Dummy data types to test the different code flows, they don't make sense otherwise
        values.add(new ConstantValue("constant1", "date"));
        values.add(new ConstantValue("constant2", "int"));
        values.add(new AttributeValue(Relation.LINEITEM, "attributeName"));
        Expression expression = new Expression(values, Operator.AND);

        ProcessFunctionWriter processFunctionWriter = getProcessFunctionWriter();
        StringBuilder code = new StringBuilder();
        testExpressionToCode(processFunctionWriter, expression, code);
        assertEquals(
                removeAllSpaces("format.parse(\"constant1\")&&constant2&&value(\"ATTRIBUTENAME\").asInstanceOf[Integer]"),
                removeAllSpaces(code.toString())
        );

        //must get a new reference, do not clear the existing values list and reuse the same reference, will result in stack overflow
        values = new ArrayList<>();
        values.add(new ConstantValue("constant2", "int"));
        values.add(expression);
        Expression expression2 = new Expression(values, Operator.OR);
        code = new StringBuilder();
        testExpressionToCode(processFunctionWriter, expression2, code);

        assertEquals(
                removeAllSpaces("constant2||(format.parse(\"constant1\")&&constant2&&value(\"ATTRIBUTENAME\").asInstanceOf[Integer])"),
                removeAllSpaces(code.toString())
        );
    }

    @Test
    public void inValidExpression() {
        List<Value> values = new ArrayList<>();

        values.add(new ConstantValue("constant1", "date"));
        thrownException.expect(IllegalArgumentException.class);
        thrownException.expectMessage("Expression with 1 value can only have ! as the operator");
        new Expression(values, Operator.LESS_THAN);

        values.add(new ConstantValue("constant2", "int"));
        values.add(new AttributeValue(Relation.LINEITEM, "attributeName"));
        thrownException.expect(IllegalArgumentException.class);
        thrownException.expectMessage("Expression with more than 2 values can only have && or || as the operator");
        new Expression(values, Operator.LESS_THAN);

        new Expression(values, Operator.AND);
        new Expression(values, Operator.OR);
    }

    private void testExpressionToCode(ProcessFunctionWriter processFunctionWriter, Expression expression, StringBuilder code) throws Exception {
        Attribute mockAttribute = new Attribute(Integer.class, 0, "attributeName");
        when(schema.getColumnAttributeByRawName(any(), any())).thenReturn(mockAttribute);
        processFunctionWriter.expressionToCode(expression, code);
    }

    private ProcessFunctionWriter getProcessFunctionWriter() {
        when(relationProcessFunction.getName()).thenReturn("ClassName");
        return new RelationProcessFunctionWriter(relationProcessFunction, schema);
    }

    public String removeAllSpaces(String str) {
        return str.replaceAll("\\s+", "");
    }
}
