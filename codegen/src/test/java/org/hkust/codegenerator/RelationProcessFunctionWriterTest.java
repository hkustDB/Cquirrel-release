package org.hkust.codegenerator;

import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.*;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RelationProcessFunctionWriterTest {

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

    @Test
    public void addConstructorAndOpenClassTest() {
        when(relationProcessFunction.getRelation()).thenReturn(relation);
        when(relation.getValue()).thenReturn("RelationName");
        when(relationProcessFunction.getThisKey()).thenReturn(Arrays.asList("thisKey1", "thisKey2"));
        when(relationProcessFunction.getNextKey()).thenReturn(Arrays.asList("nextKey1", "nextKey2"));
        when(relationProcessFunction.isLeaf()).thenReturn(true);
        when(relationProcessFunction.isRoot()).thenReturn(true);
        PicoWriter picoWriter = new PicoWriter();
        getRelationProcessFunctionWriter().addConstructorAndOpenClass(picoWriter);
        assertEquals(
                removeAllSpaces("class ClassNameProcessFunction extends RelationFKProcessFunction[Any](\"RelationName\",Array(\"THISKEY1\",\"THISKEY2\"),Array(\"NEXTKEY1\",\"NEXTKEY2\"),true){"),
                removeAllSpaces(picoWriter.toString())
        );
    }

    @Test
    public void isValidFunctionWithSingleCondition() throws Exception {
        List<Value> values = new ArrayList<>();
        String attributeName = "attributeValue";
        values.add(new ConstantValue("1", "int"));
        values.add(new AttributeValue(Relation.LINEITEM, attributeName));
        SelectCondition condition = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        Attribute mockAttribute = new Attribute(Integer.class, 0, attributeName);
        isValidFunctionTest(Collections.singletonList(condition), ("override def isValid(value: Payload): Boolean = {\n" +
                        "   if(1<value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]){\n" +
                        "   true}else{\n" +
                        "   false}\n" +
                        "}").replaceAll("\\s+", ""),
                mockAttribute);

        values.clear();
        values.add(new AttributeValue(Relation.LINEITEM, attributeName));
        values.add(new ConstantValue("1", "int"));
        condition = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        isValidFunctionTest(Collections.singletonList(condition), ("override def isValid(value: Payload): Boolean = {\n" +
                        "   if(value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]<1){\n" +
                        "   true}else{\n" +
                        "   false}\n" +
                        "}").replaceAll("\\s+", ""),
                mockAttribute);
    }

    @Test
    public void isValidFunctionWithMultipleConditions() throws Exception {
        List<Value> values = new ArrayList<>();
        String attributeName = "attributeValue";
        values.add(new ConstantValue("1", "int"));
        values.add(new AttributeValue(Relation.LINEITEM, attributeName));
        SelectCondition condition1 = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        SelectCondition condition2 = new SelectCondition(new Expression(values, Operator.EQUALS), Operator.AND);
        Attribute mockAttribute = new Attribute(Integer.class, 0, attributeName);
        isValidFunctionTest(Arrays.asList(condition1, condition2),
                "overridedefisValid(value:Payload):Boolean={if(1<value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]&&1==value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]){true}else{false}}",
                mockAttribute);
    }

    private void isValidFunctionTest(List<SelectCondition> selectConditions, final String expectedCode, Attribute mockAttribute) throws Exception {
        PicoWriter picoWriter = new PicoWriter();
        when(schema.getColumnAttributeByRawName(any(), any())).thenReturn(mockAttribute);
        getRelationProcessFunctionWriter().addIsValidFunction(selectConditions, picoWriter);
        assertEquals(expectedCode, removeAllSpaces(picoWriter.toString()));
    }

    private RelationProcessFunctionWriter getRelationProcessFunctionWriter() {
        when(relationProcessFunction.getName()).thenReturn("ClassName");
        return new RelationProcessFunctionWriter(relationProcessFunction, schema);
    }

    public String removeAllSpaces(String str) {
        return str.replaceAll("\\s+", "");
    }
}
