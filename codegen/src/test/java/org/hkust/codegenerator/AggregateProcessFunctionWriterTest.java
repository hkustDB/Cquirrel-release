package org.hkust.codegenerator;

import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.AggregateProcessFunction;
import org.hkust.objects.AggregateValue;
import org.hkust.objects.AttributeValue;
import org.hkust.objects.Operator;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AggregateProcessFunctionWriterTest {

    @Mock
    private AggregateProcessFunction aggregateProcessFunction;

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
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addConstructorAndOpenClass(picoWriter);

        assertEquals(
                removeAllSpaces("classClassNameProcessFunctionextendsAggregateProcessFunction[Any,Integer](\"ClassNameProcessFunction\",Array(),Array(),aggregateName=\"aggregateName\",deltaOutput=true) {"),
                removeAllSpaces(picoWriter.toString())
        );
    }

    @Test
    public void aggregateFunctionTest() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addAdditionFunction(picoWriter);

        assertEquals(
                removeAllSpaces("override def addition(value1: Integer, value2: Integer): Integer = value1 + value2"),
                removeAllSpaces(picoWriter.toString())
        );
    }

    @Test
    public void additionFunctionTest() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addAdditionFunction(picoWriter);

        assertEquals(
                removeAllSpaces("override def addition(value1: Integer, value2: Integer): Integer = value1 + value2"),
                removeAllSpaces(picoWriter.toString())
        );
    }

    @Test
    public void subtractionFunctionTest() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addSubtractionFunction(picoWriter);

        assertEquals(
                removeAllSpaces("override def subtraction(value1: Integer, value2: Integer): Integer = value1 - value2"),
                removeAllSpaces(picoWriter.toString())
        );
    }

    @Test
    public void initStateFunction() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addInitStateFunction(picoWriter);

        assertEquals(
                removeAllSpaces("override def initstate(): Unit = {\n" +
                "   val valueDescriptor = TypeInformation.of(new TypeHint[Integer](){})\n" +
                "   val aliveDescriptor : ValueStateDescriptor[Integer] = new ValueStateDescriptor[Integer](\"ClassNameProcessFunction\"+\"Alive\", valueDescriptor)\n" +
                "   alive = getRuntimeContext.getState(aliveDescriptor)\n" +
                "   }\n" +
                "      override val init_value: Integer = 0"),
                removeAllSpaces(picoWriter.toString())
        );
    }

    public String removeAllSpaces(String str) {
        return str.replaceAll("\\s+", "");
    }

    private AggregateProcessFunctionWriter getAggregateProcessFunctionWriter(Class<?> aggregateType) {
        when(aggregateProcessFunction.getName()).thenReturn("ClassName");
        //when(aggregateProcessFunction.getValueType()).thenReturn(aggregateType);
        AggregateValue aggregateValue = new AggregateValue("aggregateName",
                new AttributeValue(Relation.LINEITEM, "attributeValue"), Operator.SUM, Integer.class);
        when(aggregateProcessFunction.getAggregateValues()).thenReturn(Collections.singletonList(aggregateValue));
        return new AggregateProcessFunctionWriter(aggregateProcessFunction, schema);
    }
}
