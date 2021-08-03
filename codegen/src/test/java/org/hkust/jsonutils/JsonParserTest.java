package org.hkust.jsonutils;

import org.hkust.objects.*;
import org.hkust.schema.Relation;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class JsonParserTest {

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Mock
    public Map<String, Object> mockMap;

    @Before
    public void initialization() {
        MockitoAnnotations.openMocks(this);
    }


    @Test
    public void makeRelationProcessFunctionTest() {
        Mockito.when(mockMap.get("name")).thenReturn("RelationProcessFunction");
        Mockito.when(mockMap.get("relation")).thenReturn("lineitem");
        Mockito.when(mockMap.get("this_key")).thenReturn(Collections.singletonList("this_key"));
        Mockito.when(mockMap.get("next_key")).thenReturn(Collections.singletonList("next_key"));
        Mockito.when(mockMap.get("child_nodes")).thenReturn(1.0);
        Mockito.when(mockMap.get("is_Root")).thenReturn(true);
        Mockito.when(mockMap.get("is_Last")).thenReturn(false);
        Mockito.when(mockMap.get("rename_attribute")).thenReturn(null);
        List<SelectCondition> selectConditions = Collections.singletonList(new SelectCondition(
                getExpression(),
                Operator.AND
        ));

        requireNonNull(JsonParser.makeRelationProcessFunction(mockMap, selectConditions));
    }

    @Test
    public void makeAggregateProcessFunctionTest() {
        Mockito.when(mockMap.get("name")).thenReturn("AggregateProcessFunction");
        Mockito.when(mockMap.get("this_key")).thenReturn(Collections.singletonList("this_key"));
        Mockito.when(mockMap.get("next_key")).thenReturn(Collections.singletonList("next_key"));
        Mockito.when(mockMap.get("aggregation")).thenReturn("*");
        Mockito.when(mockMap.get("value_type")).thenReturn("Double");
        List<AggregateValue> aggregateValues = Collections.singletonList(
                new AggregateValue("AggregateValue",
                        new AttributeValue(Relation.LINEITEM, "attributeValue"), Operator.SUM, Double.class)
        );
        requireNonNull(JsonParser.makeAggregateProcessFunction(mockMap, aggregateValues));
    }


    @Test
    public void makeAggregateValueTest() {
        Mockito.when(mockMap.get("aggregation")).thenReturn("+");
        Mockito.when(mockMap.get("value_type")).thenReturn("double");
        Mockito.when(mockMap.get("name")).thenReturn("revenue");

        AggregateValue aggregateValue = JsonParser.makeAggregateValue(mockMap,
                new Expression(
                        Arrays.asList(
                                new AttributeValue(Relation.LINEITEM, "l_discount"),
                                new ConstantValue("1", "int")),
                        Operator.SUM));
        Value value = aggregateValue.getValue();
        assertTrue(value instanceof Expression);
        Expression expression = (Expression) value;
        assertEquals(expression.getValues().size(), 2);
    }

    @Test
    public void makeAggregateValueExceptionsTest() {
        Mockito.when(mockMap.get("aggregation")).thenReturn("#");
        thrownException.expect(IllegalArgumentException.class);
        thrownException.expectMessage("Got #");
        JsonParser.makeAggregateValue(mockMap, null);

        Mockito.when(mockMap.get("aggregation")).thenReturn("!");
        Mockito.when(mockMap.get("value_type")).thenReturn("symbol");
        thrownException.expect(IllegalArgumentException.class);
        thrownException.expectMessage("Non supported value type!");
    }

    /*@Test
    public void makeSelectConditionsTest() {
        Mockito.when(mockMap.get("operator")).thenReturn("<");
        List<SelectCondition> result = JsonParser.makeSelectConditions(mockMap, Collections.singletonList(getExpression()));
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getExpression().getValues().size(), 2);
    }*/

    @Test
    public void makeSelectConditionsExpressionsTest() {
        List<Expression> result = JsonParser.makeSelectConditionsExpressions(Arrays.asList(
                getValue(),
                getValue()
        ));

        assertEquals(result.size(), 2);
        assertEquals(result.get(0).getValues().size(), 2);
        assertEquals(result.get(1).getValues().size(), 2);
        assertEquals(result.get(0).getValues(), result.get(1).getValues());
    }

    @Test
    public void informatinJsonFilePathTest() {
        Path p = Paths.get("./input.json");
        Path q = Paths.get("output.json");
        assertEquals("./output.json", p.getParent().resolve(q).toString());
    }

    @NotNull
    private Map<String, Object> getValue() {
        Map<String, Object> map = Mockito.mock(Map.class);
        Mockito.when(map.get("operator")).thenReturn("<");
        Mockito.when(map.get("left_field")).thenReturn(new HashMap<String, Object>() {
            {
                put("type", "attribute");
                put("relation", "lineitem");
                put("name", "attributeName");

            }
        });

        Mockito.when(map.get("right_field")).thenReturn(new HashMap<String, Object>() {
            {
                put("type", "constant");
                put("value", "0.07");
                put("var_type", "Double");

            }
        });

        return map;
    }

    @NotNull
    private Expression getExpression() {
        return new Expression(Arrays.asList(new AttributeValue(Relation.LINEITEM, "attributeValue1"), new AttributeValue(Relation.LINEITEM, "attributeValue2")), Operator.AND);
    }
}
