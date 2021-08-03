package org.hkust.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SQLToJSONWriterTest {
    public final String TEST_GENERATED_JSON_PATH = "./test.json";
    public final String TEST_INFORMATION_JSON_PATH = "./information.json";
    public SQLToJSONWriter writer;

    @Mock
    private ExportTableAliasVisitor Visitor;

    @Mock
    private HashSet<SQLSelectStatement> sqlselectstatementset;

    @Mock
    private HashSet<SQLAggregateExpr> sqlaggregateexprset;

    @Mock
    private PGSelectQueryBlock pgsqb;

    @Mock
    private SQLSelectStatement sqlselectstatemet;

    @Mock
    private SQLSelect sqlselect;

    @Mock
    private List<SQLSelectItem> sqlselectitemlist;

    @Mock
    private SQLSelectItem sqlselectitem;

    @Mock
    private SQLExpr sqlexpr;

    @Test
    public void addJoinStructureTest() {
        assertThrows(NullPointerException.class,
                () -> {
                    writer.addJoinStructure(Visitor);
                }
        );
    }

    @Test
    public void checkIfRecursiveTest() {
        sqlselectstatementset = new HashSet<>();
        sqlselectstatementset.add(sqlselectstatemet);
        sqlselectitemlist = new ArrayList<>();
        sqlselectitemlist.add(sqlselectitem);
        sqlaggregateexprset = new HashSet<>();
        try {
            ExportTableAliasVisitor v = new ExportTableAliasVisitor();
            Field selectStatementField = v.getClass().getDeclaredField("selectStatement");
            selectStatementField.setAccessible(true);
            selectStatementField.set(v, sqlselectstatementset);
            when(sqlselectstatementset.iterator().next().getSelect()).thenReturn(sqlselect);
            when((PGSelectQueryBlock) sqlselect.getQuery()).thenReturn(pgsqb);
            when(pgsqb.getSelectList()).thenReturn(sqlselectitemlist);

            when(sqlselectitem.getExpr()).thenReturn(sqlexpr);
            when(sqlexpr.toString()).thenReturn("!");
            assertNull(writer.checkIfRecursive(v));

            Field aggregationField = v.getClass().getDeclaredField("aggregation");
            aggregationField.setAccessible(true);
            aggregationField.set(v, sqlaggregateexprset);
            when(sqlexpr.toString()).thenReturn("*");
//            when(sqlaggregateexprset.isEmpty()).thenReturn(true);

            final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            System.setErr(new PrintStream(errContent));
            writer.checkIfRecursive(v);
            assertEquals("No Aggregation in Recursive queries!", errContent.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void addRelationProcessFunctionTest() {
        assertThrows(NullPointerException.class,
                () -> {
                    writer.addRelationProcessFunction(Visitor);
                }
        );
    }

    @Test
    public void addAggregationFunctionTest() {
        assertThrows(NullPointerException.class,
                () -> {
                    writer.addAggregationFunction(Visitor);
                }
        );
    }

    @Test
    public void printGeneratedJsonTest() throws Exception {
        writer.printJson();
        File generatedJsonFile = new File(TEST_GENERATED_JSON_PATH);
        assertTrue(generatedJsonFile.exists());
    }

    @Test
    public void printInformationJsonTest() throws Exception {
        writer.printJson();
        File informationJsonFile = new File(TEST_INFORMATION_JSON_PATH);
        assertTrue(informationJsonFile.exists());
    }

    @Before
    public void createSQLToJSONWriter() {
        writer = new SQLToJSONWriter(TEST_GENERATED_JSON_PATH);
    }

    @After
    public void clean_garbage() {
        deleteFile(TEST_GENERATED_JSON_PATH);
        deleteFile(TEST_INFORMATION_JSON_PATH);

        System.setErr(System.err);
    }

    private boolean deleteFile(String path) {
        File file = new File(path);
        if (file.exists()) {
            return file.delete();
        } else {
            return false;
        }
    }
}
