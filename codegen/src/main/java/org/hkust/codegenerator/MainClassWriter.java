package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.ainslec.picocog.PicoWriter;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.objects.AggregateProcessFunction;
import org.hkust.objects.Node;
import org.hkust.objects.RelationProcessFunction;
import org.hkust.objects.TransformerFunction;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.jgrapht.alg.interfaces.ShortestPathAlgorithm.*;
import org.jgrapht.alg.shortestpath.*;

import java.util.*;

import static java.util.Objects.requireNonNull;
import static org.hkust.objects.Type.getStringConversionMethod;
import static org.jgrapht.Graphs.predecessorListOf;
import static org.jgrapht.Graphs.successorListOf;

class MainClassWriter implements ClassWriter {
    private static final String CLASS_NAME = "Job";
    private final List<AggregateProcessFunction> aggregateProcessFunctions;
    private final List<RelationProcessFunction> relationProcessFunctions;
    private final TransformerFunction transformerFunction;
    private final Map<Relation, Relation> joinStructure;
    private final String flinkInputPath;
    private final String flinkOutputPath;
    private final RelationSchema schema;
    //    private final Map<Relation, String> tagNames;
    private final Map<String, String> tagNames;
    private final Map<String, String> ACTIONS = ImmutableMap.of("Insert", "+", "Delete", "-");
    private Graph<String, DefaultEdge> relationGraph;
    private Map<String, Boolean> relationIsFullyHandled;
    private Map<RelationProcessFunction, Boolean> rpfIsHandled;
    private Boolean isFileSink = false;
    private Boolean isSocketSink = false;
    private Boolean isKafkaSink = false;


    MainClassWriter(Node node, RelationSchema schema, String flinkInputPath, String flinkOutputPath) {
        CheckerUtils.checkNullOrEmpty(flinkInputPath, "flinkInputPath");
        CheckerUtils.checkNullOrEmpty(flinkOutputPath, "flinkOutputPath");
        this.flinkInputPath = flinkInputPath;
        this.flinkOutputPath = flinkOutputPath;
        this.aggregateProcessFunctions = node.getAggregateProcessFunctions();
        this.relationProcessFunctions = node.getRelationProcessFunctions();
        this.transformerFunction = node.getTransformerFunction();
        this.joinStructure = node.getJoinStructure();
        this.schema = schema;
        this.tagNames = new HashMap<>();
        this.relationIsFullyHandled = new HashMap<>();
        this.rpfIsHandled = new HashMap<>();
        for (RelationProcessFunction rpf : relationProcessFunctions) {
//            tagNames.put(rpf.getRelation(), rpf.getRelation().getValue().toLowerCase() + "Tag");
            tagNames.put(rpf.getRelationAndId(), rpf.getRelationAndId() + "Tag");
            this.rpfIsHandled.put(rpf, false);
            this.relationIsFullyHandled.put(rpf.getRelation().getValue().toLowerCase(), false);
        }
        createRelationGraph();
    }

    MainClassWriter(Node node, RelationSchema schema, String flinkInputPath, String flinkOutputPath, String[] dataSinkTypes) {
        this(node, schema, flinkInputPath, flinkOutputPath);
        for (String sinkType : dataSinkTypes) {
            if (sinkType.equals("socket")) {
                this.isSocketSink = true;
            }
            if (sinkType.equals("kafka")) {
                this.isKafkaSink = true;
            }
            if (sinkType.equals("file")) {
                this.isFileSink = true;
            }
        }
    }

    private void createRelationGraph() {
        relationGraph = new SimpleDirectedGraph<>(DefaultEdge.class);

        HashSet<String> relationStringSet = new HashSet<>();
        for (Map.Entry<Relation, Relation> entry : joinStructure.entrySet()) {
            relationStringSet.add(entry.getKey().getValue().toLowerCase());
            relationStringSet.add(entry.getValue().getValue().toLowerCase());
        }
        for (String s : relationStringSet) {
            relationGraph.addVertex(s);
        }
        for (Map.Entry<Relation, Relation> entry : joinStructure.entrySet()) {
            relationGraph.addEdge(entry.getValue().getValue().toLowerCase(), entry.getKey().getValue().toLowerCase());
        }
    }

    @Override
    public String write(String filePath) throws Exception {
        final PicoWriter writer = new PicoWriter();

        addImports(writer);
        addConstructorAndOpenClass(writer);
        addMainFunction(writer);
        addGetStreamFunction(writer);
        closeClass(writer);
        writeClassFile(CLASS_NAME, filePath, writer.toString());

        return CLASS_NAME;
    }

    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import org.apache.flink.api.java.utils.ParameterTool");
        writer.writeln("import org.apache.flink.core.fs.FileSystem");
        writer.writeln("import org.apache.flink.streaming.api.TimeCharacteristic");
        writer.writeln("import org.apache.flink.streaming.api.scala._");
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import org.apache.flink.streaming.api.functions.ProcessFunction");
        writer.writeln("import org.apache.flink.util.Collector");
        writer.writeln("import org.apache.flink.api.common.serialization.SimpleStringSchema");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        writer.writeln_r("object " + CLASS_NAME + " {");
        relationProcessFunctions.forEach(rpf -> {
//            writer.writeln("val " + tagNames.get(rpf.getRelation()) + ": OutputTag[Payload] = OutputTag[Payload](\"" + rpf.getRelation().getValue() + "\")");
            writer.writeln("val " + tagNames.get(rpf.getRelationAndId()) + ": OutputTag[Payload] = OutputTag[Payload](\"" + rpf.getRelationAndId() + "\")");
        });
    }

    @VisibleForTesting
    void addMainFunction(final PicoWriter writer) {
        writer.writeln_r("def main(args: Array[String]) {");
        writer.writeln("val env = StreamExecutionEnvironment.getExecutionEnvironment");
        writer.writeln("val params: ParameterTool = ParameterTool.fromArgs(args)");
        writer.writeln("env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)");
        writer.writeln("var executionConfig = env.getConfig");
        writer.writeln("executionConfig.enableObjectReuse()");
        writer.writeln("val inputpath = \"" + flinkInputPath + "\"");
        writer.writeln("val outputpath = \"" + flinkOutputPath + "\"");
        writer.writeln("val inputStream : DataStream[Payload] = getStream(env,inputpath)");
        tagNames.forEach((key, value) -> writer.writeln("val " + key.toString().toLowerCase() + " : DataStream[Payload] = inputStream.getSideOutput(" + value + ")"));
        if (relationProcessFunctions.size() == 1) {
            writeSingleRelationStream(relationProcessFunctions.get(0), writer);
        } else {
            if (getLeafNumberOfRelationProcessFunctions() > 1) {
                writeMultipleRelationsStreamWithMultipleLeaves(writer);
            } else {
                RelationProcessFunction root = getLeafOrParent(true);
                writeMultipleRelationStream(root, writer, "", "S");
            }
        }
        writer.writeln("env.execute(\"Flink Streaming Scala API Skeleton\")");
        writer.writeln_l("}");
    }


    private void writeSingleRelationStream(RelationProcessFunction root, final PicoWriter writer) {
        writer.writeln("val result  = " + root.getRelation().toString().toLowerCase() + ".keyBy(i => i._3)");
        String className = getProcessFunctionClassName(root.getName());
        writer.writeln(".process(new " + className + "())");
        writer.writeln(".keyBy(i => i._3)");
        linkAggregateProcessFunctions(writer);
        writeDataSink(writer);
    }

    @NotNull
    private RelationProcessFunction getLeafOrParent(boolean leaf) {
        RelationProcessFunction relationProcessFunction = null;
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            if ((leaf ? rpf.isLeaf() : rpf.isRoot())) {
                relationProcessFunction = rpf;
            }
        }
        if (relationProcessFunction == null) {
            throw new RuntimeException("No relation process function found in " + relationProcessFunctions);
        }
        return relationProcessFunction;
    }

    private ArrayList<RelationProcessFunction> getLeavesRPFList() {
        ArrayList<RelationProcessFunction> leavesRPFList = new ArrayList<>();
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            if (rpf.isLeaf()) {
                leavesRPFList.add(rpf);
            }
        }
        return leavesRPFList;
    }

    private int getDistanceFromRoot(String root, String target) {
        DijkstraShortestPath<String, DefaultEdge> dijkstraAlg = new DijkstraShortestPath<>(relationGraph);
        SingleSourcePaths<String, DefaultEdge> iPaths = dijkstraAlg.getPaths(root);
        return iPaths.getPath(target).getLength();
    }

    private ArrayList<RelationProcessFunction> sortLeavesRPFAccordingToRootDistance(ArrayList<RelationProcessFunction> rpfList, RelationProcessFunction root) {
        HashMap<RelationProcessFunction, Integer> rpfDistanceToRoot = new HashMap<>();
        String rootStr = root.getRelation().getValue().toLowerCase();
        for (RelationProcessFunction rpf : rpfList) {
            String curRPFStr = rpf.getRelation().getValue().toLowerCase();
            rpfDistanceToRoot.put(rpf, getDistanceFromRoot(rootStr, curRPFStr));
        }
        ArrayList<RelationProcessFunction> res = new ArrayList<>();
        rpfDistanceToRoot.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(e -> res.add(e.getKey()));
        return res;
    }

    private void writeMultipleRelationStream(RelationProcessFunction rpf, final PicoWriter writer, String prevStreamName, String streamSuffix) {
        Relation relation = rpf.getRelation();
        String relationName = relation.toString().toLowerCase();
        String streamName = relationName + streamSuffix;
        if (rpf.isLeaf()) {
            writer.writeln("val " + streamName + " = " + relationName + ".keyBy(i => i._3)");
        } else {
            writer.writeln("val " + streamName + " = " + prevStreamName + ".connect(" + relationName + ")");
            writer.writeln(".keyBy(i => i._3, i => i._3)");
        }
        writer.writeln(".process(new " + getProcessFunctionClassName(rpf.getName()) + "())");
        RelationProcessFunction parent = getRelation(joinStructure.get(relation));
        if (parent == null) {
            writer.writeln("val result = " + streamName + ".keyBy(i => i._3)");
            linkAggregateProcessFunctions(writer);
            writeDataSink(writer);
            return;
        }
        writeMultipleRelationStream(requireNonNull(parent),
                writer,
                streamName,
                streamSuffix
        );
    }

    private void setRelationIsFullyHandled(String relationName, boolean bool) {
        relationIsFullyHandled.put(relationName, bool);
    }

    private Boolean getRelationIsFullyHandled(String relationName) {
        return relationIsFullyHandled.get(relationName);
    }

    private Boolean isRelationListFullyHandled(List<String> relationNameList) {
        for (String r : relationNameList) {
            if (!getRelationIsFullyHandled(r)) {
                return false;
            }
        }
        return true;
    }

    private void setRpfIsHandled(RelationProcessFunction rpf, Boolean bool) {
        rpfIsHandled.put(rpf, bool);
    }

    private Boolean getRpfIsHandled(RelationProcessFunction rpf) {
        return rpfIsHandled.get(rpf);
    }

    private void writeLeafRelationStream(RelationProcessFunction rpf, final PicoWriter writer) {
        String relationName = rpf.getRelationAndId().toLowerCase();
        String streamName = relationName + "S";
        writer.writeln("val " + streamName + " = " + relationName + ".keyBy(i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(rpf.getName()) + "())");

        setRelationIsFullyHandled(rpf.getRelation().toString().toLowerCase(), true);
        setRpfIsHandled(rpf, true);
    }

    private void writeUnLeafRelationStream(RelationProcessFunction rpfC, RelationProcessFunction rpf, final PicoWriter writer) {
        String relationNameC = rpfC.getRelationAndId().toLowerCase();
        String streamNameC = relationNameC + "S";
        String relationName = rpf.getRelationAndId().toLowerCase();
        String streamName = relationName + "S";
        writer.writeln("val " + streamName + " = " + streamNameC + ".connect(" + relationName + ")");
        writer.writeln(".keyBy(i => i._3, i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(rpf.getName()) + "())");

        List<String> rpfRelationC = successorListOf(relationGraph, rpf.getRelation().toString().toLowerCase());
        if (isRelationListFullyHandled(rpfRelationC)) {
            setRelationIsFullyHandled(rpf.getRelation().toString().toLowerCase(), true);
        }
        setRpfIsHandled(rpf, true);
    }

    private void writeBifurRelationSteam(RelationProcessFunction rpfC, RelationProcessFunction rpf, RelationProcessFunction rpfU, final PicoWriter writer) {
        String relationNameC = rpfC.getRelationAndId().toLowerCase();
        String streamNameC = relationNameC + "S";
        String relationName = rpf.getRelationAndId().toLowerCase();
        String streamName = relationName + "S";
        String relationNameU = rpfU.getRelationAndId().toLowerCase();
        String streamNameU = relationNameU + "S";
        writer.writeln("val " + streamName + " = " + streamNameC + ".connect(" + streamNameU + ")");
        writer.writeln(".keyBy(i => i._3, i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(rpf.getName()) + "())");

        List<String> rpfRelationC = successorListOf(relationGraph, rpf.getRelation().toString().toLowerCase());
        if (isRelationListFullyHandled(rpfRelationC)) {
            setRelationIsFullyHandled(rpf.getRelation().toString().toLowerCase(), true);
        }
        setRpfIsHandled(rpf, true);
    }

    private boolean hasMultipleChildRelation(String relationName) {
        List<String> relationC = successorListOf(relationGraph, relationName);
        if (relationC.size() > 1) {
            return true;
        } else {
            return false;
        }
    }

    private RelationProcessFunction getFatherRPFfromBifurParent(RelationProcessFunction rpf, List<RelationProcessFunction> rpfPList) {
        String r = Relation.getRelationAbbr(rpf.getRelation().toString().toLowerCase());
        for (RelationProcessFunction p : rpfPList) {
            if (p.getId().replaceAll("_", "").equals(r)) {
                return p;
            }
        }
        return null;
    }

    private RelationProcessFunction getUncleRPFfromBifurParent(RelationProcessFunction rpf, List<RelationProcessFunction> rpfPList) {
        String r = Relation.getRelationAbbr(rpf.getRelation().toString().toLowerCase());
        for (RelationProcessFunction p : rpfPList) {
            if (!p.getId().replaceAll("_", "").equals(r)) {
                return p;
            }
        }
        return null;
    }

    private RelationProcessFunction getSibleRPF(RelationProcessFunction rpf) {
        String relationName = rpf.getRelation().toString().toLowerCase();
        for (RelationProcessFunction r : relationProcessFunctions) {
            if (r.getRelation().toString().toLowerCase().equals(relationName)) {
                if (!r.getRelationAndId().equals(rpf.getRelationAndId())) {
                    return r;
                }
            }
        }
        return null;
    }

    private String writeLeafRelationToBifur(RelationProcessFunction rpf, final PicoWriter writer) {
        if (rpf.isLeaf()) {
            writeLeafRelationStream(rpf, writer);
        }
        do {
            String rpfRelationP = predecessorListOf(relationGraph, rpf.getRelation().toString().toLowerCase()).get(0);
            List<RelationProcessFunction> rpfPList = getRelationProcessFunctionListByRelationName(rpfRelationP);
            RelationProcessFunction rpfP = null;
            if (rpfPList.size() == 1) {
                writeUnLeafRelationStream(rpf, rpfPList.get(0), writer);
                rpfP = rpfPList.get(0);
            }
            if (rpfPList.size() == 2) {
                RelationProcessFunction rpfU = getUncleRPFfromBifurParent(rpf, rpfPList);
                RelationProcessFunction rpfF = getFatherRPFfromBifurParent(rpf, rpfPList);

                if (getRpfIsHandled(rpfU)) {
                    writeBifurRelationSteam(rpf, rpfF, rpfU, writer);
                    rpfP = rpfF;
                } else {
                    writeUnLeafRelationStream(rpf, rpfF, writer);
                    rpfP = rpfF;
                }
            }
            rpf = rpfP;
        } while (!hasMultipleChildRelation(rpf.getRelation().toString().toLowerCase()));

        RelationProcessFunction rpfS = getSibleRPF(rpf);
        if (getRpfIsHandled(rpfS)) {
            List<String> pList = predecessorListOf(relationGraph, rpf.getRelation().toString().toLowerCase());
            if (pList.size() >= 1) {
                String rpfRelationP = pList.get(0);
                List<RelationProcessFunction> rpfPList = getRelationProcessFunctionListByRelationName(rpfRelationP);
                RelationProcessFunction rpfF = getFatherRPFfromBifurParent(rpf, rpfPList);
                writeUnLeafRelationStream(rpf, rpfF, writer);
                return rpfF.getRelationAndId().toLowerCase() + "S";
            }
        }
        return rpf.getRelationAndId().toLowerCase() + "S";
    }

    private List<RelationProcessFunction> getRelationProcessFunctionListByRelationName(String relationName) {
        ArrayList<RelationProcessFunction> rpfList = new ArrayList<>();
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            if (rpf.getRelation().toString().toLowerCase().equals(relationName)) {
                rpfList.add(rpf);
            }
        }
        return rpfList;
    }

    private void writeMultipleRelationsStreamWithMultipleLeaves(final PicoWriter writer) {
        RelationProcessFunction rootRPF = getLeafOrParent(false);
        ArrayList<RelationProcessFunction> leavesRPFList = getLeavesRPFList();
        leavesRPFList = sortLeavesRPFAccordingToRootDistance(leavesRPFList, rootRPF);

        String latestStreamName = "";
        for (RelationProcessFunction rpf : leavesRPFList) {
            latestStreamName = writeLeafRelationToBifur(rpf, writer);
        }

        writer.writeln("val result = " + latestStreamName + ".keyBy(i => i._3)");
        linkAggregateProcessFunctions(writer);
        writeDataSink(writer);
    }

    private int getLeafNumberOfRelationProcessFunctions() {
        int count = 0;
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            if (rpf.isLeaf()) {
                count++;
            }
        }
        return count;
    }

    private void linkAggregateProcessFunctions(final PicoWriter writer) {
        int size = aggregateProcessFunctions.size();
        if (size == 1) {
            writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunctions.get(0).getName()) + "())");
        } else if (size == 2) {
            writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunctions.get(0).getName()) + "())");
            writer.writeln(".map(x => Payload(\"Aggregate\", \"Addition\", x._3, x._4, x._5, x._6))");
            writer.writeln(".keyBy(i => i._3)");
            writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunctions.get(1).getName()) + "())");
        } else {
            throw new RuntimeException("Currently only 1 or 2 aggregate process functions are supported");
        }
        if (transformerFunction != null) {
            writer.writeln(".keyBy(i => i._3)");
            writer.writeln(".process(new QTransformationFunction)");
        }
    }

    private void writeDataSink(final PicoWriter writer) {
        writer.writeln(".map(x => ( \"(\" + x._4.mkString(\"|\") + \"|\" + x._5.mkString(\"|\")+ \"|\" + x._6 + \")\" ))");
        if (this.isSocketSink) {
            writer.writeln("result.map(x => x.toString()).writeToSocket(\"localhost\",5001,new SimpleStringSchema()).setParallelism(1)");
        }
        if (this.isFileSink) {
            writer.writeln("result.writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE).setParallelism(1)");
        }
    }

    @Nullable
    private RelationProcessFunction getRelation(Relation relation) {
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            if (rpf.getRelation() == relation) {
                return rpf;
            }
        }
        return null;
    }

    @VisibleForTesting
    void addGetStreamFunction(final PicoWriter writer) {
        writer.writeln_r("private def getStream(env: StreamExecutionEnvironment, dataPath: String): DataStream[Payload] = {");
        writer.writeln("val data = env.readTextFile(dataPath).setParallelism(1)");
        writer.writeln("val format = new java.text.SimpleDateFormat(\"yyyy-MM-dd\")");
        writer.writeln("var cnt : Long = 0");
        writer.writeln("val restDS : DataStream[Payload] = data");
        writer.writeln(".process((value: String, ctx: ProcessFunction[String, Payload]#Context, out: Collector[Payload]) => {");
        writer.writeln("val header = value.substring(0,3)");
        writer.writeln("val cells : Array[String] = value.substring(3).split(\"\\\\|\")");
        writer.writeln("var relation = \"\"");
        writer.writeln("var action = \"\"");
        writer.writeln_r("header match {");

        Set<Attribute> attributes = new HashSet<>();
        for (RelationProcessFunction relationProcessFunction : relationProcessFunctions) {
            attributes.addAll(relationProcessFunction.getAttributeSet(schema));
        }
        attributes.addAll(aggregateProcessFunctions.get(0).getAttributeSet(schema));

        for (String relationName : relationIsFullyHandled.keySet()) {
            List<RelationProcessFunction> rpfList = getRelationProcessFunctionListByRelationName(relationName);
            RelationProcessFunction rpf = rpfList.get(0);

            Relation relation = rpf.getRelation();
            String lowerRelationName = relation.getValue();
            StringBuilder columnNamesCode = new StringBuilder();
            StringBuilder tupleCode = new StringBuilder();
            int numberOfMatchingColumns = attributeCode(rpf, attributes, columnNamesCode, tupleCode);
            String caseLabel = caseLabel(relation);
            ACTIONS.forEach((key, value) -> {
                writer.writeln("case \"" + value + caseLabel + "\" =>");
                writer.writeln("action = \"" + key + "\"");
                writer.writeln("relation = \"" + lowerRelationName + "\"");
                writer.writeln("val i = Tuple" + numberOfMatchingColumns + "(" + tupleCode.toString() + ")");
                writer.writeln("cnt = cnt + 1");
//                writer.writeln("ctx.output(" + tagNames.get(rpf.getRelation()) + ", Payload(relation, action, " + thisKeyCode(rpf));
                if (rpfList.size() == 1) {
                    writer.writeln("ctx.output(" + tagNames.get(rpf.getRelationAndId()) + ", Payload(relation, action, " + thisKeyCode(rpf));
                    writer.writeln("Array[Any](" + iteratorCode(numberOfMatchingColumns) + "),");
                    writer.writeln("Array[String](" + columnNamesCode.toString() + "), cnt))");
                } else {
                    for (RelationProcessFunction r : rpfList) {
                        writer.writeln("ctx.output(" + tagNames.get(r.getRelationAndId()) + ", Payload(relation, action, " + thisKeyCode(r));
                        writer.writeln("Array[Any](" + iteratorCode(numberOfMatchingColumns) + "),");
                        writer.writeln("Array[String](" + columnNamesCode.toString() + "), cnt))");
                    }
                }
            });
        }

        writer.writeln("case _ =>");
        writer.writeln("out.collect(Payload(\"\", \"\", 0, Array(), Array(), 0))");
        writer.writeln("}");
        writer.writeln("}).setParallelism(1)");
        writer.writeln("restDS");
        writer.writeln_l("}");
    }

    private String caseLabel(Relation relation) {
        if (relation.equals(Relation.PARTSUPP)) {
            return "PS";
        }
        return relation.getValue().substring(0, 2).toUpperCase();
    }

    private String thisKeyCode(RelationProcessFunction rpf) {
        List<String> thisKeyAttributes = rpf.getThisKey();
        int rpfThisKeySize = thisKeyAttributes.size();
        if (rpfThisKeySize == 1) {
            String thisKey = thisKeyAttributes.get(0);
            Attribute keyAttribute = schema.getColumnAttributeByRawName(rpf.getRelation(), thisKey);
            requireNonNull(keyAttribute);
            return "cells(" + keyAttribute.getPosition() + ")." + getStringConversionMethod(keyAttribute.getType()) + ".asInstanceOf[Any],";
        } else if (rpfThisKeySize == 2) {
            String thisKey1 = thisKeyAttributes.get(0);
            String thisKey2 = thisKeyAttributes.get(1);
            Attribute keyAttribute1 = schema.getColumnAttributeByRawName(rpf.getRelation(), thisKey1);
            requireNonNull(keyAttribute1);
            Attribute keyAttribute2 = schema.getColumnAttributeByRawName(rpf.getRelation(), thisKey2);
            requireNonNull(keyAttribute2);
            return "Tuple2( cells(" + keyAttribute1.getPosition() + ")." + getStringConversionMethod(keyAttribute1.getType()) +
                    ", cells(" + keyAttribute2.getPosition() + ")." + getStringConversionMethod(keyAttribute2.getType()) + ").asInstanceOf[Any],";
        } else {
            throw new RuntimeException("Expecting 1 or 2 thisKey values got: " + rpfThisKeySize);
        }
    }

    private String iteratorCode(int num) {
        StringBuilder code = new StringBuilder();
        num++;
        for (int i = 1; i < num; i++) {
            code.append("i._").append(i);
            if (i < num - 1) {
                code.append(",");
            }
        }
        return code.toString();
    }

    @VisibleForTesting
    int attributeCode(RelationProcessFunction rpf, Set<Attribute> agfAttributes, StringBuilder columnNamesCode, StringBuilder tupleCode) {
        Set<Attribute> attributes = new LinkedHashSet<>(agfAttributes);

        List<String> agfNextKeys = aggregateProcessFunctions.get(0).getOutputKey();
        if (agfNextKeys != null) {
            for (String key : agfNextKeys) {
                Attribute attribute = schema.getColumnAttributeByRawName(rpf.getRelation(), key);
                if (attribute != null) {
                    attributes.add(attribute);
                }
            }
        }

        List<String> agfThisKeys = aggregateProcessFunctions.get(0).getThisKey();
        if (agfThisKeys != null) {
            for (String key : agfThisKeys) {
                Attribute attribute = schema.getColumnAttributeByRawName(rpf.getRelation(), key);
                if (attribute != null) {
                    attributes.add(attribute);
                }
            }
        }

        Iterator<Attribute> iterator = attributes.iterator();
        int numberOfMatchingColumns = 0;
        while (iterator.hasNext()) {
            Attribute attribute = iterator.next();
            Attribute rpfAttribute = schema.getColumnAttributeByRawName(rpf.getRelation(), attribute.getName());
            if (rpfAttribute == null || !rpfAttribute.equals(attribute)) {
                if (!iterator.hasNext()) {
                    columnNamesCode.delete(columnNamesCode.length() - ",".length(), columnNamesCode.length());
                    tupleCode.delete(tupleCode.length() - ",".length(), tupleCode.length());
                }
                continue;
            }
            numberOfMatchingColumns++;


            columnNamesCode.append("\"").append(attribute.getName().toUpperCase()).append("\"");
            Class<?> type = attribute.getType();
            String conversionMethod = getStringConversionMethod(type);
            int position = attribute.getPosition();

            if (attribute.getName().equals("o_year")) {
                tupleCode.append("cells(").append(position).append(")")
                        .append(".substring(").append(attribute.getSubStrStartInd()).append(", ")
                        .append(attribute.getSubStrEndInd()).append(")")
                        .append(conversionMethod == null ? "" : "." + conversionMethod);
            } else if (!type.equals(Date.class)) {
                tupleCode.append("cells(").append(position).append(")").append(conversionMethod == null ? "" : "." + conversionMethod);
            } else {
                tupleCode.append(conversionMethod).append("(cells(").append(position).append("))");
            }

            if (iterator.hasNext()) {
                columnNamesCode.append(",");
                tupleCode.append(",");
            }
        }

        return numberOfMatchingColumns;
    }
}
