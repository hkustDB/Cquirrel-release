package org.hkust.objects;

import org.hkust.schema.Relation;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Node {
    private final List<RelationProcessFunction> relationProcessFunctions;
    private final List<AggregateProcessFunction> aggregateProcessFunctions;
    private final TransformerFunction transformerFunction;
    //Currently only supports 1 parent for each relation
    private final Map<Relation, Relation> joinStructure;

    public Node(List<RelationProcessFunction> relationProcessFunctions, List<AggregateProcessFunction> aggregateProcessFunctions, Map<Relation, Relation> joinStructure, TransformerFunction transformerFunction) {
        this.joinStructure = joinStructure;
        requireNonNull(relationProcessFunctions);
        requireNonNull(aggregateProcessFunctions);
        this.relationProcessFunctions = relationProcessFunctions;
        this.aggregateProcessFunctions = aggregateProcessFunctions;
        this.transformerFunction = transformerFunction;
    }

    public List<RelationProcessFunction> getRelationProcessFunctions() {
        return relationProcessFunctions;
    }

    public List<AggregateProcessFunction> getAggregateProcessFunctions() {
        return aggregateProcessFunctions;
    }

    @Nullable
    public Map<Relation, Relation> getJoinStructure() {
        return joinStructure;
    }

    public TransformerFunction getTransformerFunction() {
        return transformerFunction;
    }

    @Override
    public String toString() {
        return "Node{" +
                "relationProcessFunction=" + relationProcessFunctions +
                ", aggregateProcessFunction=" + aggregateProcessFunctions +
                '}';
    }
}
