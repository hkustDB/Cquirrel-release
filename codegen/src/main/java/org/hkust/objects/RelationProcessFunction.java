package org.hkust.objects;

import com.google.common.collect.ImmutableSet;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class RelationProcessFunction extends ProcessFunction {
    private final String name;
    private final Relation relation;
    @Nullable
    private final String id;

    private final List<String> thisKey;
    @Nullable
    private final List<String> nextKey;

    private final int childNodes;
    private final boolean isRoot;
    private final boolean isLast;
    @Nullable
    private final Map<String, String> renaming;
    @Nullable
    private final List<SelectCondition> selectConditions;


    public RelationProcessFunction(String name, String relationName, List<String> thisKey, @Nullable List<String> nextKey, int childNodes,
                                   boolean isRoot, boolean isLast, @Nullable Map<String, String> renaming,
                                   @Nullable List<SelectCondition> selectConditions) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;
        if (childNodes < 0)
            throw new RuntimeException("Number of child nodes must be >=0, got: " + childNodes);
        CheckerUtils.checkNullOrEmpty(relationName, "relationName");
        this.relation = Relation.getRelation(relationName);
        this.childNodes = childNodes;
        this.isRoot = isRoot;
        this.isLast = isLast;
        CheckerUtils.validateNonNullNonEmpty((Collection) renaming, "renaming");
        this.renaming = renaming;
        this.selectConditions = selectConditions;
        this.id = "";
    }

    public RelationProcessFunction(String name, String relationName, List<String> thisKey, @Nullable List<String> nextKey, int childNodes,
                                   boolean isRoot, boolean isLast, @Nullable Map<String, String> renaming,
                                   @Nullable List<SelectCondition> selectConditions, @Nullable String id) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;
        if (childNodes < 0)
            throw new RuntimeException("Number of child nodes must be >=0, got: " + childNodes);
        CheckerUtils.checkNullOrEmpty(relationName, "relationName");
        this.relation = Relation.getRelation(relationName);
        this.childNodes = childNodes;
        this.isRoot = isRoot;
        this.isLast = isLast;
        CheckerUtils.validateNonNullNonEmpty((Collection) renaming, "renaming");
        this.renaming = renaming;
        this.selectConditions = selectConditions;
        this.id = id;
    }

    //Used for getStream in main
    @Override
    public Set<Attribute> getAttributeSet(RelationSchema schema) {
        Set<Attribute> result = new HashSet<>(schema.getSchema(relation).getPrimaryKey());
        if (selectConditions != null) {
            selectConditions.forEach(selectCondition -> {
                selectCondition.getExpression().getValues().forEach(
                        value -> {
                            addIfAttributeValue(result, value, schema);
                        });
            });
        }

        //this_key contains elements from this relation only
        if (thisKey != null) {
            thisKey.forEach(key -> {
                result.add(schema.getColumnAttributeByRawName(relation, key));
            });
        }

        //next_key can contain elements from other relations --> skip them
        if (nextKey != null) {
            nextKey.forEach(key -> {
                Attribute attribute = schema.getColumnAttributeByRawName(relation, key);
                if (attribute != null)
                    result.add(attribute);
            });
        }

        return ImmutableSet.copyOf(result);
    }

    @Override
    public String getName() {
        return name;
    }

    public List<String> getThisKey() {
        return thisKey;
    }

    @Nullable
    public List<String> getNextKey() {
        return nextKey;
    }

    public int getChildNodes() {
        return childNodes;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public boolean isLast() {
        return isLast;
    }

    public boolean isLeaf() {
        return getChildNodes() == 0;
    }

    public Relation getRelation() {
        return relation;
    }

    public String getId() {
        if (id == null) {
            return "";
        } else {
            return id;
        }
    }

    public String getRelationAndId() {
        return relation.getValue().toLowerCase() + getId();
    }

    @Nullable
    public Map<String, String> getRenaming() {
        return renaming;
    }

    public List<SelectCondition> getSelectConditions() {
        return selectConditions;
    }

    @Override
    public String toString() {
        return "RelationProcessFunction{" +
                "name='" + name + '\'' +
                ", thisKey=" + thisKey +
                ", nextKey=" + nextKey +
                ", childNodes=" + childNodes +
                ", isRoot=" + isRoot +
                ", isLast=" + isLast +
                ", renaming=" + renaming +
                ", selectConditions=" + selectConditions +
                '}';
    }
}
