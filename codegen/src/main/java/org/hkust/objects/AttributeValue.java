package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;
import org.hkust.schema.Relation;

import java.util.Objects;

public class AttributeValue implements Value {
    private final Relation relation;
    private final String columnName;

    public AttributeValue(Relation relation, String columnName) {
        CheckerUtils.checkNullOrEmpty(columnName, "name");
        this.columnName = columnName;
        this.relation = relation;
    }

    public Relation getRelation() {
        return relation;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return "AttributeValue{" +
                "name='" + columnName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AttributeValue)) return false;
        AttributeValue that = (AttributeValue) o;
        return Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName);
    }
}