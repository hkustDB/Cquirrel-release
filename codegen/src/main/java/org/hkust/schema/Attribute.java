package org.hkust.schema;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Attribute {
    private final Class type;
    private final int position;
    private final String name;

    private final boolean isContainSubAttribute;
    private final int subStrStartInd;
    private final int subStrEndInd;

    public Attribute(Class type, int position, String name) {
        requireNonNull(type);
        if (position < 0) {
            throw new RuntimeException("position in Attribute cannot be < 0");
        }
        this.name = name;
        this.type = type;
        this.position = position;
        this.isContainSubAttribute = false;
        this.subStrStartInd = -1;
        this.subStrEndInd = -1;
    }

    public Attribute(Class type, int position, String name, boolean isContainSubAttribute, int subStrStartInd, int subStrEndInd) {
        requireNonNull(type);
        if (position < 0) {
            throw new RuntimeException("position in Attribute cannot be < 0");
        }
        this.name = name;
        this.type = type;

        this.position = position;
        this.isContainSubAttribute = isContainSubAttribute;
        this.subStrStartInd = subStrStartInd;
        this.subStrEndInd = subStrEndInd;
    }


    public static String rawColumnName(String columnName) {
        return columnName.contains("_") ? columnName.substring(2).toLowerCase() : columnName.toLowerCase();
    }

    public Class getType() {
        return type;
    }

    public int getPosition() {
        return position;
    }

    public String getName() {
        return name;
    }

    public boolean getIsContainSubAttribute() {
        return isContainSubAttribute;
    }

    public int getSubStrStartInd() {
        return subStrStartInd;
    }

    public int getSubStrEndInd() {
        return subStrEndInd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Attribute)) return false;
        Attribute attribute = (Attribute) o;
        return position == attribute.position &&
                Objects.equals(type, attribute.type) &&
                Objects.equals(name, attribute.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, position, name);
    }
}
