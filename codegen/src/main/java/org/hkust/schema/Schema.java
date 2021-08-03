package org.hkust.schema;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Schema {
    private Map<String, Attribute> attributes;
    private List<Attribute> primaryKey;
    private Relation parent;
    private List<Relation> children;
    private Relation name;
    private String columnPrefix;

    private Schema() {
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder(new Schema());
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    public List<Attribute> getPrimaryKey() {
        return primaryKey;
    }

    public Relation getParent() {
        return parent;
    }

    private void setParent(Relation parent) {
        this.parent = parent;
    }

    private void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = attributes;
    }

    private void setPrimaryKey(List<Attribute> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public List<Relation> getChildren() {
        return children;
    }

    private void setChildren(List<Relation> children) {
        this.children = children;
    }

    public Relation getRelation() {
        return name;
    }

    private void setName(Relation name) {
        this.name = name;
    }

    public String getColumnPrefix() {
        return columnPrefix;
    }

    private void setColumnPrefix(String columnPrefix) {
        this.columnPrefix = columnPrefix;
    }

    static class SchemaBuilder {
        private Schema schema;

        private SchemaBuilder(Schema schema) {
            this.schema = schema;
        }

        SchemaBuilder withAttributes(Map<String, Attribute> attributes) {
            requireNonNull(attributes);
            schema.setAttributes(ImmutableMap.copyOf(attributes));

            return this;
        }

        SchemaBuilder withParent(Relation parent) {
            schema.setParent(parent);

            return this;
        }

        SchemaBuilder withPrimaryKey(List<Attribute> primaryKey) {
            requireNonNull(primaryKey);
            if (primaryKey.isEmpty()) {
                throw new RuntimeException("Primary key list cannot be empty");
            }
            schema.setPrimaryKey(primaryKey);

            return this;
        }

        SchemaBuilder withChildren(List<Relation> children) {
            schema.setChildren(children);
            return this;
        }

        SchemaBuilder withRelationName(Relation name){
            schema.setName(name);
            return this;
        }

        SchemaBuilder withColumnPrefix(String prefix){
            schema.setColumnPrefix(prefix);
            return this;
        }

        Schema build() {
            return schema;
        }

    }

}
