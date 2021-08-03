package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;
import org.hkust.schema.Attribute;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;


public abstract class ProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> nextKey;

    public abstract Set<Attribute> getAttributeSet(RelationSchema schema);

    public ProcessFunction(final String name, final List<String> thisKey, final List<String> nextKey) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        CheckerUtils.validateNonNullNonEmpty(thisKey, "thisKey");
        CheckerUtils.validateNonNullNonEmpty(nextKey, "nextKey");

        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;
    }

    protected void addIfAttributeValue(Set<Attribute> result, Value value, RelationSchema schema) {
        if (value instanceof Expression) {
            for (Value v : ((Expression) value).getValues()) {
                addIfAttributeValue(result, v, schema);
            }
        }
        if (value instanceof AttributeValue) {
            AttributeValue attributeValue = (AttributeValue) value;
            Attribute attribute = schema.getColumnAttributeByRawName(attributeValue.getRelation(), attributeValue.getColumnName());
            if (attribute != null) {
                result.add(attribute);
            }
        }
    }

    public abstract String getName();
}
