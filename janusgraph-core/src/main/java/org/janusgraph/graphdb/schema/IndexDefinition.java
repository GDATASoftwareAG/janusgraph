package org.janusgraph.graphdb.schema;

import com.google.common.collect.Maps;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;

import java.util.Arrays;
import java.util.Map;

public class IndexDefinition extends NamedSchemaElementDefinition implements SchemaElementWithPropertiesDefinition {
    private final boolean unique;
    private final boolean compositeIndex;
    private final boolean mixedIndex;
    private final boolean schemaStatusStable;
    private final Map<String, PropertyKeyDefinition> properties;

    public IndexDefinition(String name, boolean unique, boolean compositeIndex, boolean mixedIndex, boolean schemaStatusStable) {
        super(name);
        this.unique = unique;
        this.compositeIndex = compositeIndex;
        this.mixedIndex = mixedIndex;
        this.schemaStatusStable = schemaStatusStable;
        this.properties = Maps.newHashMap();
    }

    public IndexDefinition(JanusGraphIndex idx, Map<String, RelationTypeDefinition> relationTypes) {
        this(idx.name(), idx.isUnique(), idx.isCompositeIndex(), idx.isMixedIndex(),
            Arrays.stream(idx.getFieldKeys()).allMatch(t -> idx.getIndexStatus(t).isStable()));
        for (PropertyKey pk : idx.getFieldKeys()) {
            PropertyKeyDefinition pkd = (PropertyKeyDefinition) relationTypes.get(pk.name());
            properties.put(pkd.getName(), pkd);
        }
    }

    public boolean isUnique() {
        return unique;
    }

    public boolean isCompositeIndex() {
        return compositeIndex;
    }

    public boolean isMixedIndex() {
        return mixedIndex;
    }

    public boolean isStable() {
        return schemaStatusStable;
    }

    @Override
    public PropertyKeyDefinition getPropertyKey(String name) {
        return properties.get(name);
    }

    @Override
    public Iterable<PropertyKeyDefinition> getPropertyKeys() {
        return properties.values();
    }
}
