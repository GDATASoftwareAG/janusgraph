package org.janusgraph.graphdb.schema;

public interface SchemaElementWithPropertiesDefinition {

    PropertyKeyDefinition getPropertyKey(String name);
    Iterable<PropertyKeyDefinition> getPropertyKeys();
}
