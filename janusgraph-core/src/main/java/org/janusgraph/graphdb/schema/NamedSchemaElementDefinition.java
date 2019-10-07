package org.janusgraph.graphdb.schema;

public abstract class NamedSchemaElementDefinition {

    private final String name;

    public NamedSchemaElementDefinition(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }


    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        return name.equals(((NamedSchemaElementDefinition)oth).name);
    }

    @Override
    public String toString() {
        return name;
    }

}
