package org.janusgraph.graphdb.schema;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaContainerTest extends JanusGraphBaseTest {

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.SCHEMA_CONSTRAINTS, true);
        return config.getConfiguration();
    }

    @Test
    public void testGetVertexLabelWithDefaultConfig() {
        mgmt.makeVertexLabel("vertexLabel1").make();
        finishSchema();

        VertexLabelDefinition vertexLabelDefinition = new SchemaContainer(graph).getVertexLabel("vertexLabel1");

        assertEquals("vertexLabel1", vertexLabelDefinition.getName());
        assertTrue(vertexLabelDefinition.hasDefaultConfiguration());
        assertFalse(vertexLabelDefinition.isPartitioned());
        assertFalse(vertexLabelDefinition.isStatic());
    }
    @Test
    public void testGetVertexLabels() {
        mgmt.makeVertexLabel("vertexLabel1").make();
        mgmt.makeVertexLabel("vertexLabel2").make();
        finishSchema();

        Iterable<VertexLabelDefinition> vertexLabelDefinitions = new SchemaContainer(graph).getVertexLabels();

        assertEquals(2, Iterables.size(vertexLabelDefinitions));
    }

    @Test
    public void testGetVertexLabelWithPartitioned() {
        mgmt.makeVertexLabel("vertexLabel1").partition().make();
        finishSchema();

        VertexLabelDefinition vertexLabelDefinition = new SchemaContainer(graph).getVertexLabel("vertexLabel1");

        assertFalse(vertexLabelDefinition.hasDefaultConfiguration());
        assertTrue(vertexLabelDefinition.isPartitioned());
        assertFalse(vertexLabelDefinition.isStatic());
    }

    @Test
    public void testGetVertexLabelWithStatic() {
        mgmt.makeVertexLabel("vertexLabel1").setStatic().make();
        finishSchema();

        VertexLabelDefinition vertexLabelDefinition = new SchemaContainer(graph)
            .getVertexLabel("vertexLabel1");

        assertFalse(vertexLabelDefinition.hasDefaultConfiguration());
        assertTrue(vertexLabelDefinition.isStatic());
        assertFalse(vertexLabelDefinition.isPartitioned());
    }

    @Test
    public void testGetConnectedPropertyFromVertexLabel() {
        VertexLabel label = mgmt.makeVertexLabel("vertexLabel1").make();
        PropertyKey property = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        mgmt.addProperties(label, property);
        finishSchema();

        PropertyKeyDefinition propertyKeyDefinition = new SchemaContainer(graph)
            .getVertexLabel("vertexLabel1").getPropertyKey("propertyKey1");

        assertEquals("propertyKey1", propertyKeyDefinition.getName());
    }

    @Test
    public void testGetConnectedPropertiesFromVertexLabel() {
        VertexLabel label = mgmt.makeVertexLabel("vertexLabel1").make();
        PropertyKey property1 = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        PropertyKey property2 = mgmt.makePropertyKey("propertyKey2").dataType(String.class).make();
        mgmt.addProperties(label, property1, property2);
        finishSchema();

        Iterable<PropertyKeyDefinition> propertyKeyDefinitions = new SchemaContainer(graph)
            .getVertexLabel("vertexLabel1").getPropertyKeys();

        assertEquals(2, Iterables.size(propertyKeyDefinitions));
    }

    @Test
    public void testGetConnectedPropertyFromEdgeLabel() {
        EdgeLabel edge = mgmt.makeEdgeLabel("edgeLabel1").make();
        PropertyKey property = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        mgmt.addProperties(edge, property);
        finishSchema();

        PropertyKeyDefinition propertyKeyDefinition = new SchemaContainer(graph)
            .getEdgeLabel("edgeLabel1").getPropertyKey("propertyKey1");

        assertEquals("propertyKey1", propertyKeyDefinition.getName());
    }

    @Test
    public void testGetConnectedPropertiesFromEdgeLabel() {
        EdgeLabel edge = mgmt.makeEdgeLabel("edgeLabel1").make();
        PropertyKey property1 = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        PropertyKey property2 = mgmt.makePropertyKey("propertyKey2").dataType(String.class).make();
        mgmt.addProperties(edge, property1, property2);
        finishSchema();

        Iterable<PropertyKeyDefinition> propertyKeyDefinitions = new SchemaContainer(graph)
            .getEdgeLabel("edgeLabel1").getPropertyKeys();

        assertEquals(2, Iterables.size(propertyKeyDefinitions));
    }

    @Test
    public void testGetConnectedPropertiesFromCompositeIndex() {
        PropertyKey property1 = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        PropertyKey property2 = mgmt.makePropertyKey("propertyKey2").dataType(String.class).make();
        mgmt.buildIndex("index1", Vertex.class).addKey(property1).addKey(property2).buildCompositeIndex();
        finishSchema();

        Iterable<PropertyKeyDefinition> propertyKeyDefinitions = new SchemaContainer(graph)
            .getIndex("index1").getPropertyKeys();

        assertEquals(2, Iterables.size(propertyKeyDefinitions));
    }

    @Test
    public void testCompositeIndex() {
        PropertyKey property1 = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        mgmt.buildIndex("index1", Vertex.class).addKey(property1).buildCompositeIndex();
        finishSchema();

        IndexDefinition indexDefinition = new SchemaContainer(graph)
            .getIndex("index1");

        assertTrue(indexDefinition.isCompositeIndex());
        assertFalse(indexDefinition.isMixedIndex());
        assertFalse(indexDefinition.isUnique());
        assertTrue(indexDefinition.isStable());
    }

    @Test
    public void testUniqueCompositeIndex() {
        PropertyKey property1 = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        mgmt.buildIndex("index1", Vertex.class).addKey(property1).unique().buildCompositeIndex();
        finishSchema();

        IndexDefinition indexDefinition = new SchemaContainer(graph)
            .getIndex("index1");

        assertTrue(indexDefinition.isCompositeIndex());
        assertFalse(indexDefinition.isMixedIndex());
        assertTrue(indexDefinition.isUnique());
        assertTrue(indexDefinition.isStable());
    }

    @Test
    public void testNoStableIndex() {
        PropertyKey property1 = mgmt.makePropertyKey("propertyKey1").dataType(String.class).make();
        JanusGraphIndex index1 = mgmt.buildIndex("index1", Vertex.class).addKey(property1).unique().buildCompositeIndex();
        mgmt.updateIndex(index1, SchemaAction.DISABLE_INDEX);
        finishSchema();

        IndexDefinition indexDefinition = new SchemaContainer(graph)
            .getIndex("index1");

        assertFalse(indexDefinition.isStable());
    }

}
