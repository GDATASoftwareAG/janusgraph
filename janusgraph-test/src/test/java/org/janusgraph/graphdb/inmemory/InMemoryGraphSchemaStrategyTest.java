package org.janusgraph.graphdb.inmemory;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InMemoryGraphSchemaStrategyTest extends JanusGraphBaseTest {

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        return config.getConfiguration();
    }

    @Override
    public void clopen(Object... settings) {
        if (settings!=null && settings.length>0) {
            if (graph!=null && graph.isOpen()) {
                Preconditions.checkArgument(!graph.vertices().hasNext() &&
                    !graph.edges().hasNext(),"Graph cannot be re-initialized for InMemory since that would delete all data");
                graph.close();
            }
            Map<JanusGraphBaseTest.TestConfigOption,Object> options = validateConfigOptions(settings);
            ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
            config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
            for (Map.Entry<JanusGraphBaseTest.TestConfigOption,Object> option : options.entrySet()) {
                config.set(option.getKey().option, option.getValue(), option.getKey().umbrella);
            }
            open(config.getConfiguration());
        }
        newTx();
    }

    @Test
    public void testAddVertex(){
        mgmt.makeVertexLabel("test").make();
        finishSchema();

        graph.traversal().addV("test").next();
    }

    @Test
    public void testFailsAddVertex(){
        assertThrows(IllegalArgumentException.class, () ->
            graph.traversal().addV("test").next());
    }

    @Test
    public void testAddEdge(){
        mgmt.makeVertexLabel("vertex1").make();
        mgmt.makeEdgeLabel("edge1").make();
        finishSchema();

        graph.traversal().addV("vertex1").as("v1").addV("vertex1").addE("edge1").to("v1").next();
    }

    @Test
    public void testFailsAddEdge(){
        mgmt.makeVertexLabel("vertex1").make();
        finishSchema();

        assertThrows(IllegalArgumentException.class, () ->
            graph.traversal().addV("vertex1").as("v1").addV("vertex1").addE("edge1").to("v1").next());
    }

    @Test
    public void testAddPropertyInVertexStep() {
        mgmt.makeVertexLabel("vertex1").make();
        mgmt.makePropertyKey("property1").dataType(String.class).make();
        finishSchema();

        graph.traversal().addV("vertex1").property("property1", "test").next();

        Vertex vertex1 = graph.traversal().V().hasLabel("vertex1").next();
        assertNotNull(vertex1);
        assertEquals("test", vertex1.property("property1").value());
    }

    @Test
    public void testFailsAddPropertyInVertexStep(){
        mgmt.makeVertexLabel("vertex1").make();
        finishSchema();

        assertThrows(IllegalArgumentException.class, () ->
            graph.traversal().addV("vertex1").property("property1", "test").next());
    }

    @Test
    public void testFailsAddProperty(){
        mgmt.makeVertexLabel("vertex1").make();
        mgmt.makePropertyKey("property1").dataType(String.class).make();
        finishSchema();
        graph.traversal().addV("vertex1").property("property1", "test").next();

        assertThrows(IllegalArgumentException.class, () ->
            graph.traversal().V().has("vertex1", "property1", "test").property("property2", "").next());
    }
    @Test
    public void testAddPropertyConstraintUsingVertexIdAndProperty(){
        VertexLabel vertex1 = mgmt.makeVertexLabel("vertex1").make();
        PropertyKey property1 = mgmt.makePropertyKey("property1").dataType(String.class).make();
        mgmt.addProperties(vertex1, property1);
        finishSchema();
        Vertex vertex11 = graph.traversal().addV("vertex1").next();

        graph.traversal().V(vertex11.id()).property("property1", "test").next();
    }
}
