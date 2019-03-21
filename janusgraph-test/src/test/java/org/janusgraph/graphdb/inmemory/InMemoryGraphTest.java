// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.inmemory;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class InMemoryGraphTest extends JanusGraphTest {

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
            Map<TestConfigOption,Object> options = validateConfigOptions(settings);
            ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
            config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
            for (Map.Entry<TestConfigOption,Object> option : options.entrySet()) {
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

    @Override @Test @Disabled
    public void testLocalGraphConfiguration() {}

    @Override @Test @Disabled
    public void testMaskableGraphConfig() {}

    @Override @Test @Disabled
    public void testGlobalGraphConfig() {}

    @Override @Test @Disabled
    public void testGlobalOfflineGraphConfig() {}

    @Override @Test @Disabled
    public void testFixedGraphConfig() {}

    @Override @Test @Disabled
    public void testManagedOptionMasking() {}

    @Override @Test @Disabled
    public void testTransactionConfiguration() {}

    @Override @Test @Disabled
    public void testTinkerPopOptimizationStrategies() {}

    @Override @Test @Disabled
    public void testDataTypes() {}

    @Override @Test @Disabled
    public void testForceIndexUsage() {}

    @Override @Test @Disabled
    public void testAutomaticTypeCreation() {}

    @Override @Test @Disabled
    public void simpleLogTest() {}

    @Override @Test @Disabled
    public void simpleLogTestWithFailure() {}

    @Override @Test @Disabled
    public void testIndexUpdatesWithReindexAndRemove() {}

    @Override @Test @Disabled
    public void testIndexUpdateSyncWithMultipleInstances() {}

    @Override @Test @Disabled
    public void testClearStorage() {}

    @Override @Test @Disabled
    public void testAutoSchemaMakerForEdgePropertyConstraints() {}

    @Override @Test @Disabled
    public void testAutoSchemaMakerForVertexPropertyConstraints() {}

    @Override @Test @Disabled
    public void testAutoSchemaMakerForConnectionConstraints() {}

    @Override @Test @Disabled
    public void testSupportDirectCommitOfSchemaChangesForVertexProperties() {}

    @Override @Test @Disabled
    public void testSupportDirectCommitOfSchemaChangesForConnection() {}

    @Override @Test @Disabled
    public void testSupportDirectCommitOfSchemaChangesForEdgeProperties() {}
}
