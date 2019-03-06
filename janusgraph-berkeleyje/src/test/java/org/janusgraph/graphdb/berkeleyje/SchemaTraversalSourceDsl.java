package org.janusgraph.graphdb.berkeleyje;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseVertexLabel;

public class SchemaTraversalSourceDsl extends GraphTraversalSource {
    public SchemaTraversalSourceDsl(Graph graph, TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }
    public SchemaTraversalSourceDsl(final Graph graph) {
        super(graph);
    }

    public GraphTraversal<Vertex, Vertex> vertexLabel() {
        return this.clone().V()
            .hasLabel(BaseVertexLabel.DEFAULT_VERTEXLABEL.name())
            .has(BaseKey.SchemaCategory.name(), JanusGraphSchemaCategory.VERTEXLABEL);
    }

    public GraphTraversal<Vertex, Vertex> vertexLabel(String label) {
        return this.clone().V()
            .hasLabel(BaseVertexLabel.DEFAULT_VERTEXLABEL.name())
            .has(BaseKey.SchemaCategory.name(), JanusGraphSchemaCategory.VERTEXLABEL)
            .has(BaseKey.SchemaName.name(), JanusGraphSchemaCategory.VERTEXLABEL.getSchemaName(label));
    }


    public GraphTraversal<Vertex, Vertex> propertyKey() {
        return this.clone().V()
            .hasLabel(BaseVertexLabel.DEFAULT_VERTEXLABEL.name())
            .has(BaseKey.SchemaCategory.name(), JanusGraphSchemaCategory.PROPERTYKEY);
    }

    public GraphTraversal<Vertex, Vertex> edgeLabel() {
        return this.clone().V()
            .hasLabel(BaseVertexLabel.DEFAULT_VERTEXLABEL.name())
            .has(BaseKey.SchemaCategory.name(), JanusGraphSchemaCategory.EDGELABEL);
    }

}
