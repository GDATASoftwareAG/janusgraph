package org.janusgraph.graphdb.berkeleyje;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;

@GremlinDsl(traversalSource = "org.janusgraph.graphdb.berkeleyje.SchemaTraversalSourceDsl")
public interface SchemaTraversalDsl<S, E> extends GraphTraversal.Admin<S, E> {
    public default GraphTraversal<S, Vertex> mappedProperties() {
        return out(BaseLabel.SchemaDefinitionEdge.name());
    }

    public default GraphTraversal<S, String> getSchemaName() {
        return values(BaseKey.SchemaName.name()).unfold();
    }
}
