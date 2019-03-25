package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.Transaction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JanusGraphSchemaStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {

    private static final JanusGraphSchemaStrategy INSTANCE = new JanusGraphSchemaStrategy();

    private JanusGraphSchemaStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Graph graph = traversal.getGraph().get();

        //If this is a compute graph then we can't apply local traversal optimisation at this stage.
        Transaction janusGraph = (Transaction) graph;
        for (final Step step : traversal.getSteps()) {
            if (step instanceof AddVertexStartStep) {
                checkAddVertex(janusGraph, traversal, (AddVertexStartStep) step);
            } else if (step instanceof AddEdgeStep) {
                checkAddEdge(janusGraph, traversal, (AddEdgeStep) step);
            }else if (step instanceof AddPropertyStep){
                checkAddProperty(janusGraph, traversal, (AddPropertyStep) step);
            }
        }
    }

    private void checkAddProperty(Transaction janusGraph, Traversal.Admin<?, ?> traversal, AddPropertyStep step) {
        String key = step.getParameters().get(T.key, () -> "string").get(0);
        if (janusGraph.getPropertyKey(key) == null) {
            throw new IllegalArgumentException("Property Key with given name does not exist: " + key);
        }
    }

    private void checkAddEdge(Transaction janusGraph, Traversal.Admin<?,?> traversal, AddEdgeStep step) {
        String label = step.getParameters().get(T.label, () -> "string").get(0);
        if (janusGraph.getEdgeLabel(label) == null) {
            throw new IllegalArgumentException("Edge Label with given name does not exist: " + label);
        }
    }

    private void checkAddVertex(Transaction janusGraph, Traversal.Admin<?, ?> traversal, AddVertexStartStep step) {
        Parameters parameters = step.getParameters();
        String label = parameters.get(T.label, () -> "string").get(0);
        if (janusGraph.getVertexLabel(label) == null) {
            throw new IllegalArgumentException("Vertex Label with given name does not exist: " + label);
        }
        Map<Object, List<Object>> raw = parameters.getRaw(T.label);

        for (Map.Entry<Object, List<Object>> entry : raw.entrySet()) {
            String key = (String) entry.getKey();
            if (janusGraph.getPropertyKey(key) == null) {
                throw new IllegalArgumentException("Property Key with given name does not exist: " + key);
            }
        }
    }

    @Override
    public Set<Class<? extends VerificationStrategy>> applyPost() {
        return Collections.singleton(ComputerVerificationStrategy.class);
    }

    public static JanusGraphSchemaStrategy instance() {
        return INSTANCE;
    }
}
