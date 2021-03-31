// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.grpc.schema;

import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.janusgraph.graphdb.grpc.JanusGraphGrpcServerBaseTest;
import org.janusgraph.graphdb.grpc.JanusGraphManagerClient;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.NullableDuration;
import org.janusgraph.graphdb.grpc.types.VertexLabel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

public class SchemaManagerClientTest extends JanusGraphGrpcServerBaseTest {

    private static final String defaultGraphName = "graph";

    private JanusGraphContext getDefaultContext() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);
        return janusGraphManagerClient.getContextByGraphName(defaultGraphName);
    }

    @Test
    public void testGetVertexLabelByNameNotFound() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getVertexLabelByName("test"));

        assertEquals(Status.NOT_FOUND.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetVertexLabelByNameInvalidArgumentEmptyName() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getVertexLabelByName(""));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetVertexLabelByNameInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, () -> schemaManagerClient.getVertexLabelByName("test"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test", "test2" })
    public void testGetVertexLabelByNameVertexLabelExists(String vertexLabelName) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(vertexLabelName));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertEquals(vertexLabelName, vertexLabel.getName());
        assertFalse(vertexLabel.getPartitioned());
        assertFalse(vertexLabel.getReadOnly());
        assertNotEquals(0, vertexLabel.getId().getValue());
        assertTrue(vertexLabel.getTimeToLive().hasNull());
        assertFalse(vertexLabel.getTimeToLive().hasData());
    }

    @Test
    public void testGetVertexLabelByNameVertexLabelIsReadOnly() {
        final String vertexLabelName = "testReadOnly";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(vertexLabelName)
            .setReadOnly(true));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertTrue(vertexLabel.getReadOnly());
    }

    @Test
    public void testGetVertexLabelByNameVertexLabelIsPartitioned() {
        final String vertexLabelName = "testPartitioned";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(vertexLabelName)
            .setPartitioned(true));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertTrue(vertexLabel.getPartitioned());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 2000, 12})
    public void testGetVertexLabelByNameVertexLabelHasTtl(int ttlInSeconds) {
        final String vertexLabelName = "testPartitioned";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(vertexLabelName)
            .setPartitioned(true)
            .setTimeToLive(NullableDuration.newBuilder().setData(Duration.newBuilder().setSeconds(ttlInSeconds))));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertFalse(vertexLabel.getTimeToLive().hasNull());
        assertTrue(vertexLabel.getTimeToLive().hasData());
        assertEquals(ttlInSeconds, vertexLabel.getTimeToLive().getData().getSeconds());
    }

    @Test
    public void testGetEdgeLabelByNameNotFound() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getEdgeLabelByName("test"));

        assertEquals(Status.NOT_FOUND.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetEdgeLabelByNameInvalidArgumentEmptyName() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getEdgeLabelByName(""));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetEdgeLabelByNameInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, () -> schemaManagerClient.getEdgeLabelByName("test"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test", "test2" })
    public void testGetEdgeLabelByNameEdgeLabelExists(String edgeLabelName) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(edgeLabelName));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(edgeLabelName);

        assertEquals(edgeLabelName, edgeLabel.getName());
        assertEquals(EdgeLabel.Direction.BOTH, edgeLabel.getDirection());
        assertEquals(EdgeLabel.Multiplicity.MULTI, edgeLabel.getMultiplicity());
        assertNotEquals(0, edgeLabel.getId().getValue());
    }

    @ParameterizedTest
    @EnumSource(mode = EXCLUDE, names = { "UNRECOGNIZED" })
    public void testGetEdgeLabelByNameWithDefinedMultiplicity(EdgeLabel.Multiplicity multiplicity) {
        final String edgeLabelName = "testMultiplicity";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(edgeLabelName)
            .setMultiplicity(multiplicity));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(edgeLabelName);

        assertEquals(edgeLabelName, edgeLabel.getName());
        assertEquals(EdgeLabel.Direction.BOTH, edgeLabel.getDirection());
        assertEquals(multiplicity, edgeLabel.getMultiplicity());
        assertNotEquals(0, edgeLabel.getId().getValue());
    }

    @ParameterizedTest
    @EnumSource(mode = EXCLUDE, names = { "UNRECOGNIZED" })
    public void testGetEdgeLabelByNameWithDefinedDirection(EdgeLabel.Direction direction) {
        final String edgeLabelName = "testDirection";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(edgeLabelName)
            .setDirection(direction));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(edgeLabelName);

        assertEquals(edgeLabelName, edgeLabel.getName());
        assertEquals(direction, edgeLabel.getDirection());
        assertEquals(EdgeLabel.Multiplicity.MULTI, edgeLabel.getMultiplicity());
        assertNotEquals(0, edgeLabel.getId().getValue());
    }
}