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

package org.janusgraph.graphdb.grpc.schema.util;

import com.google.protobuf.Duration;
import com.google.protobuf.Int64Value;
import com.google.protobuf.NullValue;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.NullableDuration;
import org.janusgraph.graphdb.grpc.types.VertexLabel;
import org.janusgraph.graphdb.types.VertexLabelVertex;

public class GrpcUtils {

    public static VertexLabel createVertexLabelProto(org.janusgraph.core.VertexLabel vertexLabel) {
        NullableDuration.Builder duration = NullableDuration.newBuilder();
        if (!(vertexLabel instanceof VertexLabelVertex)) {
            throw new IllegalStateException("VertexLabel should be always an instance of VertexLabelVertex");
        }
        int ttl = ((VertexLabelVertex) vertexLabel).getTTL();
        if (ttl != 0) {
            duration.setData(Duration.newBuilder().setSeconds(ttl));
        } else {
            duration.setNull(NullValue.NULL_VALUE);
        }

        return VertexLabel.newBuilder()
            .setId(Int64Value.of(vertexLabel.longId()))
            .setName(vertexLabel.name())
            .setPartitioned(vertexLabel.isPartitioned())
            .setReadOnly(vertexLabel.isStatic())
            .setTimeToLive(duration)
            .build();
    }

    public static Multiplicity convertGrpcEdgeMultiplicity(EdgeLabel.Multiplicity multiplicity) {
        switch (multiplicity) {
            case MANY2ONE:
                return Multiplicity.MANY2ONE;
            case ONE2MANY:
                return Multiplicity.ONE2MANY;
            case ONE2ONE:
                return Multiplicity.ONE2ONE;
            case SIMPLE:
                return Multiplicity.SIMPLE;
            case MULTI:
            default:
                return Multiplicity.MULTI;
        }
    }

    public static EdgeLabel.Multiplicity convertToGrpcMultiplicity(Multiplicity multiplicity) {
        switch (multiplicity) {
            case SIMPLE:
                return EdgeLabel.Multiplicity.SIMPLE;
            case ONE2MANY:
                return EdgeLabel.Multiplicity.ONE2MANY;
            case MANY2ONE:
                return EdgeLabel.Multiplicity.MANY2ONE;
            case ONE2ONE:
                return EdgeLabel.Multiplicity.ONE2ONE;
            case MULTI:
            default:
                return EdgeLabel.Multiplicity.MULTI;
        }
    }

    public static EdgeLabel createEdgeLabelProto(org.janusgraph.core.EdgeLabel edgeLabel) {
        return EdgeLabel.newBuilder()
            .setId(Int64Value.of(edgeLabel.longId()))
            .setName(edgeLabel.name())
            .setMultiplicity(convertToGrpcMultiplicity(edgeLabel.multiplicity()))
            .setDirection(edgeLabel.isDirected() ? EdgeLabel.Direction.BOTH : EdgeLabel.Direction.OUT)
            .build();
    }
}
