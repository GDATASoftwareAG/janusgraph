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

package org.janusgraph.graphdb.query.condition;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.schema.JanusGraphSchemaElement;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.types.system.SystemRelationType;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Evaluates elements based on their visibility
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VisibilityFilterCondition<E extends JanusGraphElement> extends Literal<E> {

    public enum Visibility { NORMAL, SYSTEM }

    private final Visibility visibility;

    public VisibilityFilterCondition(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public boolean evaluate(E element) {
        switch(visibility) {
            case NORMAL: return true;
            case SYSTEM: return (element instanceof JanusGraphRelation &&
                                    ((JanusGraphRelation)element).getType() instanceof SystemRelationType)
                    || (element instanceof JanusGraphVertex && element instanceof JanusGraphSchemaElement);
            default: throw new AssertionError("Unrecognized visibility: " + visibility);
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(visibility).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other));

    }

    @Override
    public String toString() {
        return "visibility:"+visibility.toString().toLowerCase();
    }
}
