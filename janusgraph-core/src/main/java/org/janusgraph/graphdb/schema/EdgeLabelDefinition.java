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

package org.janusgraph.graphdb.schema;

import com.google.common.collect.Maps;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.PropertyKey;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class EdgeLabelDefinition extends RelationTypeDefinition implements SchemaElementWithPropertiesDefinition {

    private final boolean unidirected;
    private final Map<String, PropertyKeyDefinition> properties;

    public EdgeLabelDefinition(String name, long id, Multiplicity multiplicity, boolean unidirected) {
        super(name, id, multiplicity);
        this.unidirected = unidirected;
        this.properties = Maps.newHashMap();
    }

    public EdgeLabelDefinition(EdgeLabel el, Map<String, RelationTypeDefinition> relationTypes) {
        this(el.name(), el.longId(), el.multiplicity(), el.isUnidirected());
        for (PropertyKey pk : el.mappedProperties()) {
            PropertyKeyDefinition pkd = (PropertyKeyDefinition)relationTypes.get(pk.name());
            properties.put(pkd.getName(), pkd);
        }
    }

    public boolean isDirected() {
        return !unidirected;
    }

    public boolean isUnidirected() {
        return unidirected;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        if (unidirected) return dir == Direction.OUT;
        else return dir == Direction.BOTH;
    }

    @Override
    public PropertyKeyDefinition getPropertyKey(String name) {
        return properties.get(name);
    }

    @Override
    public Iterable<PropertyKeyDefinition> getPropertyKeys() {
        return properties.values();
    }
}
