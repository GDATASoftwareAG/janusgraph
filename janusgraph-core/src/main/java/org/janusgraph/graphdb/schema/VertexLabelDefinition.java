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
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexLabelDefinition extends SchemaElementDefinition implements SchemaElementWithPropertiesDefinition {

    private final boolean isPartitioned;
    private final boolean isStatic;
    private final Map<String, PropertyKeyDefinition> properties;

    public VertexLabelDefinition(String name, long id, boolean isPartitioned, boolean isStatic) {
        super(name, id);
        this.isPartitioned = isPartitioned;
        this.isStatic = isStatic;
        this.properties = Maps.newHashMap();
    }

    public VertexLabelDefinition(VertexLabel vl, Map<String, RelationTypeDefinition> relationTypes) {
        this(vl.name(), vl.longId(), vl.isPartitioned(), vl.isStatic());
        for (PropertyKey pk : vl.mappedProperties()) {
            PropertyKeyDefinition pkd = (PropertyKeyDefinition)relationTypes.get(pk.name());
            properties.put(pkd.getName(), pkd);
        }
    }

    public boolean isStatic() {
        return isStatic;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public boolean hasDefaultConfiguration() {
        return !isPartitioned && !isStatic;
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
