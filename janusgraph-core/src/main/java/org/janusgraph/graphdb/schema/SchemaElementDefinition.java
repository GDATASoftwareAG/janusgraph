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

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class SchemaElementDefinition extends NamedSchemaElementDefinition {

    private final long id;


    public SchemaElementDefinition(String name, long id) {
        super(name);
        this.id = id;
    }

    public long getLongId() {
        return id;
    }

}
