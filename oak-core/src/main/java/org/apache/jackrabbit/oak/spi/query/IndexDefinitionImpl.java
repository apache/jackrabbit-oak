/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IndexDefinitionImpl implements IndexDefinition {

    private final String name;
    private final String type;
    private final String path;
    private final boolean unique;
    private final Map<String, String> properties;

    public IndexDefinitionImpl(String name, String type, String path) {
        this(name, type, path, false, Collections.<String, String> emptyMap());
    }

    public IndexDefinitionImpl(String name, String type, String path,
            boolean unique, Map<String, String> properties) {
        this.name = name;
        this.type = type;
        this.path = path;
        this.unique = unique;
        if (properties != null) {
            this.properties = properties;
        } else {
            this.properties = new HashMap<String, String>();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "IndexDefinitionImpl [name=" + name + ", type=" + type
                + ", path=" + path + ", unique=" + unique + ", properties="
                + properties + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result
                + ((properties == null) ? 0 : properties.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + (unique ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IndexDefinitionImpl other = (IndexDefinitionImpl) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (properties == null) {
            if (other.properties != null)
                return false;
        } else if (!properties.equals(other.properties))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        if (unique != other.unique)
            return false;
        return true;
    }
}
