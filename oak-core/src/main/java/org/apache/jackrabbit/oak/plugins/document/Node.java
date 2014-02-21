/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;

/**
 * Represents a node held in memory (in the cache for example).
 */
public interface Node extends CacheValue {

    Children NO_CHILDREN = new Children();

    String getPropertyAsString(String propertyName);

    void setProperty(String propertyName, String value);

    void setProperty(PropertyState property);

    Set<String> getPropertyNames();

    void copyTo(Node newNode);

    boolean hasNoChildren();

    String getId();

    void append(JsopWriter json, boolean includeId);

    void setLastRevision(Revision lastRevision);

    Revision getLastRevision();

    String getPath();

    UpdateOp asOperation(boolean isNew);

    /**
     * A list of children for a node.
     */
    public static class Children implements CacheValue {

        final ArrayList<String> children = new ArrayList<String>();
        boolean hasMore;

        @Override
        public int getMemory() {
            int size = 114;
            for (String c : children) {
                size += c.length() * 2 + 56;
            }
            return size;
        }

        @Override
        public String toString() {
            return children.toString();
        }
    }
}
