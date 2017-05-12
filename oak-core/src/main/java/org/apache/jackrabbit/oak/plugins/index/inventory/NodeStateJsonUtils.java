/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.inventory;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

final class NodeStateJsonUtils {

    public static String toJson(NodeState state, boolean includeHiddenContent) {
        JsopWriter json = new JsopBuilder();
        copyAsJson(json, state, includeHiddenContent);
        return json.toString();
    }

    public static void copyAsJson(JsopWriter json, NodeState state, boolean includeHiddenContent) {
        json.object();
        copyNode(state, json, includeHiddenContent);
        json.endObject();
    }

    private static void copyNode(NodeState state, JsopWriter json, boolean includeHiddenContent) {
        copyProperties(state, json, includeHiddenContent);
        for (ChildNodeEntry cne : state.getChildNodeEntries()) {
            if (!includeHiddenContent && NodeStateUtils.isHidden(cne.getName())) {
                continue;
            }
            json.key(cne.getName());
            json.object();
            copyNode(cne.getNodeState(), json, includeHiddenContent);
            json.endObject();
        }
    }

    private static void copyProperties(NodeState state, JsopWriter json, boolean includeHiddenContent) {
        for (PropertyState ps : state.getProperties()) {
            String name = ps.getName();
            if (!includeHiddenContent && NodeStateUtils.isHidden(name)) {
                continue;
            }
            if (ps.isArray()) {
                json.key(name).array();
                for (int i = 0; i < ps.count(); i++) {
                    copyProperty(ps, i, json);
                }
                json.endArray();
            } else {
                json.key(name);
                copyProperty(ps, 0, json);
            }
        }
    }

    private static void copyProperty(PropertyState ps, int i, JsopWriter json) {
        switch (ps.getType().tag()) {
            case PropertyType.LONG:
                long longVal = ps.isArray() ? ps.getValue(Type.LONG, i) : ps.getValue(Type.LONG);
                json.value(longVal);
                break;
            case PropertyType.BOOLEAN:
                boolean boolVal = ps.isArray() ? ps.getValue(Type.BOOLEAN, i) : ps.getValue(Type.BOOLEAN);
                json.value(boolVal);
                break;
            case PropertyType.BINARY:
                Blob b = ps.isArray() ? ps.getValue(Type.BINARY, i) : ps.getValue(Type.BINARY);
                String binVal = toString(b);
                json.value(binVal);
                break;
            default:
                String strVal = ps.isArray() ? ps.getValue(Type.STRING, i) : ps.getValue(Type.STRING);
                json.value(strVal);
        }
    }

    private static String toString(Blob b) {
        String id = b.getContentIdentity();
        return id == null ? "<binary>#" + b.length() : b.getContentIdentity();
    }
}
