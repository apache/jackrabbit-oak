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
package org.apache.jackrabbit.oak.kernel;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BOOLEANS;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import java.io.IOException;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * TODO document
 */
class JsopDiff implements NodeStateDiff {

    private final KernelNodeStore store;

    protected final JsopBuilder jsop;

    protected final String path;

    JsopDiff(KernelNodeStore store, JsopBuilder jsop, String path) {
        this.store = store;
        this.jsop = jsop;
        this.path = path;
    }

    JsopDiff(KernelNodeStore store) {
        this(store, new JsopBuilder(), "/");
    }

    public static void diffToJsop(
            KernelNodeStore store, NodeState before, NodeState after,
            String path, JsopBuilder jsop) {
        after.compareAgainstBaseState(before, new JsopDiff(store, jsop, path));
    }

    protected JsopDiff createChildDiff(JsopBuilder jsop, String path) {
        return new JsopDiff(store, jsop, path);
    }

    //-----------------------------------------------------< NodeStateDiff >--

    @Override
    public boolean propertyAdded(PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        toJson(after, jsop);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        toJson(after, jsop);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        jsop.tag('^').key(buildPath(before.getName())).value(null);
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        jsop.tag('+').key(buildPath(name));
        toJson(after, jsop);
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        jsop.tag('-').value(buildPath(name));
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        String path = buildPath(name);
        after.compareAgainstBaseState(before, createChildDiff(jsop, path));
        return true;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return jsop.toString();
    }

    //-----------------------------------------------------------< private >--

    protected String buildPath(String name) {
        return PathUtils.concat(path, name);
    }

    private void toJson(NodeState nodeState, JsopBuilder jsop) {
        jsop.object();
        for (PropertyState property : nodeState.getProperties()) {
            jsop.key(property.getName());
            toJson(property, jsop);
        }
        for (ChildNodeEntry child : nodeState.getChildNodeEntries()) {
            jsop.key(child.getName());
            toJson(child.getNodeState(), jsop);
        }
        jsop.endObject();
    }

    private void toJson(PropertyState propertyState, JsopBuilder jsop) {
        if (propertyState.isArray()) {
            Type<?> type = propertyState.getType();
            if (type == STRINGS || propertyState.count() > 0) {
                jsop.array();
                toJsonValue(propertyState, jsop);
                jsop.endArray();
            } else {
                jsop.value(TypeCodes.EMPTY_ARRAY
                        + PropertyType.nameFromValue(type.tag()));
            }
        } else {
            toJsonValue(propertyState, jsop);
        }
    }

    private void toJsonValue(PropertyState property, JsopBuilder jsop) {
        int type = property.getType().tag();
        switch (type) {
            case PropertyType.BOOLEAN:
                for (boolean value : property.getValue(BOOLEANS)) {
                    jsop.value(value);
                }
                break;
            case PropertyType.LONG:
                for (long value : property.getValue(LONGS)) {
                    jsop.value(value);
                }
                break;
            case PropertyType.BINARY:
                for (Blob value : property.getValue(BINARIES)) {
                    String binId = writeBlob(value);
                    jsop.value(TypeCodes.encode(type, binId));
                }
                break;
            default:
                for (String value : property.getValue(STRINGS)) {
                    if (PropertyType.STRING != type || TypeCodes.split(value) != -1) {
                        value = TypeCodes.encode(type, value);
                    }
                    jsop.value(value);
                }
                break;
        }
    }

    /**
     * Make sure {@code blob} is persisted and return the id of the persisted blob.
     * @param blob  blob to persist
     * @return  id of the persisted blob
     */
    private String writeBlob(Blob blob) {
        KernelBlob kernelBlob;
        if (blob instanceof KernelBlob) {
            kernelBlob = (KernelBlob) blob;
        } else {
            try {
                kernelBlob = store.createBlob(blob.getNewStream());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return kernelBlob.getBinaryID();
    }

}