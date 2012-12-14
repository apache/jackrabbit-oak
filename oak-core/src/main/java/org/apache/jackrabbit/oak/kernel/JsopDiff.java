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

import java.io.IOException;
import java.io.InputStream;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BOOLEANS;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

class JsopDiff implements NodeStateDiff {
    private static final Logger log = LoggerFactory.getLogger(JsopDiff.class);

    private final MicroKernel kernel;

    protected final JsopBuilder jsop;

    protected final String path;

    public JsopDiff(MicroKernel kernel, JsopBuilder jsop, String path) {
        this.kernel = kernel;
        this.jsop = jsop;
        this.path = path;
    }

    public JsopDiff(MicroKernel kernel) {
        this(kernel, new JsopBuilder(), "/");
    }

    public static void diffToJsop(
            MicroKernel kernel, NodeState before, NodeState after,
            String path, JsopBuilder jsop) {
        after.compareAgainstBaseState(before, new JsopDiff(kernel, jsop, path));
    }

    protected JsopDiff createChildDiff(JsopBuilder jsop, String path) {
        return new JsopDiff(kernel, jsop, path);
    }

    //-----------------------------------------------------< NodeStateDiff >--

    @Override
    public void propertyAdded(PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        toJson(after, jsop);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        toJson(after, jsop);
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        jsop.tag('^').key(buildPath(before.getName())).value(null);
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        jsop.tag('+').key(buildPath(name));
        toJson(after, jsop);
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        jsop.tag('-').value(buildPath(name));
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        String path = buildPath(name);
        after.compareAgainstBaseState(before, createChildDiff(jsop, path));
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
            jsop.array();
            toJsonValue(propertyState, jsop);
            jsop.endArray();
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
                    String binId;
                    if (value instanceof KernelBlob) {
                        binId = ((KernelBlob) value).getBinaryID();
                    } else {
                        InputStream is = value.getNewStream();
                        binId = kernel.write(is);
                        close(is);
                    }
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

    private static void close(InputStream stream) {
        try {
            stream.close();
        }
        catch (IOException e) {
            log.warn("Error closing stream", e);
        }
    }
}