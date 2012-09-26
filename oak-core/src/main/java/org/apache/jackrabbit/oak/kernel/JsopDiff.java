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

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

class JsopDiff implements NodeStateDiff {

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
            for (CoreValue value : propertyState.getValues()) {
                toJson(value, jsop);
            }
            jsop.endArray();
        } else {
            toJson(propertyState.getValue(), jsop);
        }
    }

    private void toJson(CoreValue value, JsopBuilder jsop) {
        int type = value.getType();
        if (type == PropertyType.BOOLEAN) {
            jsop.value(value.getBoolean());
        } else if (type == PropertyType.LONG) {
            jsop.value(value.getLong());
        } else {
            String string;
            if (type == PropertyType.BINARY
                    && !(value instanceof BinaryValue)) {
                string = kernel.write(value.getNewStream());
            } else {
                string = value.getString();
            }
            if (type != PropertyType.STRING
                    || CoreValueMapper.startsWithHint(string)) {
                string = CoreValueMapper.getHintForType(type) + ':' + string;
            }
            jsop.value(string);
        }
    }
}