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
package org.apache.jackrabbit.oak.json;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * TODO document
 */
public class JsopDiff implements NodeStateDiff {

    private final JsopBuilder jsop;

    private final BlobSerializer blobs;

    private final String path;

    private final int depth;

    private JsopDiff(
            JsopBuilder jsop, BlobSerializer blobs, String path, int depth) {
        this.jsop = jsop;
        this.blobs = blobs;

        if (path != null) {
            this.path = path;
        } else {
            this.path = "/";
        }

        if (depth >= 0) {
            this.depth = depth;
        } else {
            this.depth = Integer.MAX_VALUE;
        }
    }

    public JsopDiff(BlobSerializer blobs, String path) {
        this(new JsopBuilder(), blobs, path, Integer.MAX_VALUE);
    }

    public JsopDiff(BlobSerializer blobs) {
        this(blobs, "/");
    }

    public JsopDiff(String path, int depth) {
        this(new JsopBuilder(), new BlobSerializer(), path, depth);
    }

    public JsopDiff() {
        this("/", Integer.MAX_VALUE);
    }

    /**
     * Create the JSOP diff between {@code before} and {@code after} for
     * debugging purposes.
     * <p>
     * This method does not store binaries but returns them inlined
     * in the format <code>Blob{...}</code>, where the <code>...</code>
     * is implementation-dependent - typically the SHA256 hash of the binary.
     *
     * @param before  before node state
     * @param after  after node state
     * @return  jsop diff between {@code before} and {@code after}
     */
    public static String diffToJsop(NodeState before, NodeState after) {
        JsopDiff diff = new JsopDiff();
        after.compareAgainstBaseState(before, diff);
        return diff.toString();
    }

    //-----------------------------------------------------< NodeStateDiff >--

    @Override
    public boolean propertyAdded(PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        new JsonSerializer(jsop, blobs).serialize(after);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        new JsonSerializer(jsop, blobs).serialize(after);
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
        new JsonSerializer(jsop, blobs).serialize(after);
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        jsop.tag('-').value(buildPath(name));
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        if (depth > 0) {
            after.compareAgainstBaseState(before, new JsopDiff(
                    jsop, blobs, buildPath(name), depth - 1));
        } else {
            jsop.tag('^');
            jsop.key(buildPath(name));
            jsop.object();
            jsop.endObject();
        }
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

}