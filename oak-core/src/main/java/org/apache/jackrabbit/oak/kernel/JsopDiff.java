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

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * TODO document
 */
public class JsopDiff implements NodeStateDiff {

    private final KernelNodeStore store;

    private final JsopBuilder jsop;

    protected final String path;

    JsopDiff(KernelNodeStore store, JsopBuilder jsop, String path) {
        this.store = store;
        this.jsop = jsop;
        this.path = path;
    }

    JsopDiff(KernelNodeStore store) {
        this(store, new JsopBuilder(), "/");
    }

    /**
     * Create the jsop diff between {@code before} and {@code after} and
     * stores binaries to {@code store}.
     *
     * @param store   node store for storing binaries.
     * @param before  before node state
     * @param after   after node state
     * @param path    base path
     * @param jsop    builder to feed in the diff
     */
    public static void diffToJsop(
            KernelNodeStore store, NodeState before, NodeState after,
            String path, JsopBuilder jsop) {
        after.compareAgainstBaseState(before, new JsopDiff(store, jsop, path));
    }

    /**
     * Create the jsop diff between {@code before} and {@code after} for
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
        class ToStringDiff extends JsopDiff {
            public ToStringDiff() {
                super(null);
            }

            public ToStringDiff(JsopBuilder jsop, String path) {
                super(null, jsop, path);
            }

            @Override
            protected String writeBlob(Blob blob) {
                return "Blob{" + blob + '}';
            }

            @Override
            protected JsopDiff createChildDiff(JsopBuilder jsop, String path) {
                return new ToStringDiff(jsop, path);
            }
        }

        JsopDiff diff = new ToStringDiff();
        after.compareAgainstBaseState(before, diff);
        return diff.toString();
    }

    protected JsopDiff createChildDiff(JsopBuilder jsop, String path) {
        return new JsopDiff(store, jsop, path);
    }

    //-----------------------------------------------------< NodeStateDiff >--

    @Override
    public boolean propertyAdded(PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        new DiffJsonSerializer().serialize(after);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        jsop.tag('^').key(buildPath(after.getName()));
        new DiffJsonSerializer().serialize(after);
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
        new DiffJsonSerializer().serialize(after);
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

    /**
     * Make sure {@code blob} is persisted and return the id of
     * the persisted blob.
     *
     * @param blob  blob to persist
     * @return  id of the persisted blob
     */
    protected String writeBlob(Blob blob) {
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

    private class DiffJsonSerializer extends JsonSerializer {

        DiffJsonSerializer() {
            super(jsop);
        }

        @Override
        protected JsonSerializer getChildSerializer() {
            return new DiffJsonSerializer();
        }

        /**
         * Make sure {@code blob} is persisted and return the id of
         * the persisted blob.
         *
         * @param blob  blob to persist
         * @return  id of the persisted blob
         */
        @Override
        protected String getBlobId(Blob blob) {
            return writeBlob(blob);
        }

    }

}