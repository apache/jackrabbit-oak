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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.model.Scalar;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.model.AbstractNodeState;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.mk.model.ScalarImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Basic {@link NodeState} implementation based on the {@link MicroKernel}
 * interface. This class makes an attempt to load data lazily.
 */
class KernelNodeState extends AbstractNodeState {

    /**
     * Maximum number of child nodes kept in memory.
     */
    static final int MAX_CHILD_NODE_NAMES = 1000;

    private final MicroKernel kernel;

    private final String path;

    private final String revision;

    private Map<String, PropertyState> properties;

    private long childNodeCount = -1;

    private Map<String, NodeState> childNodes; // TODO: WeakReference?

    public KernelNodeState(MicroKernel kernel, String path, String revision) {
        this.kernel = kernel;
        this.path = path;
        this.revision = revision;
    }

    private synchronized void init() {
        if (properties == null) {
            String json = kernel.getNodes(
                    path, revision, 0, 0, MAX_CHILD_NODE_NAMES, null);

            JsopReader reader = new JsopTokenizer(json);
            reader.read('{');
            properties = new LinkedHashMap<String, PropertyState>();
            childNodes = new LinkedHashMap<String, NodeState>();
            do {
                String name = reader.readString();
                reader.read(':');
                if (":childNodeCount".equals(name)) {
                    childNodeCount =
                            Long.valueOf(reader.read(JsopTokenizer.NUMBER));
                } else if (reader.matches('{')) {
                    reader.read('}');
                    String childPath = path + '/' + name;
                    if ("/".equals(path)) {
                        childPath = '/' + name;
                    }
                    childNodes.put(name, new KernelNodeState(
                            kernel, childPath, revision));
                } else if (reader.matches(JsopTokenizer.NUMBER)) {
                    properties.put(name, new KernelPropertyState(
                            name, ScalarImpl.numberScalar(reader.getToken())));
                } else if (reader.matches(JsopTokenizer.STRING)) {
                    properties.put(name, new KernelPropertyState(
                            name, ScalarImpl.stringScalar(reader.getToken())));
                } else if (reader.matches(JsopTokenizer.TRUE)) {
                    properties.put(name, new KernelPropertyState(
                            name, ScalarImpl.booleanScalar(true)));
                } else if (reader.matches(JsopTokenizer.FALSE)) {
                    properties.put(name, new KernelPropertyState(
                            name, ScalarImpl.booleanScalar(false)));
                } else if (reader.matches('[')) {
                    properties.put(name, new KernelPropertyState(
                            name, readArray(reader)));
                } else {
                    throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
                }
            } while (reader.matches(','));
            reader.read('}');
            reader.read(JsopTokenizer.END);
        }
    }

    @Override
    public long getPropertyCount() {
        init();
        return properties.size();
    }

    @Override
    public PropertyState getProperty(String name) {
        init();
        return properties.get(name);
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        init();
        return properties.values();
    }

    @Override
    public long getChildNodeCount() {
        init();
        return childNodeCount;
    }

    @Override
    public NodeState getChildNode(String name) {
        init();
        NodeState child = childNodes.get(name);
        if (child == null && childNodeCount > MAX_CHILD_NODE_NAMES) {
            String childPath = getChildPath(name);
            try {
                kernel.getNodes(childPath, revision, 0, 0, 0, null);
                child = new KernelNodeState(kernel, childPath, revision);
            } catch (MicroKernelException e) {
                // FIXME: Better way to determine whether a child node exists
            }
        }
        return child;
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries(
            long offset, int count) {
        init();
        if (count == -1) {
            count = Integer.MAX_VALUE;
            if (childNodeCount > count) {
                throw new RuntimeException("Too many child nodes");
            }
        }

        List<ChildNodeEntry> entries = new ArrayList<ChildNodeEntry>();

        if (offset < childNodes.size()) {
            Iterator<Map.Entry<String, NodeState>> iterator =
                    childNodes.entrySet().iterator();
            while (offset > 0) {
                iterator.next();
                offset--;
            }
            while (count > 0 && iterator.hasNext()) {
                Map.Entry<String, NodeState> entry = iterator.next();
                entries.add(new KernelChildNodeEntry(
                        entry.getKey(), entry.getValue()));
                count--;
            }
            offset = childNodes.size();
        }

        if (count > 0 && childNodeCount > MAX_CHILD_NODE_NAMES) {
            String json = kernel.getNodes(
                    path, revision, 0, offset, count, null);

            JsopReader reader = new JsopTokenizer(json);
            reader.read('{');
            do {
                String name = reader.readString();
                reader.read(':');
                if (reader.matches('{')) {
                    reader.read('}');
                    String childPath = getChildPath(name);
                    NodeState child =
                            new KernelNodeState(kernel, childPath, revision);
                    entries.add(new KernelChildNodeEntry(name, child));
                } else {
                    reader.read();
                }
            } while (reader.matches(','));
            reader.read('}');
            reader.read(JsopTokenizer.END);
        }

        return entries;
    }

    private String getChildPath(String name) {
        if ("/".equals(path)) {
            return '/' + name;
        } else {
            return path + '/' + name;
        }
    }

    private static List<Scalar> readArray(JsopReader reader) {
        List<Scalar> values = new ArrayList<Scalar>();
        while (!reader.matches(']')) {
            if (reader.matches(JsopTokenizer.NUMBER)) {
                values.add(ScalarImpl.numberScalar(reader.getToken()));
            } else if (reader.matches(JsopTokenizer.STRING)) {
                values.add(ScalarImpl.stringScalar(reader.getToken()));
            } else if (reader.matches(JsopTokenizer.TRUE)) {
                values.add(ScalarImpl.booleanScalar(true));
            } else if (reader.matches(JsopTokenizer.FALSE)) {
                values.add(ScalarImpl.booleanScalar(false));
            } else {
                throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
            }
            reader.matches(',');
        }
        return values;
    }

}