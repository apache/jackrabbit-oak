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
package org.apache.jackrabbit.oak.jcr.state;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.jcr.json.JsonHandler;
import org.apache.jackrabbit.oak.jcr.json.JsonParser;
import org.apache.jackrabbit.oak.jcr.json.JsonTokenizer;
import org.apache.jackrabbit.oak.jcr.json.JsonValue;
import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonArray;
import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonAtom;
import org.apache.jackrabbit.oak.jcr.json.Token;
import org.apache.jackrabbit.oak.jcr.json.UnescapingJsonTokenizer;
import org.apache.jackrabbit.oak.jcr.util.Function0;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.apache.jackrabbit.oak.model.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.model.AbstractNodeState;
import org.apache.jackrabbit.oak.model.ChildNodeEntry;
import org.apache.jackrabbit.oak.model.NodeState;
import org.apache.jackrabbit.oak.model.PropertyState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code NodeState} implementation on top of a {@code MicroKernel}.
 * todo this class should probably go into oak-core
 */
public class PersistentNodeState extends AbstractNodeState {
    private final MicroKernel microkernel;
    private final String revision;
    private final Path path;

    /**
     * Create a new {@code NodeState} instance for the given {@code path} and {@code revision}.
     *
     * @param microkernel
     * @param path
     * @param revision
     */
    public PersistentNodeState(MicroKernel microkernel, Path path, String revision) {
        this.microkernel = microkernel;
        this.path = path;
        this.revision = revision;
    }

    private final Function0<Map<String, PropertyStateImpl>> properties =
        new Function0<Map<String, PropertyStateImpl>>() {
            private Map<String, PropertyStateImpl> properties;

            @Override
            public Map<String, PropertyStateImpl> apply() {
                if (properties == null) {
                    properties = readProperties();
                }
                return properties;
            }
    };

    @Override
    public PropertyState getProperty(String name) {
        return properties.apply().get(name);
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return properties.apply().values();
    }

    @Override
    public NodeState getChildNode(String name) {
        if (microkernel.nodeExists(name, revision)) {
            return new PersistentNodeState(microkernel, path.concat(name), revision);
        }
        else {
            return null;
        }
    }

    @Override
    public long getChildNodeCount() {
        JsonValue count = properties.apply().get(":childNodeCount").getValue();
        long c = toLong(count);
        if (c < 0) {
            return super.getChildNodeCount();
        }
        else {
            return c;
        }
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries(final long offset, final int count) {
        String json = microkernel.getNodes(path.toMkPath(), revision, 1, offset, count, null);
        final List<ChildNodeEntry> childNodeEntries = new ArrayList<ChildNodeEntry>();

        new JsonParser(new JsonHandler(){
            @Override
            public void object(JsonParser parser, Token key, JsonTokenizer tokenizer) {
                super.object(parser, key, tokenizer);
                childNodeEntries.add(createChildNodeEntry(key.text()));
            }
        }).parseObject(new UnescapingJsonTokenizer(json));

        return childNodeEntries;
    }

    //------------------------------------------< private >---

    private ChildNodeEntry createChildNodeEntry(final String name) {
        return new AbstractChildNodeEntry() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public NodeState getNode() {
                return new PersistentNodeState(microkernel, path.concat(name), revision);
            }

            @Override
            public String toString() {
                return "ChildNodeEntry(" + name + ')';
            }
        };
    }

    private static long toLong(JsonValue count) {
        if (count == null || !count.isAtom()) {
            return -1;
        }
        else {
            try {
                return Long.parseLong(count.asAtom().value());
            }
            catch (NumberFormatException e) {
                return -1;
            }
        }
    }

    private Map<String, PropertyStateImpl> readProperties() {
        String json = microkernel.getNodes(path.toMkPath(), revision, 0, 0, -1, null);
        final Map<String, PropertyStateImpl> properties = new HashMap<String, PropertyStateImpl>();

        new JsonParser(new JsonHandler(){
            JsonArray multiValue;

            @Override
            public void atom(Token key, Token value) {
                if (multiValue == null) {
                    properties.put(key.text(), new PropertyStateImpl(key.text(), new JsonAtom(value)));
                }
                else {
                    multiValue.add(new JsonAtom(value));
                }
            }

            @Override
            public void array(JsonParser parser, Token key, JsonTokenizer tokenizer) {
                multiValue = new JsonArray();
                super.array(parser, key, tokenizer);
                properties.put(key.text(), new PropertyStateImpl(key.text(), multiValue));
                multiValue = null;
            }
        }).parseObject(new UnescapingJsonTokenizer(json));

        return properties;
    }

}
