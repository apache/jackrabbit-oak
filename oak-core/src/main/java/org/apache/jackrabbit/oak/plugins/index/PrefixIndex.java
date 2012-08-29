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
package org.apache.jackrabbit.oak.plugins.index;

import java.io.IOException;
import java.util.Iterator;

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexDefinitionImpl;
import org.apache.jackrabbit.oak.spi.query.IndexUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * An index for all values with a given prefix.
 */
public class PrefixIndex implements PIndex {

    private final Indexer indexer;
    private final BTree tree;
    private final String prefix;

    private final IndexDefinition indexDefinition;

    public PrefixIndex(Indexer indexer, String prefix) {
        this(indexer, prefix, new IndexDefinitionImpl(prefix,
                PropertyIndexFactory.TYPE_PREFIX, PathUtils.concat(
                        IndexUtils.DEFAULT_INDEX_HOME, prefix), false, null));
    }

    public PrefixIndex(Indexer indexer, String prefix, IndexDefinition indexDefinition) {
        this.indexer = indexer;
        this.prefix = prefix;
        this.tree = new BTree(indexer, Indexer.TYPE_PREFIX + prefix, false);
        tree.setMinSize(10);
        this.indexDefinition = indexDefinition;
    }

    public static PrefixIndex fromNodeName(Indexer indexer, String nodeName) {
        if (!nodeName.startsWith(Indexer.TYPE_PREFIX)) {
            return null;
        }
        String prefix = nodeName.substring(Indexer.TYPE_PREFIX.length());
        return new PrefixIndex(indexer, prefix);
    }

    public String getPrefix() {
        return prefix;
    }

    @Override
    public IndexDefinition getDefinition() {
        return indexDefinition;
    }

    @Override
    public void addOrRemoveNode(NodeImpl node, boolean add) {
        String nodePath = node.getPath();
        for (int i = 0, size = node.getPropertyCount(); i < size; i++) {
            String propertyName = node.getProperty(i);
            String value = node.getPropertyValue(i);
            addOrRemoveProperty(nodePath, propertyName, value, add);
        }
    }

    @Override
    public void addOrRemoveProperty(String nodePath, String propertyName,
            String value, boolean add) {
        JsopTokenizer t = new JsopTokenizer(value);
        if (t.matches(JsopReader.STRING)) {
            String v = t.getToken();
            if (v.startsWith(prefix)) {
                addOrRemove(nodePath, propertyName, v, add);
            }
        } else if (t.matches('[')) {
            if (!t.matches(']')) {
                do {
                    if (t.matches(JsopReader.STRING)) {
                        String v = t.getToken();
                        if (v.startsWith(prefix)) {
                            addOrRemove(nodePath, propertyName, v, add);
                        }
                    } else if (t.matches(JsopReader.FALSE)) {
                        // ignore
                    } else if (t.matches(JsopReader.TRUE)) {
                        // ignore
                    } else if (t.matches(JsopReader.NULL)) {
                        // ignore
                    } else if (t.matches(JsopReader.NUMBER)) {
                        // ignore
                    }
                } while (t.matches(','));
                t.read(']');
            }
        }
    }

    private void addOrRemove(String path, String propertyName, String value, boolean add) {
        String v = value.substring(prefix.length());
        String p = PathUtils.concat(path,  propertyName);
        if (add) {
            tree.add(v, p);
        } else {
            tree.remove(v, p);
        }
    }

    /**
     * Get an iterator over the paths for the given property value.
     *
     * @param value the value (including the prefix)
     * @param revision the revision
     * @return an iterator of the paths (an empty iterator if not found)
     * @throws IllegalArgumentException if the value doesn't start with the prefix
     */
    @Override
    public Iterator<String> getPaths(String value, String revision) {
        if (!value.startsWith(prefix)) {
            throw new IllegalArgumentException(
                    "The value doesn't start with \"" + prefix + "\": " + value);
        }
        String v = value.substring(prefix.length());
        indexer.updateUntil(revision);
        Cursor c = tree.findFirst(v);
        return new Cursor.RangeIterator(c, v);
    }

    @Override
    public void close() throws IOException {
        // not needed
    }

    @Override
    public NodeState processCommit(NodeStore store, NodeState before,
            NodeState after) throws CommitFailedException {
        // TODO Auto-generated method stub
        return null;
    }

}
