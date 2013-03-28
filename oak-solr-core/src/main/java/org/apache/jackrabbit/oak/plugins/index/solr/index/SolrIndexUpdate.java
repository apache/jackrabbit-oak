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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.base.Preconditions;

/**
 * A Solr index update
 */
public class SolrIndexUpdate implements Closeable {

    private final String path;

    private final NodeBuilder index;
    private OakSolrConfiguration configuration;

    private final Map<String, NodeState> insert = new TreeMap<String, NodeState>();

    private final Set<String> remove = new TreeSet<String>();

    public SolrIndexUpdate(String path, NodeBuilder index, OakSolrConfiguration configuration) {
        this.path = path;
        this.index = index;
        this.configuration = configuration;
    }

    public void insert(String path, NodeBuilder value) {
        Preconditions.checkArgument(path.startsWith(this.path));
        String key = path.substring(this.path.length());
        if (!insert.containsKey(key)) {
            if ("".equals(key)) {
                key = "/";
            }
            if (value != null) {
                insert.put(key, value.getNodeState());
            }
        }
    }

    public void remove(String path) {
        Preconditions.checkArgument(path.startsWith(this.path));
        remove.add(path.substring(this.path.length()));
    }

    boolean getAndResetReindexFlag() {
        boolean reindex = index.getProperty("reindex") != null
                && index.getProperty("reindex").getValue(
                Type.BOOLEAN);
        index.setProperty("reindex", false);
        return reindex;
    }

    public void apply(SolrServer solrServer) throws CommitFailedException {
        if (remove.isEmpty() && insert.isEmpty()) {
            return;
        }
        try {
            for (String p : remove) {
                deleteSubtreeWriter(solrServer, p);
            }
            for (String p : insert.keySet()) {
                NodeState ns = insert.get(p);
                addSubtreeWriter(solrServer, p, ns);
            }
            OakSolrUtils.commitByPolicy(solrServer,  configuration.getCommitPolicy());
        } catch (IOException e) {
            throw new CommitFailedException(
                    "Failed to update the full text search index", e);
        } catch (SolrServerException e) {
            throw new CommitFailedException(
                    "Failed to update the full text search index", e);
        } finally {
            remove.clear();
            insert.clear();
        }
    }

    private Collection<SolrInputDocument> docsFromState(String path, @Nonnull NodeState state) {
        List<SolrInputDocument> solrInputDocuments = new LinkedList<SolrInputDocument>();
        SolrInputDocument inputDocument = docFromState(path, state);
        solrInputDocuments.add(inputDocument);
        for (ChildNodeEntry childNodeEntry : state.getChildNodeEntries()) {
            solrInputDocuments.addAll(docsFromState(new StringBuilder(path).append('/').
                    append(childNodeEntry.getName()).toString(), childNodeEntry.getNodeState()));
        }

        return solrInputDocuments;
    }

    private SolrInputDocument docFromState(String path, NodeState state) {
        SolrInputDocument inputDocument = new SolrInputDocument();
        inputDocument.addField(configuration.getPathField(), path);
        for (PropertyState propertyState : state.getProperties()) {
            // try to get the field to use for this property from configuration
            String fieldName = configuration.getFieldNameFor(propertyState.getType());
            if (fieldName != null) {
                inputDocument.addField(fieldName, propertyState.getValue(propertyState.getType()));
            } else {
                // or fallback to adding propertyName:stringValue(s)
                if (propertyState.isArray()) {
                    for (String s : propertyState.getValue(Type.STRINGS)) {
                        inputDocument.addField(propertyState.getName(), s);
                    }
                } else {
                    inputDocument.addField(propertyState.getName(), propertyState.getValue(Type.STRING));
                }
            }
        }
        return inputDocument;
    }

    private void deleteSubtreeWriter(SolrServer solrServer, String path)
            throws IOException, SolrServerException {
        // TODO verify the removal of the entire sub-hierarchy
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        solrServer.deleteByQuery(new StringBuilder(configuration.getPathField())
                .append(':').append(path).append("*").toString());
    }

    private void addSubtreeWriter(SolrServer solrServer, String path,
                                  NodeState state) throws IOException, SolrServerException {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        solrServer.add(docFromState(path, state));
    }

    @Override
    public void close() throws IOException {
        remove.clear();
        insert.clear();
    }

    @Override
    public String toString() {
        return "SolrIndexUpdate [path=" + path + ", insert=" + insert
                + ", remove=" + remove + "]";
    }
}
