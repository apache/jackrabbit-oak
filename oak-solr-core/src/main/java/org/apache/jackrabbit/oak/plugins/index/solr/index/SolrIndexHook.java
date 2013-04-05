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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IndexHook} implementation that is responsible for keeping the
 * {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex} up to date
 * <p/>
 * This handles the status of the index update inside a flat list of {@link SolrInputDocument}s
 * for additions and a {@link StringBuilder} of ids for deletions of documents.
 * <p/>
 * Note also that at the moment this is not configurable but assumes Solr has been
 * configured ot have some specific fields and analyzers..
 *
 * @see org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex
 * @see SolrIndexDiff
 */
public class SolrIndexHook implements IndexHook, Closeable {

    private static final Logger log = LoggerFactory.getLogger(SolrNodeStateDiff.class);

    private final Collection<SolrInputDocument> solrInputDocuments;

    private final StringBuilder deleteByIdQueryBuilder;
    private final String path;
    private final String uniqueKey;

    private IOException exception;
    private final SolrServer solrServer;
    private final NodeBuilder nodebuilder;

    public SolrIndexHook(String path, NodeBuilder nodeBuilder, SolrServer solrServer) {
        this.nodebuilder = nodeBuilder;
        this.path = path;
        this.solrServer = solrServer;
        this.uniqueKey = "path_exact";
        this.solrInputDocuments = new LinkedList<SolrInputDocument>();
        this.deleteByIdQueryBuilder = initializeDeleteQueryBuilder();
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
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
        // TODO : make id field configurable
        inputDocument.addField(uniqueKey, path);
        for (PropertyState propertyState : state.getProperties()) {
            // TODO : enable selecting field from property type
            if (propertyState.isArray()) {
                for (String s : propertyState.getValue(Type.STRINGS)) {
                    inputDocument.addField(propertyState.getName(), s);
                }
            } else {
                inputDocument.addField(propertyState.getName(), propertyState.getValue(Type.STRING));
            }
        }
        return inputDocument;
    }

    @Override
    public void propertyAdded(PropertyState after) {
        solrInputDocuments.add(docFromState(getPath(), nodebuilder.getNodeState()));
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        solrInputDocuments.add(docFromState(getPath(), nodebuilder.getNodeState()));
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        solrInputDocuments.add(docFromState(getPath(), nodebuilder.getNodeState()));
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return null;
        }
        if (exception == null) {
            try {
                addSubtree(name, after);
            } catch (IOException e) {
                exception = e;
            }
        }
        return null;
    }

    private void addSubtree(String name, NodeState nodeState) throws IOException {
        solrInputDocuments.addAll(docsFromState(name, nodeState));
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return null;
        }
        return new SolrIndexHook(name, nodebuilder, solrServer);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) {
        if (NodeStateUtils.isHidden(name)) {
            return null;
        }
        if (exception == null) {
            try {
                deleteSubtree(name, before);
            } catch (IOException e) {
                exception = e;
            }
        }
        return null;
    }

    private void deleteSubtree(String name, NodeState before) throws IOException {
        // TODO : handle cases where default operator is AND
        for (SolrInputDocument doc : docsFromState(name, before)) {
            deleteByIdQueryBuilder.append(doc.getFieldValue(uniqueKey)).append(" ");
        }
    }

    private StringBuilder initializeDeleteQueryBuilder() {
        return new StringBuilder(uniqueKey).append(":(");
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (exception == null) {
            try {
                apply();
            } catch (CommitFailedException e) {
                exception = new IOException(e);
            }
        }

    }

    public void apply() throws CommitFailedException {
        try {
            if (exception != null) {
                throw exception;
            }
            boolean somethingToSend = false;
            try {
                // handle adds
                if (solrInputDocuments.size() > 0) {
                    solrServer.add(solrInputDocuments);
                    somethingToSend = true;
                }
                // handle deletions
                if (deleteByIdQueryBuilder.length() > 12) {
                    solrServer.deleteByQuery(deleteByIdQueryBuilder.append(")").toString());
                    if (!somethingToSend) {
                        somethingToSend = true;
                    }
                }

            } catch (SolrServerException e) {
                throw new IOException(e);
            }

            if (somethingToSend) {
                solrServer.commit();
            }

            if (log.isDebugEnabled()) {
                log.debug(new StringBuilder("added ").append(solrInputDocuments.size()).append(" documents").toString());
            }

        } catch (Exception e) {
            try {
                if (solrServer != null) {
                    solrServer.rollback();
                }
            } catch (Exception e1) {
                log.warn("An error occurred while rollback-ing too {}", e);
            }
            throw new CommitFailedException(e);
        }
    }

    @Override
    public Editor reindex(NodeState state) throws CommitFailedException {
        try {
            close();
            deleteByIdQueryBuilder.append(getPath()).append("*");
            solrInputDocuments.addAll(docsFromState(getPath(), state));
            apply();
        } catch (IOException e) {
            throw new CommitFailedException(e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        solrInputDocuments.clear();
        deleteByIdQueryBuilder.delete(4, deleteByIdQueryBuilder.length());
    }

    public String getPath() {
        return path;
    }
}
