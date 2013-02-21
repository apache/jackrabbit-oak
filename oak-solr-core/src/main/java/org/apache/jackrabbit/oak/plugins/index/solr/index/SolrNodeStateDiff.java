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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Solr based {@link IndexHook}
 */
class SolrNodeStateDiff implements NodeStateDiff {

    private static final Logger log = LoggerFactory.getLogger(SolrNodeStateDiff.class);

    private final Collection<SolrInputDocument> solrInputDocuments;

    private final StringBuilder deleteByIdQueryBuilder;

    private final SolrServer solrServer;

    private IOException exception;

    public SolrNodeStateDiff(SolrServer solrServer) {
        this.solrServer = solrServer;
        solrInputDocuments = new LinkedList<SolrInputDocument>();
        deleteByIdQueryBuilder = initializeDeleteQueryBuilder();
    }

    public void postProcess(NodeState state) throws IOException {
        if (exception != null) {
            throw exception;
        }
        try {
            // handle adds
            if (solrInputDocuments.size() > 0) {
                solrServer.add(solrInputDocuments);
            }
            // handle deletions
            if (deleteByIdQueryBuilder.length() > 12) {
                solrServer.deleteByQuery(deleteByIdQueryBuilder.append(")").toString());
            }

            // default to softCommit
            solrServer.commit(false, false, true);

            if (log.isDebugEnabled()) {
                log.debug(new StringBuilder("added ").append(solrInputDocuments.size()).append(" documents").toString());
            }

            // free structures
            solrInputDocuments.clear();
            deleteByIdQueryBuilder.delete(4, deleteByIdQueryBuilder.length());
        } catch (SolrServerException e) {
            try {
                if (solrServer != null) {
                    solrServer.rollback();
                }
            } catch (SolrServerException e1) {
                // do nothing
            }
            throw new IOException(e);
        }
    }

    private Collection<SolrInputDocument> docsFromState(String path, @Nonnull NodeState state) {
        List<SolrInputDocument> solrInputDocuments = new LinkedList<SolrInputDocument>();
        SolrInputDocument inputDocument = new SolrInputDocument();
        // TODO : make id field configurable
        inputDocument.addField("path_exact", path);
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
        solrInputDocuments.add(inputDocument);
        for (ChildNodeEntry childNodeEntry : state.getChildNodeEntries()) {
            solrInputDocuments.addAll(docsFromState(new StringBuilder(path).append('/').
                    append(childNodeEntry.getName()).toString(), childNodeEntry.getNodeState()));
        }

        return solrInputDocuments;
    }

    @Override
    public void propertyAdded(PropertyState after) {
        // TODO implement this
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        // TODO implement this
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        // TODO implement this
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        if (exception == null) {
            try {
                addSubtree(name, after);
            } catch (IOException e) {
                exception = e;
            }
        }
    }

    private void addSubtree(String name, NodeState after) throws IOException {
        solrInputDocuments.addAll(docsFromState(name, after));
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        if (exception == null) {
            try {
                SolrNodeStateDiff diff = new SolrNodeStateDiff(solrServer);
                after.compareAgainstBaseState(before, diff);
                diff.postProcess(after);
            } catch (IOException e) {
                exception = e;
            }
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        if (exception == null) {
            try {
                deleteSubtree(name, before);
            } catch (IOException e) {
                exception = e;
            }
        }
    }

    private void deleteSubtree(String name, NodeState before) throws IOException {
        // TODO : handle cases where default operator is AND
        for (SolrInputDocument doc : docsFromState(name, before)) {
            deleteByIdQueryBuilder.append(doc.getFieldValue("path_exact")).append(" ");
        }
    }

    private StringBuilder initializeDeleteQueryBuilder() {
        return new StringBuilder("path_exact:(");
    }

}
