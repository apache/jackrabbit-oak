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
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.util.OakSolrUtils;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * Index editor for keeping a Solr index up to date.
 */
public class SolrIndexEditor implements IndexEditor {

    /** Parent editor, or {@code null} if this is the root editor. */
    private final SolrIndexEditor parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /** Index definition node builder */
    private final NodeBuilder definition;

    private final SolrServer solrServer;

    private final OakSolrConfiguration configuration;

    private boolean propertiesChanged = false;

    private final IndexUpdateCallback updateCallback;

    SolrIndexEditor(
            NodeBuilder definition, SolrServer solrServer,
            OakSolrConfiguration configuration,
            IndexUpdateCallback callback) throws CommitFailedException {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.definition = definition;
        this.solrServer = solrServer;
        this.configuration = configuration;
        this.updateCallback = callback;
    }

    private SolrIndexEditor(SolrIndexEditor parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.definition = parent.definition;
        this.solrServer = parent.solrServer;
        this.configuration = parent.configuration;
        this.updateCallback = parent.updateCallback;
    }

    public String getPath() {
        if (path == null) { // => parent != null
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    @Override
    public void enter(NodeState before, NodeState after) {
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (propertiesChanged || !before.exists()) {
            updateCallback.indexUpdate();
            try {
                solrServer.add(docFromState(after));
            } catch (SolrServerException e) {
                throw new CommitFailedException(
                        "Solr", 2, "Failed to add a document to Solr", e);
            } catch (IOException e) {
                throw new CommitFailedException(
                        "Solr", 6, "Failed to send data to Solr", e);
            }
        }

        if (parent == null) {
            try {
                OakSolrUtils.commitByPolicy(
                        solrServer,  configuration.getCommitPolicy());
            } catch (SolrServerException e) {
                throw new CommitFailedException(
                        "Solr", 3, "Failed to commit changes to Solr", e);
            } catch (IOException e) {
                throw new CommitFailedException(
                        "Solr", 6, "Failed to send data to Solr", e);
            }
        }
    }

    @Override
    public void propertyAdded(PropertyState after) {
        propertiesChanged = true;
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        propertiesChanged = true;
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        propertiesChanged = true;
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        return new SolrIndexEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        return new SolrIndexEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        // TODO: Proper escaping
        String path = PathUtils.concat(getPath(), name).replace("/", "\\/");

        try {
            solrServer.deleteByQuery(String.format(
                    "%s:%s*", configuration.getPathField(), path));
            updateCallback.indexUpdate();
        } catch (SolrServerException e) {
            throw new CommitFailedException(
                    "Solr", 5, "Failed to remove documents from Solr", e);
        } catch (IOException e) {
            throw new CommitFailedException(
                    "Solr", 6, "Failed to send data to Solr", e);
        }

        return null; // no need to recurse down the removed subtree
    }

    private SolrInputDocument docFromState(NodeState state) {
        SolrInputDocument inputDocument = new SolrInputDocument();
        String path = getPath();
        inputDocument.addField(configuration.getPathField(), path);
        for (PropertyState property : state.getProperties()) {
            // try to get the field to use for this property from configuration
            String fieldName = configuration.getFieldNameFor(property.getType());
            if (fieldName != null) {
                inputDocument.addField(
                        fieldName, property.getValue(property.getType()));
            } else {
                // or fallback to adding propertyName:stringValue(s)
                if (property.isArray()) {
                    for (String s : property.getValue(Type.STRINGS)) {
                        inputDocument.addField(property.getName(), s);
                    }
                } else {
                    inputDocument.addField(
                            property.getName(), property.getValue(Type.STRING));
                }
            }
        }
        return inputDocument;
    }

}
