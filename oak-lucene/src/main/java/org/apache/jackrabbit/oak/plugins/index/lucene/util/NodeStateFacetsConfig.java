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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.facet.FacetsConfig;

/**
 * {@link FacetsConfig} persisted extension.
 */
class NodeStateFacetsConfig extends FacetsConfig {

    private static final String MULTIVALUED = "multivalued";

    private final NodeBuilder nodeBuilder;

    NodeStateFacetsConfig(NodeBuilder nodeBuilder) {
        this.nodeBuilder = nodeBuilder.child(LuceneIndexConstants.FACETS);
        if (!this.nodeBuilder.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            this.nodeBuilder.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        }
        NodeBuilder current = this.nodeBuilder;
        String prefix = "";
        readMVFacets(current, prefix);
    }

    private void readMVFacets(NodeBuilder current, String prefix) {
        for (String childName : current.getChildNodeNames()) {
            NodeBuilder child = current.child(childName);
            super.setMultiValued(childName, child.getProperty(MULTIVALUED).getValue(Type.BOOLEAN));
            if (prefix.length() > 0) {
                super.setMultiValued(prefix + "/" + childName, child.getProperty(MULTIVALUED).getValue(Type.BOOLEAN));
                readMVFacets(child, childName);
            }
        }
    }

    @Override
    public synchronized void setMultiValued(String dimName, boolean v) {
        super.setMultiValued(dimName, v);
        if (v) {
            NodeBuilder current = this.nodeBuilder;
            for (String p : PathUtils.elements(dimName)) {
                NodeBuilder child = current.child(p);
                if (!child.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
                    child.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
                }
                child.setProperty(MULTIVALUED, true);
                current = child;
            }
        }
    }
}
