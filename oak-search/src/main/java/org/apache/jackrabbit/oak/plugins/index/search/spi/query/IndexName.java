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
package org.apache.jackrabbit.oak.plugins.index.search.spi.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An index name, which possibly contains two version numbers: the product
 * version number, and the customer version number.
 * 
 * The format of an index node name is: 
 * - The name of the index, 
 * - optionally a dash ('-') and the product version number, 
 * - optionally "-custom-" and the customer version number.
 * 
 * If the node name doesn't contain version numbers / dashes, then version 0 is
 * assumed (for both the product version number and customer version number).
 */
public class IndexName implements Comparable<IndexName> {

    private final static Logger LOG = LoggerFactory.getLogger(IndexName.class);
    
    // already logged index names
    private static final HashSet<String> LOGGED_WARN = new HashSet<>();
    // when LOGGED_WARN will be cleared
    private static long nextLogWarnClear;

    public String getNodeName() {
        return nodeName;
    }

    public int getCustomerVersion() {
        return customerVersion;
    }

    public int getProductVersion() {
        return productVersion;
    }

    private final String nodeName;
    private final String baseName;
    private final boolean isVersioned;
    private final int productVersion;
    private final int customerVersion;
    private final boolean isLegal;
    
    /**
     * Parse the node name. Both node names with version and without version are
     * supported.
     * 
     * @param nodeName the node name (starting from root; e.g. "/oak:index/lucene")
     * @return the index name object
     */
    public static IndexName parse(final String nodeName) {
        String baseName = nodeName;
        int index = baseName.lastIndexOf('-');
        if (index < 0) {
            return new IndexName(nodeName, true);
        }
        String last = baseName.substring(index + 1);
        baseName = baseName.substring(0, index);
        try {
            int v1 = Integer.parseInt(last);
            if (!baseName.endsWith("-custom")) {
                return new IndexName(nodeName, baseName, v1, 0);
            }
            baseName = baseName.substring(0, 
                    baseName.length() - "-custom".length());
            index = baseName.lastIndexOf('-');
            if (index < 0) {
                return new IndexName(nodeName, baseName, 0, v1);
            }
            last = baseName.substring(index + 1);
            baseName = baseName.substring(0, index);
            int v2 = Integer.parseInt(last);
            return new IndexName(nodeName, baseName, v2, v1);
        } catch (NumberFormatException e) {
            long now = System.currentTimeMillis();
            if (nextLogWarnClear < now) {
                LOGGED_WARN.clear();
                // clear again each 5 minutes
                nextLogWarnClear = now + 5 * 60 * 1000;
            }
            if (LOGGED_WARN.add(nodeName)) {
                LOG.warn("Index name format error: " + nodeName);
            }
            return new IndexName(nodeName, false);
        }
    }
    
    private IndexName(String nodeName, boolean isLegal) {
        // not versioned
        this.nodeName = nodeName;
        this.baseName = nodeName;
        this.isVersioned = false;
        this.productVersion = 0;
        this.customerVersion = 0;
        this.isLegal = isLegal;
    }

    private IndexName(String nodeName, String baseName, int productVersion, int customerVersion) {
        // versioned
        this.nodeName = nodeName;
        this.baseName = baseName;
        this.isVersioned = true;
        this.productVersion = productVersion;
        this.customerVersion = customerVersion;
        this.isLegal = true;
    }
    
    public String toString() {
        return nodeName + 
                " base=" + baseName + 
                (isVersioned ? " versioned": "") + 
                " product=" + productVersion + 
                " custom=" + customerVersion +
                (isLegal ? "" : " illegal");
    }
    
    @Override
    public int compareTo(IndexName o) {
        int comp = baseName.compareTo(o.baseName);
        if (comp != 0) {
            return comp;
        }
        comp = Integer.compare(productVersion, o.productVersion);
        if (comp != 0) {
            return comp;
        }
        return Integer.compare(customerVersion, o.customerVersion);
    }

    /**
     * Filter out index that are replaced by another index with the same base
     * name but newer version.
     * 
     * Indexes without a version number in the name are always used, except if
     * there is an active index with the same base name but a newer version.
     * 
     * Active indexes have a hidden ":oak:mount-" node, which means they are
     * indexed in the read-only node store.
     * 
     * @param indexPaths the set of index paths
     * @param rootState the root node state (used to find hidden nodes)
     * @return the filtered list
     */
    public static Collection<String> filterReplacedIndexes(Collection<String> indexPaths, NodeState rootState) {
        HashMap<String, IndexName> latestVersions = new HashMap<String, IndexName>();
        for (String p : indexPaths) {
            IndexName indexName = IndexName.parse(p);
            if (indexName.isVersioned) {
                // which might not be a good idea - instead, it should check if the composite node store is used
                // (but how?)
                if (!isIndexActive(p, rootState)) {
                    // the index is inactive, so not used
                    continue;
                }
            }
            IndexName stored = latestVersions.get(indexName.baseName);
            if (stored == null || stored.compareTo(indexName) < 0) {
                // no old version, or old version is smaller: replace
                latestVersions.put(indexName.baseName, indexName);
            }
        }
        ArrayList<String> result = new ArrayList<>(latestVersions.size());
        for (IndexName n : latestVersions.values()) {
            result.add(n.nodeName);
        }
        return result;
    }

    private static boolean isIndexActive(String indexPath, NodeState rootState) {
        NodeState indexNode = rootState;
        for(String e : PathUtils.elements(indexPath)) {
            indexNode = indexNode.getChildNode(e);
        }
        for(String c : indexNode.getChildNodeNames()) {
            if (c.startsWith(":oak:mount-")) {
                return true;
            }
        }
        return false;
    }
    
}