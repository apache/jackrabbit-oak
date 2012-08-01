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
package org.apache.jackrabbit.oak.plugins.lucene;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class LuceneIndexUtils {

    /**
     * switch to "oak:index" as soon as it is possible
     */
    public static final String DEFAULT_INDEX_HOME = "/oak-index";

    public static final String DEFAULT_INDEX_NAME = "default";

    public static final String[] DEFAULT_INDEX_PATH = { "oak-index", "default",
            ":data" };

    private LuceneIndexUtils() {

    }

    /**
     * 
     * You still need to call #commit afterwards to persist the changes
     * 
     * @param index
     * @param path
     * @param indexName
     * @return
     */
    public static Tree createIndexNode(Tree index, String path, String indexName) {
        for (String e : PathUtils.elements(path)) {
            if (PathUtils.denotesRoot(e)) {
                continue;
            }
            if (index.hasChild(e)) {
                index = index.getChild(e);
            } else {
                index = index.addChild(e);
            }
        }
        if (!index.hasChild(":data")) {
            index.addChild(":data");
        }
        return index;
    }

    /**
     * 
     * Checks if any of the index's children qualifies as an index node, and
     * returns the list of good candidates.
     * 
     * For now each child that has a :data node is considered to be a potential
     * index
     * 
     * @param indexHome
     *            the location of potential index nodes
     * @return the list of existing indexes
     */
    public static List<LuceneIndexInfo> getIndexInfos(NodeState indexHome,
            String parentPath) {
        if (indexHome == null) {
            return Collections.<LuceneIndexInfo> emptyList();
        }
        List<String> parent = segmentPath(parentPath);
        List<LuceneIndexInfo> tempIndexes = new ArrayList<LuceneIndexInfo>();
        for (ChildNodeEntry c : indexHome.getChildNodeEntries()) {
            NodeState child = c.getNodeState();
            if (child.hasChildNode(":data")) {
                List<String> childIndexPath = new ArrayList<String>(parent);
                childIndexPath.add(c.getName());
                childIndexPath.add(":data");
                tempIndexes.add(new LuceneIndexInfo(c.getName(), childIndexPath));
            }
        }
        return tempIndexes;
    }

    private static List<String> segmentPath(String path) {
        List<String> pathElements = new ArrayList<String>();
        for (String e : PathUtils.elements(path)) {
            pathElements.add(e);
        }
        return pathElements;
    }
}
