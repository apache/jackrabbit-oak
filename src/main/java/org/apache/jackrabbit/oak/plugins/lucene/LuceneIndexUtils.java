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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexDefinitionImpl;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class LuceneIndexUtils {

    public static final String DEFAULT_INDEX_NAME = "default-lucene";

    public static final String INDEX_DATA_CHILD_NAME = ":data";

    // public static final String[] DEFAULT_INDEX_PATH = { "oak-index",
    // "default",
    // ":data" };

    private LuceneIndexUtils() {

    }

    /**
     * 
     * You still need to call #commit afterwards to persist the changes
     * 
     * @param index
     * @param indexName
     * @return
     */
    public static Tree createIndexNode(Tree index, String indexName,
            CoreValueFactory vf) {
        if (index.hasChild(indexName)) {
            index = index.getChild(indexName);
        } else {
            index = index.addChild(indexName);
        }
        index.setProperty(IndexDefinition.TYPE_PROPERTY_NAME,
                vf.createValue(LuceneIndexFactory.TYPE));
        if (!index.hasChild(INDEX_DATA_CHILD_NAME)) {
            index.addChild(INDEX_DATA_CHILD_NAME);
        }
        return index;
    }

    /**
     * 
     * Checks if any of the index's children qualifies as an index node, and
     * returns the list of good candidates.
     * 
     * For now each child that has a "type=lucene" property and a ":data" node
     * is considered to be a potential index
     * 
     * @param indexHome
     *            the location of potential index nodes
     * @return the list of existing indexes
     */
    public static List<IndexDefinition> getIndexInfos(NodeState indexHome,
            String parentPath) {
        if (indexHome == null) {
            return Collections.<IndexDefinition> emptyList();
        }
        List<IndexDefinition> tempIndexes = new ArrayList<IndexDefinition>();
        for (ChildNodeEntry c : indexHome.getChildNodeEntries()) {
            NodeState child = c.getNodeState();

            PropertyState type = child
                    .getProperty(IndexDefinition.TYPE_PROPERTY_NAME);
            if (type == null
                    || type.isArray()
                    || !LuceneIndexFactory.TYPE.equals(type.getValue()
                            .getString())) {
                continue;
            }

            if (child.hasChildNode(INDEX_DATA_CHILD_NAME)) {
                Map<String, String> props = new HashMap<String, String>();
                for (PropertyState ps : child.getProperties()) {
                    if (ps != null && !ps.isArray()) {
                        String v = ps.getValue().getString();
                        props.put(ps.getName(), v);
                    }
                }
                tempIndexes.add(new IndexDefinitionImpl(c.getName(), type
                        .getValue().getString(), PathUtils.concat(parentPath,
                        c.getName()), false, null));
            }
        }
        return tempIndexes;
    }
}
