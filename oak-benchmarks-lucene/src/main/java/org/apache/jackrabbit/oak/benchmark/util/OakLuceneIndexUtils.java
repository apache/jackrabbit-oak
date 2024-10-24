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
package org.apache.jackrabbit.oak.benchmark.util;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OakLuceneIndexUtils {

    /**
     * Helper method to create or update a lucene property index definition.
     *
     * @param session the session
     * @param indexDefinitionName the name of the node for the index definition
     * @param propertyNames the list of properties to index
     * @param type the types of the properties in order of the properties
     * @param orderedPropsMap the ordered props and its properties
     * @param persistencePath the path if the persistence=file (default is repository)
     * @return the node just created
     * @throws RepositoryException the repository exception
     */
    public static Node luceneIndexDefinition(Session session, String indexDefinitionName,
                                             String async, String[] propertyNames, String[] type,
                                             Map<String, Map<String, String>> orderedPropsMap, String persistencePath)
            throws RepositoryException {

        Node root = session.getRootNode();
        Node indexDefRoot = JcrUtils.getOrAddNode(root, IndexConstants.INDEX_DEFINITIONS_NAME,
                NodeTypeConstants.NT_UNSTRUCTURED);

        Node indexDef = JcrUtils.getOrAddNode(indexDefRoot, indexDefinitionName,
                IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);

        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        indexDef.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        if (async != null) {
            indexDef.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, async);
        }
        // Set indexed property names
        indexDef.setProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propertyNames,
                PropertyType.NAME);

        Node propsNode = JcrUtils.getOrAddNode(indexDef, FulltextIndexConstants.PROP_NODE);
        for (int i = 0; i < propertyNames.length; i++) {
            Node propNode =
                    JcrUtils.getOrAddNode(propsNode, propertyNames[i], NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            propNode.setProperty(FulltextIndexConstants.PROP_TYPE, type[i]);
        }

        // Set ordered property names
        if ((orderedPropsMap != null) && !orderedPropsMap.isEmpty()) {
            List<String> orderedProps = new ArrayList<>();
            for (Map.Entry<String, Map<String, String>> orderedPropEntry : orderedPropsMap
                    .entrySet()) {
                Node propNode = JcrUtils.getOrAddNode(propsNode, orderedPropEntry.getKey(),
                        NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                propNode.setProperty(FulltextIndexConstants.PROP_TYPE,
                        orderedPropEntry.getValue().get(FulltextIndexConstants.PROP_TYPE));
                orderedProps.add(orderedPropEntry.getKey());
            }
            if (!orderedProps.isEmpty()) {
                indexDef.setProperty(FulltextIndexConstants.ORDERED_PROP_NAMES,
                        orderedProps.toArray(new String[orderedProps.size()]),
                        PropertyType.NAME);
            }
        }

        // Set file persistence if specified
        if (!Strings.isNullOrEmpty(persistencePath)) {
            indexDef.setProperty(FulltextIndexConstants.PERSISTENCE_NAME,
                    FulltextIndexConstants.PERSISTENCE_FILE);
            indexDef.setProperty(FulltextIndexConstants.PERSISTENCE_PATH,
                    persistencePath);
        }
        session.save();

        return indexDef;
    }

}
