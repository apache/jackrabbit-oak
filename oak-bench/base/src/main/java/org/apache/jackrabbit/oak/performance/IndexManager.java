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
package org.apache.jackrabbit.oak.performance;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.old.Indexer;

/**
 * A utility class to manage indexes in Oak.
 */
public class IndexManager {

    /**
     * The root node of the index definition (configuration) nodes.
     */
    public static final String INDEX_CONFIG_PATH = Indexer.INDEX_CONFIG_PATH;

    /**
     * Creates a property index for the given property if such an index doesn't
     * exist yet, and if the repository supports property indexes. The session
     * may not have pending changes.
     * 
     * @param session the session
     * @param propertyName the property name
     * @return true if the index was created or already existed
     */
    public static boolean createPropertyIndex(Session session,
            String propertyName) throws RepositoryException {
        return createIndex(session, "property@" + propertyName);
    }

    private static Node getIndexNode(Session session)
            throws RepositoryException {
        Node n = session.getRootNode();
        for (String e : PathUtils.elements(INDEX_CONFIG_PATH)) {
            if (!n.hasNode(e)) {
                return null;
            }
            n = n.getNode(e);
        }
        return n;
    }

    private static boolean createIndex(Session session, String indexNodeName)
            throws RepositoryException {
        if (session.hasPendingChanges()) {
            throw new RepositoryException("The session has pending changes");
        }
        Node indexes = getIndexNode(session);
        if (indexes == null) {
            return false;
        }
        if (!indexes.hasNode(indexNodeName)) {
            indexes.addNode(indexNodeName);
            session.save();
        }
        return true;
    }

}
