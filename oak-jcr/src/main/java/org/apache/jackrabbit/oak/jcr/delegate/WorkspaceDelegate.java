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
package org.apache.jackrabbit.oak.jcr.delegate;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;

/**
 * Delegate class for workspace operations.
 */
public class WorkspaceDelegate {

    private final SessionContext context;

    public WorkspaceDelegate(SessionContext context) {
        this.context = checkNotNull(context);
    }

    /**
     * Copy a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @throws RepositoryException
     */
    public void copy(String srcPath, String destPath) throws RepositoryException {
        SessionDelegate sessionDelegate = context.getSessionDelegate();
        AccessManager accessManager = context.getAccessManager();
        Root root = sessionDelegate.getContentSession().getLatestRoot();
        // check destination
        Tree dest = root.getTree(destPath);
        if (dest.exists()) {
            throw new ItemExistsException(destPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = root.getTree(destParentPath);
        if (!destParent.exists()) {
            throw new PathNotFoundException(PathUtils.getParentPath(destPath));
        }

        // check source exists
        Tree src = root.getTree(srcPath);
        if (!src.exists()) {
            throw new PathNotFoundException(srcPath);
        }

        accessManager.checkPermissions(destPath, Permissions.getString(Permissions.NODE_TYPE_MANAGEMENT));

        try {
            new WorkspaceCopy(root, srcPath, destPath).perform();
            root.commit();
            sessionDelegate.refresh(true);
        } catch (CommitFailedException e) {
            throw e.asRepositoryException();
        }
    }

    //---------------------------< internal >-----------------------------------

    private class WorkspaceCopy {

        private final Map<String, String> translated = Maps.newHashMap();
        private final String srcPath;
        private final String destPath;
        private final Root currentRoot;

        public WorkspaceCopy(Root currentRoot, String srcPath, String destPath) {
            this.srcPath = checkNotNull(srcPath);
            this.destPath = checkNotNull(destPath);
            this.currentRoot = checkNotNull(currentRoot);
        }

        public void perform() throws RepositoryException {
            if (!currentRoot.copy(srcPath, destPath)) {
                throw new RepositoryException("Cannot copy node at " + srcPath + " to " + destPath);
            }
            Tree src = currentRoot.getTree(srcPath);
            Tree dest = currentRoot.getTree(destPath);
            generateNewIdentifiers(dest);
            updateReferences(src, dest);
        }

        public void generateNewIdentifiers(Tree t) throws RepositoryException {
            if (t.hasProperty(JCR_UUID)) {
                getNewId(t);
            }
            for (Tree c : t.getChildren()) {
                generateNewIdentifiers(c);
            }
        }

        /**
         * Recursively updates references on the destination tree as defined by
         * <code>Workspace.copy()</code>.
         *
         * @param src  the source tree of the copy operation.
         * @param dest the unprocessed copy of the tree.
         */
        private void updateReferences(Tree src, Tree dest)
                throws RepositoryException {
            for (PropertyState prop : src.getProperties()) {
                Type<?> type = prop.getType();
                if (type == Type.REFERENCE
                        || type == Type.REFERENCES
                        || type == Type.WEAKREFERENCE
                        || type == Type.WEAKREFERENCES) {
                    updateProperty(prop, dest);
                }
            }
            for (Tree child : src.getChildren()) {
                updateReferences(child, dest.getChild(child.getName()));
            }
        }

        private void updateProperty(PropertyState prop, Tree dest)
                throws RepositoryException {
            boolean multi = prop.isArray();
            boolean weak = prop.getType() == Type.WEAKREFERENCE
                    || prop.getType() == Type.WEAKREFERENCES;
            List<String> ids = new ArrayList<String>();
            for (int i = 0; i < prop.count(); i++) {
                String id;
                if (weak) {
                    id = prop.getValue(Type.WEAKREFERENCE, i);
                } else {
                    id = prop.getValue(Type.REFERENCE, i);
                }
                translateId(id, ids);
            }
            PropertyState p;
            if (multi) {
                if (weak) {
                    p = MultiGenericPropertyState.weakreferenceProperty(
                            prop.getName(), ids);
                } else {
                    p = MultiGenericPropertyState.referenceProperty(
                            prop.getName(), ids);
                }
            } else {
                if (weak) {
                    p = GenericPropertyState.weakreferenceProperty(prop.getName(), ids.get(0));
                } else {
                    p = GenericPropertyState.referenceProperty(prop.getName(), ids.get(0));
                }
            }
            dest.setProperty(p);
        }

        private void translateId(String id, List<String> ids)
                throws RepositoryException {
            String newId = translated.get(id);
            if (newId != null) {
                ids.add(newId);
            } else {
                ids.add(id);
            }
        }

        private String getNewId(Tree t) throws RepositoryException {
            PropertyState uuid = t.getProperty(JCR_UUID);
            if (uuid == null) {
                // not referenceable?
                throw new RepositoryException(
                        "Node is not referenceable: " + t.getPath());
            }
            String targetId = uuid.getValue(Type.STRING);
            // new id needed?
            if (!translated.containsKey(targetId)) {
                String newId = IdentifierManager.generateUUID();
                translated.put(targetId, newId);
                t.setProperty(JCR_UUID, newId, Type.STRING);
            }
            return translated.get(targetId);
        }
    }

}
