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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.lock.LockConstants;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

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
        Tree destParent = dest.getParent();
        if (!destParent.exists()) {
            throw new PathNotFoundException(destParent.getPath());
        }

        // check source exists
        Tree src = root.getTree(srcPath);
        if (src.isRoot()) {
            throw new RepositoryException("Cannot copy the root node");
        }
        if (!src.exists()) {
            throw new PathNotFoundException(srcPath);
        }

        accessManager.checkPermissions(destPath, Permissions.getString(Permissions.NODE_TYPE_MANAGEMENT));

        try {
            Tree typeRoot = root.getTree(NODE_TYPES_PATH);
            new WorkspaceCopy(src, destParent, Text.getName(destPath), typeRoot, sessionDelegate.getAuthInfo().getUserID()).perform();
            context.getSessionDelegate().commit(root);
            sessionDelegate.refresh(true);
        } catch (CommitFailedException e) {
            throw e.asRepositoryException();
        }
    }

    //---------------------------< internal >-----------------------------------

    private static final class WorkspaceCopy {

        private final Map<String, String> translated = Maps.newHashMap();

        private final Tree src;
        private final Tree destParent;
        private final String destName;

        private final Tree typeRoot;
        private final String userId;

        public WorkspaceCopy(@Nonnull Tree src, @Nonnull Tree destParent,
                             @Nonnull String destName, @Nonnull Tree typeRoot,
                             @Nonnull String userId) {
            this.src = src;
            this.destParent = destParent;
            this.destName = destName;
            this.typeRoot = typeRoot;
            this.userId = userId;
        }

        public void perform() throws RepositoryException {
            copy(src, destParent, destName);
            updateReferences(src, destParent.getChild(destName));
        }

        private void copy(@Nonnull Tree source, @Nonnull Tree destParent, @Nonnull String destName) throws RepositoryException {
            String primaryType = TreeUtil.getPrimaryTypeName(source);
            if (primaryType == null) {
                primaryType = TreeUtil.getDefaultChildType(typeRoot, destParent, destName);
                if (primaryType == null) {
                    throw new ConstraintViolationException("Cannot determine default node type.");
                }
            }
            Tree dest = TreeUtil.addChild(destParent, destName, primaryType, typeRoot, userId);
            for (PropertyState property : source.getProperties()) {
                String propName = property.getName();
                if (JCR_MIXINTYPES.equals(propName)) {
                    for (String mixin : property.getValue(Type.NAMES)) {
                        TreeUtil.addMixin(dest, mixin, typeRoot, userId);
                    }
                } else if (JCR_UUID.equals(propName)) {
                    String sourceId = property.getValue(Type.STRING);
                    String newId = IdentifierManager.generateUUID();
                    dest.setProperty(JCR_UUID, newId, Type.STRING);
                    if (!translated.containsKey(sourceId)) {
                        translated.put(sourceId, newId);
                    }
                } else if (!JCR_PRIMARYTYPE.equals(propName)
                        && !VersionConstants.VERSION_PROPERTY_NAMES.contains(propName)
                        && !LockConstants.LOCK_PROPERTY_NAMES.contains(propName)) {
                    dest.setProperty(property);
                }
            }
            if (TreeUtil.isNodeType(source, JcrConstants.MIX_VERSIONABLE, typeRoot)) {
                String sourceBaseVersionId = source.getProperty(JcrConstants.JCR_BASEVERSION).getValue(Type.STRING);
                dest.setProperty(VersionConstants.HIDDEN_COPY_SOURCE, sourceBaseVersionId, Type.PATH);
            }
            for (Tree child : source.getChildren()) {
                copy(child, dest, child.getName());
            }
        }

        /**
         * Recursively updates references on the destination tree as defined by
         * {@code Workspace.copy()}.
         *
         * @param src  the source tree of the copy operation.
         * @param dest the unprocessed copy of the tree.
         */
        private void updateReferences(Tree src, Tree dest)
                throws RepositoryException {
            for (PropertyState prop : src.getProperties()) {
                if (isReferenceType(prop) && !VersionConstants.VERSION_PROPERTY_NAMES.contains(prop.getName()))
                    updateProperty(prop, dest);
            }
            for (Tree child : src.getChildren()) {
                updateReferences(child, dest.getChild(child.getName()));
            }
        }

        private boolean isReferenceType(PropertyState property) {
            Type<?> type = property.getType();
            return (type == Type.REFERENCE
                    || type == Type.REFERENCES
                    || type == Type.WEAKREFERENCE
                    || type == Type.WEAKREFERENCES);
        }

        private void updateProperty(PropertyState prop, Tree dest) {
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

        private void translateId(String id, List<String> ids) {
            String newId = translated.get(id);
            if (newId != null) {
                ids.add(newId);
            } else {
                ids.add(id);
            }
        }
    }

}
