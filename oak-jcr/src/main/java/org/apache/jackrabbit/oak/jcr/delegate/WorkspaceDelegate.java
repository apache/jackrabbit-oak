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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.plugins.lock.LockConstants;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

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

        String userId = sessionDelegate.getAuthInfo().getUserID();
        new WorkspaceCopy(src, destParent, Text.getName(destPath)).perform(root, userId);
        sessionDelegate.refresh(true);
    }

    //---------------------------< internal >-----------------------------------

    private static final class WorkspaceCopy {
        private final Map<String, String> translated = Maps.newHashMap();

        private final Tree source;
        private final Tree destParent;
        private final String destName;

        public WorkspaceCopy(@Nonnull Tree source, @Nonnull Tree destParent, @Nonnull String destName) {
            this.source = source;
            this.destParent = destParent;
            this.destName = destName;
        }

        public void perform(@Nonnull Root root, @Nonnull String userId) throws RepositoryException {
            try {
                Tree typeRoot = root.getTree(NODE_TYPES_PATH);
                copy(source, destParent, destName, typeRoot, userId);
                updateReferences(source, destParent.getChild(destName));

                Map<String, String> copyInfo = new HashMap<String, String>();
                copyInfo.put("copy-source", source.getPath());
                if (TreeUtil.isNodeType(source, JcrConstants.MIX_VERSIONABLE, typeRoot)) {
                    String sourceBaseVersionId = source.getProperty(JcrConstants.JCR_BASEVERSION).getValue(Type.STRING);
                    copyInfo.put(VersionConstants.JCR_COPIED_FROM, sourceBaseVersionId);
                }
                root.commit(ImmutableMap.<String, Object>copyOf(copyInfo));
            } catch (CommitFailedException e) {
                throw e.asRepositoryException();
            }
        }

        private void copy(Tree source, Tree destParent, String destName, Tree typeRoot, String userId)
                throws RepositoryException {
            String primaryType = TreeUtil.getPrimaryTypeName(source);
            Tree dest = TreeUtil.addChild(
                    destParent, destName, primaryType, typeRoot, userId);
            for (PropertyState property : source.getProperties()) {
                String propName = property.getName();
                if (JCR_MIXINTYPES.equals(propName)) {
                    for (String mixin : property.getValue(Type.NAMES)) {
                        TreeUtil.addMixin(dest, mixin, typeRoot, userId);
                    }
                } else if (JCR_UUID.equals(propName)) {
                    String sourceId = property.getValue(Type.STRING);
                    String newId = UUIDUtils.generateUUID();
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
            for (Tree child : source.getChildren()) {
                copy(child, dest, child.getName(), typeRoot, userId);
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
                if (isReferenceType(prop) && !VersionConstants.VERSION_PROPERTY_NAMES.contains(prop.getName())) {
                    updateProperty(prop, dest);
                }
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
