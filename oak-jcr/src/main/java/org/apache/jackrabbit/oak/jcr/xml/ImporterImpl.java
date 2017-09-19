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
package org.apache.jackrabbit.oak.jcr.xml;

import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionManager;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.session.WorkspaceImpl;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeType;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.xml.Importer;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImporterImpl implements Importer {
    private static final Logger log = LoggerFactory.getLogger(ImporterImpl.class);

    private final Tree importTargetTree;
    private final Tree ntTypesRoot;
    private final int uuidBehavior;

    private final String userID;
    private final AccessManager accessManager;

    private final EffectiveNodeTypeProvider effectiveNodeTypeProvider;
    private final DefinitionProvider definitionProvider;

    private final IdResolver idLookup;

    private final Stack<Tree> parents;

    /**
     * helper object that keeps track of remapped uuid's and imported reference
     * properties that might need correcting depending on the uuid mappings
     */
    private final ReferenceChangeTracker refTracker;

    private final List<ProtectedItemImporter> pItemImporters = new ArrayList<ProtectedItemImporter>();

    /**
     * Currently active importer for protected nodes.
     */
    private ProtectedNodeImporter pnImporter;

    /**
     * Creates a new importer instance.
     *
     * @param absPath  The absolute JCR paths such as passed to the JCR call.
     * @param sessionContext The context of the editing session
     * @param root The write {@code Root}, which in case of a workspace import
     * is different from the {@code Root} associated with the editing session.
     * @param uuidBehavior The uuid behavior
     * @param isWorkspaceImport {@code true} if this is a workspace import,
     * {@code false} otherwise.
     * @throws javax.jcr.RepositoryException If the initial validation of the
     * path or the state of target node/session fails.
     */
    public ImporterImpl(String absPath,
                        SessionContext sessionContext,
                        Root root,
                        int uuidBehavior,
                        boolean isWorkspaceImport) throws RepositoryException {
        String oakPath = sessionContext.getOakPath(absPath);
        if (oakPath == null) {
            throw new RepositoryException("Invalid name or path: " + absPath);
        }
        if (!PathUtils.isAbsolute(oakPath)) {
            throw new RepositoryException("Not an absolute path: " + absPath);
        }

        SessionDelegate sd = sessionContext.getSessionDelegate();
        if (isWorkspaceImport && sd.hasPendingChanges()) {
            throw new RepositoryException("Pending changes on session. Cannot run workspace import.");
        }

        this.uuidBehavior = uuidBehavior;
        userID = sd.getAuthInfo().getUserID();

        importTargetTree = root.getTree(oakPath);
        if (!importTargetTree.exists()) {
            throw new PathNotFoundException(absPath);
        }

        WorkspaceImpl wsp = sessionContext.getWorkspace();
        VersionManager vMgr = wsp.getVersionManager();
        if (!vMgr.isCheckedOut(absPath)) {
            throw new VersionException("Target node is checked in.");
        }
        if (importTargetTree.getStatus() != Tree.Status.NEW && wsp.getLockManager().isLocked(absPath)) {
            throw new LockException("Target node is locked.");
        }
        effectiveNodeTypeProvider = wsp.getNodeTypeManager();
        definitionProvider = wsp.getNodeTypeManager();
        ntTypesRoot = root.getTree(NODE_TYPES_PATH);

        accessManager = sessionContext.getAccessManager();

        idLookup = new IdResolver(root, sd.getContentSession());

        refTracker = new ReferenceChangeTracker();

        parents = new Stack<Tree>();
        parents.push(importTargetTree);

        pItemImporters.clear();
        for (ProtectedItemImporter importer : sessionContext.getProtectedItemImporters()) {
            // FIXME this passes the session scoped name path mapper also for workspace imports
            if (importer.init(sessionContext.getSession(), root, sessionContext, isWorkspaceImport, uuidBehavior, refTracker, sessionContext.getSecurityProvider())) {
                pItemImporters.add(importer);
            }
        }
    }

    private Tree createTree(@Nonnull Tree parent, @Nonnull NodeInfo nInfo, @CheckForNull String uuid) throws RepositoryException {
        String ntName = nInfo.getPrimaryTypeName();
        Tree child = TreeUtil.addChild(
                parent, nInfo.getName(), ntName, ntTypesRoot, userID);
        if (ntName != null) {
            accessManager.checkPermissions(child, child.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.NODE_TYPE_MANAGEMENT);
        }
        if (uuid != null) {
            child.setProperty(JcrConstants.JCR_UUID, uuid);
        }
        for (String mixin : nInfo.getMixinTypeNames()) {
            TreeUtil.addMixin(child, mixin, ntTypesRoot, userID);
        }
        return child;
    }

    private void createProperty(Tree tree, PropInfo pInfo, PropertyDefinition def) throws RepositoryException {
        tree.setProperty(pInfo.asPropertyState(def));
        int type = pInfo.getType();
        if (type == PropertyType.REFERENCE || type == PropertyType.WEAKREFERENCE) {
            // store reference for later resolution
            refTracker.processedReference(new Reference(tree, pInfo.getName()));
        }
    }

    private Tree resolveUUIDConflict(Tree parent,
                                     Tree conflicting,
                                     String conflictingId,
                                     NodeInfo nodeInfo) throws RepositoryException {
        Tree tree;
        if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW) {
            // create new with new uuid
            tree = createTree(parent, nodeInfo, UUID.randomUUID().toString());
            // remember uuid mapping
            if (isNodeType(tree, JcrConstants.MIX_REFERENCEABLE)) {
                refTracker.put(nodeInfo.getUUID(), TreeUtil.getString(tree, JcrConstants.JCR_UUID));
            }
        } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW) {
            // if conflicting node is shareable, then clone it
            String msg = "a node with uuid " + nodeInfo.getUUID() + " already exists!";
            log.debug(msg);
            throw new ItemExistsException(msg);
        } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING) {
            if (conflicting == null) {
                // since the conflicting node can't be read,
                // we can't remove it
                String msg = "node with uuid " + conflictingId + " cannot be removed";
                log.debug(msg);
                throw new RepositoryException(msg);
            }

            // make sure conflicting node is not importTargetNode or an ancestor thereof
            if (importTargetTree.getPath().startsWith(conflicting.getPath())) {
                String msg = "cannot remove ancestor node";
                log.debug(msg);
                throw new ConstraintViolationException(msg);
            }
            // remove conflicting
            conflicting.remove();
            // create new with given uuid
            tree = createTree(parent, nodeInfo, nodeInfo.getUUID());
        } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING) {
            if (conflicting == null) {
                // since the conflicting node can't be read,
                // we can't replace it
                String msg = "node with uuid " + conflictingId + " cannot be replaced";
                log.debug(msg);
                throw new RepositoryException(msg);
            }

            if (conflicting.isRoot()) {
                String msg = "root node cannot be replaced";
                log.debug(msg);
                throw new RepositoryException(msg);
            }
            // 'replace' current parent with parent of conflicting
            parent = conflicting.getParent();

            // replace child node
            //TODO ordering! (what happened to replace?)
            conflicting.remove();
            tree = createTree(parent, nodeInfo, nodeInfo.getUUID());
        } else {
            String msg = "unknown uuidBehavior: " + uuidBehavior;
            log.debug(msg);
            throw new RepositoryException(msg);
        }
        return tree;
    }

    private void importProperties(@Nonnull Tree tree,
                                  @Nonnull List<PropInfo> propInfos,
                                  boolean ignoreRegular) throws RepositoryException {
        // process properties
        for (PropInfo pi : propInfos) {
            // find applicable definition
            //TODO find better heuristics?
            EffectiveNodeType ent = effectiveNodeTypeProvider.getEffectiveNodeType(tree);
            PropertyDefinition def = ent.getPropertyDefinition(pi.getName(), pi.getType(), pi.isUnknownMultiple());
            if (def.isProtected()) {
                // skip protected property
                log.debug("Protected property {}", pi.getName());

                // notify the ProtectedPropertyImporter.
                for (ProtectedPropertyImporter ppi : getPropertyImporters()) {
                    if (ppi.handlePropInfo(tree, pi, def)) {
                        log.debug("Protected property -> delegated to ProtectedPropertyImporter");
                        break;
                    } /* else: p-i-Importer isn't able to deal with this property. try next pp-importer */
                }
            } else if (!ignoreRegular) {
                // regular property -> create the property
                createProperty(tree, pi, def);
            }
        }
        for (ProtectedPropertyImporter ppi : getPropertyImporters()) {
            ppi.propertiesCompleted(tree);
        }
    }

    private Iterable<ProtectedPropertyImporter> getPropertyImporters() {
        return Iterables.filter(Iterables.transform(pItemImporters, new Function<ProtectedItemImporter, ProtectedPropertyImporter>() {
            @Nullable
            @Override
            public ProtectedPropertyImporter apply(@Nullable ProtectedItemImporter importer) {
                if (importer instanceof ProtectedPropertyImporter) {
                    return (ProtectedPropertyImporter) importer;
                } else {
                    return null;
                }
            }
        }), Predicates.notNull());
    }

    private Iterable<ProtectedNodeImporter> getNodeImporters() {
        return Iterables.filter(Iterables.transform(pItemImporters, new Function<ProtectedItemImporter, ProtectedNodeImporter>() {
            @Nullable
            @Override
            public ProtectedNodeImporter apply(@Nullable ProtectedItemImporter importer) {
                if (importer instanceof ProtectedNodeImporter) {
                    return (ProtectedNodeImporter) importer;
                } else {
                    return null;
                }
            }
        }), Predicates.notNull());
    }

    //-----------------------------------------------------------< Importer >---

    @Override
    public void start() throws RepositoryException {
        // nop
    }

    @Override
    public void startNode(NodeInfo nodeInfo, List<PropInfo> propInfos)
            throws RepositoryException {
        Tree parent = parents.peek();
        Tree tree = null;
        String id = nodeInfo.getUUID();
        String nodeName = nodeInfo.getName();
        String ntName = nodeInfo.getPrimaryTypeName();

        if (parent == null) {
            log.debug("Skipping node: {}", nodeName);
            // parent node was skipped, skip this child node too
            parents.push(null); // push null onto stack for skipped node
            // notify the p-i-importer
            if (pnImporter != null) {
                pnImporter.startChildInfo(nodeInfo, propInfos);
            }
            return;
        }

        NodeDefinition parentDef = getDefinition(parent);
        if (parentDef.isProtected()) {
            // skip protected node
            parents.push(null);
            log.debug("Skipping protected node: {}", nodeName);

            if (pnImporter != null) {
                // pnImporter was already started (current nodeInfo is a sibling)
                // notify it about this child node.
                pnImporter.startChildInfo(nodeInfo, propInfos);
            } else {
                // no importer defined yet:
                // test if there is a ProtectedNodeImporter among the configured
                // importers that can handle this.
                // if there is one, notify the ProtectedNodeImporter about the
                // start of a item tree that is protected by this parent. If it
                // potentially is able to deal with it, notify it about the child node.
                for (ProtectedNodeImporter pni : getNodeImporters()) {
                    if (pni.start(parent)) {
                        log.debug("Protected node -> delegated to ProtectedNodeImporter");
                        pnImporter = pni;
                        pnImporter.startChildInfo(nodeInfo, propInfos);
                        break;
                    } /* else: p-i-Importer isn't able to deal with the protected tree.
                     try next. and if none can handle the passed parent the
                     tree below will be skipped */
                }
            }
            return;
        }

        if (parent.hasChild(nodeName)) {
            // a node with that name already exists...
            Tree existing = parent.getChild(nodeName);
            NodeDefinition def = getDefinition(existing);
            if (!def.allowsSameNameSiblings()) {
                // existing doesn't allow same-name siblings,
                // check for potential conflicts
                if (def.isProtected() && isNodeType(existing, ntName)) {
                    /*
                     use the existing node as parent for the possible subsequent
                     import of a protected tree, that the protected node importer
                     may or may not be able to deal with.
                     -> upon the next 'startNode' the check for the parent being
                        protected will notify the protected node importer.
                     -> if the importer is able to deal with that node it needs
                        to care of the complete subtree until it is notified
                        during the 'endNode' call.
                     -> if the import can't deal with that node or if that node
                        is the a leaf in the tree to be imported 'end' will
                        not have an effect on the importer, that was never started.
                    */
                    log.debug("Skipping protected node: {}", existing);
                    parents.push(existing);
                    /**
                     * let ProtectedPropertyImporters handle the properties
                     * associated with the imported node. this may include overwriting,
                     * merging or just adding missing properties.
                     */
                    importProperties(existing, propInfos, true);
                    return;
                }
                if (def.isAutoCreated() && isNodeType(existing, ntName)) {
                    // this node has already been auto-created, no need to create it
                    tree = existing;
                } else {
                    // edge case: colliding node does have same uuid
                    // (see http://issues.apache.org/jira/browse/JCR-1128)
                    String existingIdentifier = IdentifierManager.getIdentifier(existing);
                    if (!(existingIdentifier.equals(id)
                            && (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING
                            || uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING))) {
                        throw new ItemExistsException(
                                "Node with the same UUID exists:" + existing);
                    }
                    // fall through
                }
            }
        }

        if (tree == null) {
            // create node
            if (id == null) {
                // no potential uuid conflict, always add new node
                tree = createTree(parent, nodeInfo, null);
            } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW) {
                // always create a new UUID even if no
                // conflicting node exists. see OAK-1244
                tree = createTree(parent, nodeInfo, UUID.randomUUID().toString());
                // remember uuid mapping
                if (isNodeType(tree, JcrConstants.MIX_REFERENCEABLE)) {
                    refTracker.put(nodeInfo.getUUID(), TreeUtil.getString(tree, JcrConstants.JCR_UUID));
                }
            } else {

                Tree conflicting = idLookup.getConflictingTree(id);
                if (conflicting != null && conflicting.exists()) {
                    // resolve uuid conflict
                    tree = resolveUUIDConflict(parent, conflicting, id, nodeInfo);
                    if (tree == null) {
                        // no new node has been created, so skip this node
                        parents.push(null); // push null onto stack for skipped node
                        log.debug("Skipping existing node {}", nodeInfo.getName());
                        return;
                    }
                } else {
                    // create new with given uuid
                    tree = createTree(parent, nodeInfo, id);
                }
            }
        }

        // process properties
        importProperties(tree, propInfos, false);

        if (tree.exists()) {
            parents.push(tree);
        }
    }


    @Override
    public void endNode(NodeInfo nodeInfo) throws RepositoryException {
        Tree parent = parents.pop();
        if (parent == null) {
            if (pnImporter != null) {
                pnImporter.endChildInfo();
            }
        } else if (getDefinition(parent).isProtected()) {
            if (pnImporter != null) {
                pnImporter.end(parent);
                // and reset the pnImporter field waiting for the next protected
                // parent -> selecting again from available importers
                pnImporter = null;
            }
        }

        idLookup.rememberImportedUUIDs(parent);
    }

    @Override
    public void end() throws RepositoryException {
        /**
         * adjust references that refer to uuids which have been mapped to
         * newly generated uuids on import
         */
        // 1. let protected property/node importers handle protected ref-properties
        //    and (protected) properties underneath a protected parent node.
        for (ProtectedItemImporter ppi : pItemImporters) {
            ppi.processReferences();
        }

        // 2. regular non-protected properties.
        Iterator<Object> iter = refTracker.getProcessedReferences();
        while (iter.hasNext()) {
            Object ref = iter.next();
            if (!(ref instanceof Reference)) {
                continue;
            }

            Reference reference = (Reference) ref;
            if (reference.isMultiple()) {
                Iterable<String> values = reference.property.getValue(Type.STRINGS);
                List<String> newValues = Lists.newArrayList();
                for (String original : values) {
                    String adjusted = refTracker.get(original);
                    if (adjusted != null) {
                        newValues.add(adjusted);
                    } else {
                        // reference doesn't need adjusting, just copy old value
                        newValues.add(original);
                    }
                }
                reference.setProperty(newValues);
            } else {
                String original = reference.property.getValue(Type.STRING);
                String adjusted = refTracker.get(original);
                if (adjusted != null) {
                    reference.setProperty(adjusted);
                }
            }
        }
        refTracker.clear();
    }

    private boolean isNodeType(Tree tree, String ntName) throws RepositoryException {
        return effectiveNodeTypeProvider.isNodeType(tree, ntName);
    }

    private NodeDefinition getDefinition(Tree tree) throws RepositoryException {
        if (tree.isRoot()) {
            return definitionProvider.getRootDefinition();
        } else {
            return definitionProvider.getDefinition(tree.getParent(), tree);
        }
    }

    private static final class Reference {

        private final Tree tree;
        private final PropertyState property;

        private Reference(Tree tree, String propertyName) {
            this.tree = tree;
            this.property = tree.getProperty(propertyName);
        }

        private boolean isMultiple() {
            return property.isArray();
        }

        private void setProperty(String newValue) {
            PropertyState prop = PropertyStates.createProperty(property.getName(), newValue, property.getType().tag());
            tree.setProperty(prop);
        }

        private void setProperty(Iterable<String> newValues) {
            PropertyState prop = PropertyStates.createProperty(property.getName(), newValues, property.getType());
            tree.setProperty(prop);
        }
    }

    /**
     * Resolves 'uuid' property values to {@code Tree} objects and optionally
     * keeps track of newly imported UUIDs.
     */
    private static final class IdResolver {
        /**
         * There are two IdentifierManagers used.
         *
         * 1) currentStateIdManager - Associated with current root on which all import
         *    operations are being performed
         *
         * 2) baseStateIdManager - Associated with the initial root on which
         *    no modifications are performed
         */
        private final IdentifierManager currentStateIdManager;
        private final IdentifierManager baseStateIdManager;

        /**
         * Set of newly created uuid from nodes which are
         * created in this import, which are only remembered if the editing
         * session doesn't have any pending transient changes preventing this
         * performance optimisation from working properly (see OAK-2246).
         */
        private final Set<String> importedUUIDs;

        private IdResolver(@Nonnull Root root, @Nonnull ContentSession contentSession) {
            currentStateIdManager = new IdentifierManager(root);
            baseStateIdManager = new IdentifierManager(contentSession.getLatestRoot());

            if (!root.hasPendingChanges()) {
                importedUUIDs = new HashSet<String>();
            } else {
                importedUUIDs = null;
            }
        }


        @CheckForNull
        private Tree getConflictingTree(@Nonnull String id) {
            //1. First check from base state that tree corresponding to
            //this id exist
            Tree conflicting = baseStateIdManager.getTree(id);
            if (conflicting == null && importedUUIDs != null) {
                //1.a. Check if id is found in newly created nodes
                if (importedUUIDs.contains(id)) {
                    conflicting = currentStateIdManager.getTree(id);
                }
            } else {
                //1.b Re obtain the conflicting tree from Id Manager
                //associated with current root. Such that any operation
                //on it gets reflected in later operations
                //In case a tree with same id was removed earlier then it
                //would return null
                conflicting = currentStateIdManager.getTree(id);
            }
            return conflicting;
        }

        private void rememberImportedUUIDs(@CheckForNull Tree tree) {
            if (tree == null || importedUUIDs == null) {
                return;
            }

            String uuid = TreeUtil.getString(tree, JcrConstants.JCR_UUID);
            if (uuid != null) {
                importedUUIDs.add(uuid);
            }

            for (Tree child : tree.getChildren()) {
                rememberImportedUUIDs(child);
            }
        }
    }
}
