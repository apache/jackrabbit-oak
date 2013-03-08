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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemExistsException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.NamespaceHelper;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>SessionImporter</code> ...
 */
public class SessionImporter implements Importer {

    private static Logger log = LoggerFactory.getLogger(SessionImporter.class);

    private final Session session;
    private final Node importTargetNode;
    private final Root root;
    private final int uuidBehavior;

    private final NamespaceHelper namespaceHelper;
    private Stack<Node> parents;

    /**
     * helper object that keeps track of remapped uuid's and imported reference
     * properties that might need correcting depending on the uuid mappings
     */
    private final ReferenceChangeTracker refTracker;

    //TODO clarify how to provide ProtectedItemImporters
    private final List<ProtectedItemImporter> pItemImporters = new ArrayList<ProtectedItemImporter>();

    /**
     * Currently active importer for protected nodes.
     */
    private ProtectedNodeImporter pnImporter = null;

    /**
     * Creates a new <code>SessionImporter</code> instance.
     */
    public SessionImporter(Node importTargetNode,
                           Root root,
                           Session session,
                           SessionDelegate dlg,
                           NamespaceHelper helper,
                           UserConfiguration userConfig,
                           AccessControlConfiguration accessControlConfig,
                           int uuidBehavior) throws RepositoryException {
        this.importTargetNode = importTargetNode;
        this.session = session;
        this.root = root;
        this.namespaceHelper = helper;
        this.uuidBehavior = uuidBehavior;

        refTracker = new ReferenceChangeTracker();

        parents = new Stack<Node>();
        parents.push(importTargetNode);

        pItemImporters.clear();

        //TODO clarify how to provide ProtectedItemImporters
        for (ProtectedItemImporter importer : userConfig.getProtectedItemImporters()) {
            if (importer.init(session, root, dlg.getNamePathMapper(), false, uuidBehavior, refTracker)) {
                pItemImporters.add(importer);
            }
        }
        for (ProtectedItemImporter importer : accessControlConfig.getProtectedItemImporters()) {
            if (importer.init(session, root, dlg.getNamePathMapper(), false, uuidBehavior, refTracker)) {
                pItemImporters.add(importer);
            }
        }
    }

    /**
     * make sure the editing session is allowed create nodes with a
     * specified node type (and ev. mixins),<br>
     * NOTE: this check is not executed in a single place as the parent
     * may change in case of
     * {@link javax.jcr.ImportUUIDBehavior#IMPORT_UUID_COLLISION_REPLACE_EXISTING IMPORT_UUID_COLLISION_REPLACE_EXISTING}.
     *
     * @param parent   parent node
     * @param nodeName the name
     * @throws javax.jcr.RepositoryException if an error occurs
     */
    protected void checkPermission(Node parent, String nodeName)
            throws RepositoryException {
        //TODO clarify how to check permissions
//        if (!session.getAccessControlManager().isGranted(session.getQPath(parent.getPath()), nodeName, Permissions.NODE_TYPE_MANAGEMENT)) {
//            throw new AccessDeniedException("Insufficient permission.");
//        }
    }

    protected Node createNode(Node parent,
                              String nodeName,
                              String nodeTypeName,
                              String[] mixinNames,
                              String uuid)
            throws RepositoryException {
        Node node;

        // add node
        node = parent.addNode(nodeName, nodeTypeName == null ? namespaceHelper.getJcrName(NamespaceRegistry.NAMESPACE_NT, "unstructured") : nodeTypeName);
        if (uuid != null) {
            root.getTree(node.getPath()).setProperty(NamespaceRegistry.PREFIX_JCR + ":uuid", uuid);
        }
        // add mixins
        if (mixinNames != null) {
            for (String mixinName : mixinNames) {
                node.addMixin(mixinName);
            }
        }
        return node;
    }


    protected void createProperty(Node node, PropInfo pInfo, PropertyDefinition def) throws RepositoryException {
        // convert serialized values to Value objects
        Value[] va = pInfo.getValues(pInfo.getTargetType(def));

        // multi- or single-valued property?
        String name = pInfo.getName();
        int type = pInfo.getType();
        if (va.length == 1 && !def.isMultiple()) {
            Exception e = null;
            try {
                // set single-value
                node.setProperty(name, va[0]);
            } catch (ValueFormatException vfe) {
                e = vfe;
            } catch (ConstraintViolationException cve) {
                e = cve;
            }
            if (e != null) {
                // setting single-value failed, try setting value array
                // as a last resort (in case there are ambiguous property
                // definitions)
                node.setProperty(name, va, type);
            }
        } else {
            // can only be multi-valued (n == 0 || n > 1)
            node.setProperty(name, va, type);
        }
        if (type == PropertyType.REFERENCE || type == PropertyType.WEAKREFERENCE) {
            // store reference for later resolution
            refTracker.processedReference(node.getProperty(name));
        }
    }

    protected Node resolveUUIDConflict(Node parent,
                                       String conflictingId,
                                       NodeInfo nodeInfo)
            throws RepositoryException {
        Node node;
        Node conflicting;
        try {
            conflicting = session.getNodeByIdentifier(conflictingId);
        } catch (ItemNotFoundException infe) {
            // conflicting node can't be read,
            // most likely due to lack of read permission
            conflicting = null;
        }

        if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW) {
            // create new with new uuid
            checkPermission(parent, nodeInfo.getName());
            node = createNode(parent, nodeInfo.getName(),
                    nodeInfo.getPrimaryTypeName(), nodeInfo.getMixinTypeNames(), null);
            // remember uuid mapping
            if (node.isNodeType(JcrConstants.MIX_REFERENCEABLE)) {
                refTracker.put(nodeInfo.getUUID(), node.getIdentifier());
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
            if (importTargetNode.getPath().startsWith(conflicting.getPath())) {
                String msg = "cannot remove ancestor node";
                log.debug(msg);
                throw new ConstraintViolationException(msg);
            }
            // remove conflicting
            conflicting.remove();
            // create new with given uuid
            checkPermission(parent, nodeInfo.getName());
            node = createNode(parent, nodeInfo.getName(),
                    nodeInfo.getPrimaryTypeName(), nodeInfo.getMixinTypeNames(), nodeInfo.getUUID());
        } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING) {
            if (conflicting == null) {
                // since the conflicting node can't be read,
                // we can't replace it
                String msg = "node with uuid " + conflictingId + " cannot be replaced";
                log.debug(msg);
                throw new RepositoryException(msg);
            }

            if (conflicting.getDepth() == 0) {
                String msg = "root node cannot be replaced";
                log.debug(msg);
                throw new RepositoryException(msg);
            }
            // 'replace' current parent with parent of conflicting
            parent = conflicting.getParent();

            // replace child node
            checkPermission(parent, nodeInfo.getName());
            //TODO ordering! (what happened to replace?)
            conflicting.remove();
            node = createNode(parent, nodeInfo.getName(),
                    nodeInfo.getPrimaryTypeName(), nodeInfo.getMixinTypeNames(), nodeInfo.getUUID());
        } else {
            String msg = "unknown uuidBehavior: " + uuidBehavior;
            log.debug(msg);
            throw new RepositoryException(msg);
        }
        return node;
    }

    //-------------------------------------------------------------< Importer >

    /**
     * {@inheritDoc}
     */
    public void start() throws RepositoryException {
        // nop
    }

    /**
     * {@inheritDoc}
     */
    public void startNode(NodeInfo nodeInfo, List<PropInfo> propInfos)
            throws RepositoryException {
        Node parent = parents.peek();

        // process node

        Node node = null;
        String id = nodeInfo.getUUID();
        String nodeName = nodeInfo.getName();
        String ntName = nodeInfo.getPrimaryTypeName();
        String[] mixins = nodeInfo.getMixinTypeNames();

        if (parent == null) {
            log.debug("Skipping node: " + nodeName);
            // parent node was skipped, skip this child node too
            parents.push(null); // push null onto stack for skipped node
            // notify the p-i-importer
            if (pnImporter != null) {
                pnImporter.startChildInfo(nodeInfo, propInfos);
            }
            return;
        }

        if (parent.getDefinition().isProtected()) {
            // skip protected node
            parents.push(null);
            log.debug("Skipping protected node: " + nodeName);

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
                for (ProtectedItemImporter pni : pItemImporters) {
                    if (pni instanceof ProtectedNodeImporter && ((ProtectedNodeImporter) pni).start(root.getTree(parent.getPath()))) {
                        log.debug("Protected node -> delegated to ProtectedNodeImporter");
                        pnImporter = (ProtectedNodeImporter) pni;
                        pnImporter.startChildInfo(nodeInfo, propInfos);
                        break;
                    } /* else: p-i-Importer isn't able to deal with the protected tree.
                     try next. and if none can handle the passed parent the
                     tree below will be skipped */
                }
            }
            return;
        }

        if (parent.hasNode(nodeName)) {
            // a node with that name already exists...
            Node existing = parent.getNode(nodeName);
            NodeDefinition def = existing.getDefinition();
            if (!def.allowsSameNameSiblings()) {
                // existing doesn't allow same-name siblings,
                // check for potential conflicts
                if (def.isProtected() && existing.isNodeType(ntName)) {
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
                    log.debug("Skipping protected node: " + existing);
                    parents.push(existing);
                    return;
                }
                if (def.isAutoCreated() && existing.isNodeType(ntName)) {
                    // this node has already been auto-created, no need to create it
                    node = existing;
                } else {
                    // edge case: colliding node does have same uuid
                    // (see http://issues.apache.org/jira/browse/JCR-1128)
                    if (!(existing.getIdentifier().equals(id)
                            && (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING
                            || uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING))) {
                        throw new ItemExistsException(
                                "Node with the same UUID exists:" + existing);
                    }
                    // fall through
                }
            }
        }

        if (node == null) {
            // create node
            if (id == null) {
                // no potential uuid conflict, always add new node
                checkPermission(parent, nodeName);
                node = createNode(parent, nodeName, ntName, mixins, id);
            } else {
                // potential uuid conflict
                boolean isConflicting;
                try {
                    // the following is a fail-fast test whether
                    // an item exists (regardless of access control)
                    session.getNodeByIdentifier(id);
                    isConflicting = true;
                } catch (ItemNotFoundException e) {
                    isConflicting = false;
                } catch (RepositoryException e) {
                    log.warn("Access Control Issues?", e);
                    isConflicting = true;
                }

                if (isConflicting) {
                    // resolve uuid conflict
                    node = resolveUUIDConflict(parent, id, nodeInfo);
                    if (node == null) {
                        // no new node has been created, so skip this node
                        parents.push(null); // push null onto stack for skipped node
                        log.debug("Skipping existing node " + nodeInfo.getName());
                        return;
                    }
                } else {
                    // create new with given uuid
                    checkPermission(parent, nodeName);
                    node = createNode(parent, nodeName, ntName, mixins, id);
                }
            }
        }

        // process properties

        //TODO remove hack that processes principal name first
        int principalNameIndex = -1;
        for (int k = 0; k < propInfos.size(); k++) {
            PropInfo propInfo = propInfos.get(k);
            if ("rep:principalName".equals(propInfo.getName())) {
                principalNameIndex = k;
                break;
            }
        }
        if (principalNameIndex >= 0) {
            propInfos.add(0, propInfos.remove(principalNameIndex));
        }
        for (PropInfo pi : propInfos) {
            // find applicable definition
            //TODO find a proper way to get the EffectiveNodeTypeProvider
            NodeTypeManager nodeTypeManager = session.getWorkspace().getNodeTypeManager();
            if (nodeTypeManager instanceof EffectiveNodeTypeProvider) {
                EffectiveNodeTypeProvider entp = (EffectiveNodeTypeProvider) nodeTypeManager;

                //TODO find better heuristics?
                PropertyDefinition def = pi.getPropertyDef(entp.getEffectiveNodeType(node));
                if (def.isProtected()) {
                    // skip protected property
                    log.debug("Skipping protected property " + pi.getName());

                    // notify the ProtectedPropertyImporter.
                    for (ProtectedItemImporter ppi : pItemImporters) {
                        if (ppi instanceof ProtectedPropertyImporter && ((ProtectedPropertyImporter) ppi).handlePropInfo(root.getTree(node.getPath()), pi, def)) {
                            log.debug("Protected property -> delegated to ProtectedPropertyImporter");
                            break;
                        } /* else: p-i-Importer isn't able to deal with this property.
                             try next pp-importer */

                    }
                } else {
                    // regular property -> create the property
                    createProperty(node, pi, def);
                }
            } else {
                log.warn("missing EffectiveNodeTypeProvider");
            }
        }

        parents.push(node);
    }


    /**
     * {@inheritDoc}
     */
    public void endNode(NodeInfo nodeInfo) throws RepositoryException {
        Node parent = parents.pop();
        if (parent == null) {
            if (pnImporter != null) {
                pnImporter.endChildInfo();
            }
        } else if (parent.getDefinition().isProtected()) {
            if (pnImporter != null) {
                pnImporter.end(root.getTree(parent.getPath()));
                // and reset the pnImporter field waiting for the next protected
                // parent -> selecting again from available importers
                pnImporter = null;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void end() throws RepositoryException {
        /**
         * adjust references that refer to uuid's which have been mapped to
         * newly generated uuid's on import
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
            if (!(ref instanceof Property)) {
                continue;
            }

            Property prop = (Property) ref;
            // being paranoid...
            if (prop.getType() != PropertyType.REFERENCE
                    && prop.getType() != PropertyType.WEAKREFERENCE) {
                continue;
            }
            if (prop.isMultiple()) {
                Value[] values = prop.getValues();
                Value[] newVals = new Value[values.length];
                for (int i = 0; i < values.length; i++) {
                    Value val = values[i];
                    String original = val.getString();
                    String adjusted = refTracker.get(original);
                    if (adjusted != null) {
                        newVals[i] = session.getValueFactory().createValue(
                                session.getNodeByIdentifier(adjusted),
                                prop.getType() != PropertyType.REFERENCE);
                    } else {
                        // reference doesn't need adjusting, just copy old value
                        newVals[i] = val;
                    }
                }
                prop.setValue(newVals);
            } else {
                Value val = prop.getValue();
                String original = val.getString();
                String adjusted = refTracker.get(original);
                if (adjusted != null) {
                    prop.setValue(session.getNodeByIdentifier(adjusted).getIdentifier());
                }
            }
        }
        refTracker.clear();
    }
}
