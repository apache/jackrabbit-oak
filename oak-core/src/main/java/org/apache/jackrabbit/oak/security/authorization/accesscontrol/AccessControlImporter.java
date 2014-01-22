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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link ProtectedNodeImporter} implementation that handles access control lists,
 * entries and restrictions.
 */
public class AccessControlImporter implements ProtectedNodeImporter, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(AccessControlImporter.class);

    private static final int CHILD_STATUS_UNDEFINED = 0;
    private static final int CHILD_STATUS_ACE = 1;
    private static final int CHILD_STATUS_RESTRICTION = 2;

    private AccessControlManager acMgr;
    private PrincipalManager principalManager;
    private ReadOnlyNodeTypeManager ntMgr;

    private boolean initialized = false;
    private int childStatus;

    private JackrabbitAccessControlList acl;
    private MutableEntry entry;

    private int importBehavior;

    //----------------------------------------------< ProtectedItemImporter >---

    @Override
    public boolean init(Session session, Root root, NamePathMapper namePathMapper,
            boolean isWorkspaceImport, int uuidBehavior,
            ReferenceChangeTracker referenceTracker, SecurityProvider securityProvider) {
        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }
        try {
            AuthorizationConfiguration config = securityProvider.getConfiguration(AuthorizationConfiguration.class);
            importBehavior = Util.getImportBehavior(config);

            if (isWorkspaceImport) {
                acMgr = config.getAccessControlManager(root, namePathMapper);
                PrincipalConfiguration pConfig = securityProvider.getConfiguration(PrincipalConfiguration.class);
                principalManager = pConfig.getPrincipalManager(root, namePathMapper);
            } else {
                acMgr = session.getAccessControlManager();
                principalManager = ((JackrabbitSession) session).getPrincipalManager();
            }
            ntMgr = ReadOnlyNodeTypeManager.getInstance(root, namePathMapper);
            initialized = true;
        } catch (RepositoryException e) {
            log.warn("Error while initializing access control importer", e);
        }
        return initialized;
    }

    @Override
    public void processReferences() throws RepositoryException {
        // nothing to do.
    }

    //----------------------------------------------< ProtectedNodeImporter >---

    @Override
    public boolean start(Tree protectedParent) throws RepositoryException {
        checkInitialized();

        // the acl node must have been added during the regular import before
        // this importer is only successfully started if an valid ACL was created.
        acl = getACL(protectedParent);
        return acl != null;
    }

    @Override
    public void end(Tree protectedParent) throws RepositoryException {
        if (acl != null) {
            acMgr.setPolicy(acl.getPath(), acl);
            acl = null;
        } else {
            throw new IllegalStateException("End reached without ACL to write back.");
        }
    }

    @Override
    public void startChildInfo(NodeInfo childInfo, List<PropInfo> propInfos) throws RepositoryException {
        checkInitialized();
        String ntName = childInfo.getPrimaryTypeName();
        if (NT_REP_GRANT_ACE.equals(ntName) || NT_REP_DENY_ACE.equals(ntName)) {
            if (entry != null) {
                throw new ConstraintViolationException("Invalid child node sequence: ACEs may not be nested.");
            }
            entry = new MutableEntry(NT_REP_GRANT_ACE.equals(ntName));
            for (PropInfo prop : propInfos) {
                String name = prop.getName();
                if (REP_PRINCIPAL_NAME.equals(name)) {
                    entry.setPrincipal(prop.getTextValue());
                } else if (REP_PRIVILEGES.equals(name)) {
                    entry.setPrivilegeNames(prop.getTextValues());
                } else {
                    entry.addRestriction(prop);
                }
            }
            childStatus = CHILD_STATUS_ACE;
        } else if (NT_REP_RESTRICTIONS.equals(ntName)) {
            if (entry == null) {
                throw new ConstraintViolationException("Invalid child node sequence: Restriction must be associated with an ACE");
            }
            entry.addRestrictions(propInfos);
            childStatus = CHILD_STATUS_RESTRICTION;
        } else {
            throw new ConstraintViolationException("Invalid child node with type " + ntName);
        }
    }

    @Override
    public void endChildInfo() throws RepositoryException {
        checkInitialized();
        switch (childStatus) {
            case CHILD_STATUS_ACE:
                // write the ace to the policy
                entry.applyTo(acl);
                entry = null;
                childStatus = CHILD_STATUS_UNDEFINED;
                break;
            case CHILD_STATUS_RESTRICTION:
                // back to ace status
                childStatus = CHILD_STATUS_ACE;
                break;
            default:
                throw new ConstraintViolationException("Invalid child node sequence.");
        }
    }

    //------------------------------------------------------------< private >---
    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
    }

    @CheckForNull
    private JackrabbitAccessControlList getACL(Tree tree) throws RepositoryException {
        String nodeName = tree.getName();

        JackrabbitAccessControlList acl = null;
        if (!tree.isRoot()) {
            Tree parent = tree.getParent();
            if (AccessControlConstants.REP_POLICY.equals(nodeName)
                    && ntMgr.isNodeType(tree, AccessControlConstants.NT_REP_ACL)) {
                acl = getACL(parent.getPath());
            } else if (AccessControlConstants.REP_REPO_POLICY.equals(nodeName)
                    && ntMgr.isNodeType(tree, AccessControlConstants.NT_REP_ACL)
                    && parent.isRoot()) {
                acl = getACL((String) null);
            }
        }

        if (acl != null) {
            // clear all existing entries
            for (AccessControlEntry ace: acl.getAccessControlEntries()) {
                acl.removeAccessControlEntry(ace);
            }
        }

        return acl;
    }

    @CheckForNull
    private JackrabbitAccessControlList getACL(String path) throws RepositoryException {
        JackrabbitAccessControlList acl = null;
        for (AccessControlPolicy p : acMgr.getPolicies(path)) {
            if (p instanceof JackrabbitAccessControlList) {
                acl = (JackrabbitAccessControlList) p;
                break;
            }
        }
        return acl;
    }

    //--------------------------------------------------------------------------
    private final class MutableEntry {

        final boolean isAllow;

        Principal principal;
        List<Privilege> privileges;
        Map<String, Value> restrictions = new HashMap<String, Value>();

        boolean ignore;

        private MutableEntry(boolean isAllow) {
            this.isAllow = isAllow;
        }

        private void setPrincipal(TextValue txtValue) {
            String principalName = txtValue.getString();
            principal = principalManager.getPrincipal(principalName);
            if (principal == null) {
                switch (importBehavior) {
                    case ImportBehavior.IGNORE:
                        log.debug("Unknown principal " + principalName + " -> Ignoring this ACE.");
                        ignore = true;
                        break;
                    case ImportBehavior.ABORT:
                        throw new AccessControlException("Unknown principal " + principalName);
                    case ImportBehavior.BESTEFFORT:
                        principal = new PrincipalImpl(principalName);
                }
            }
        }

        private void setPrivilegeNames(List<? extends TextValue> txtValues) throws RepositoryException {
            privileges = new ArrayList<Privilege>();
            for (TextValue value : txtValues) {
                Value privilegeName = value.getValue(PropertyType.NAME);
                privileges.add(acMgr.privilegeFromName(privilegeName.getString()));
            }
        }

        private void addRestriction(PropInfo propInfo) throws RepositoryException {
            String restrictionName = propInfo.getName();
            int targetType = acl.getRestrictionType(restrictionName);
            restrictions.put(propInfo.getName(), propInfo.getValue(targetType));
        }

        private void addRestrictions(List<PropInfo> propInfos) throws RepositoryException {
            for (PropInfo prop : propInfos) {
                addRestriction(prop);
            }
        }

        private void applyTo(JackrabbitAccessControlList acl) throws RepositoryException {
            checkNotNull(acl);
            if (!ignore) {
                acl.addEntry(principal, privileges.toArray(new Privilege[privileges.size()]), isAllow, restrictions);
            } else {
                log.debug("Unknown principal: Ignore ACE based on ImportBehavior.IGNORE configuration.");
            }
        }
    }
}