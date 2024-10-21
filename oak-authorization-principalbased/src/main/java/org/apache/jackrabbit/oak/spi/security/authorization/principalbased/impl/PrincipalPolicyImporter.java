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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NODE_PATH;

/**
 * Implementation of the {@link ProtectedNodeImporter} and {@link ProtectedPropertyImporter}for principal policies.
 */
class PrincipalPolicyImporter implements ProtectedNodeImporter, ProtectedPropertyImporter, Constants {

    private static final Logger log = LoggerFactory.getLogger(PrincipalPolicyImporter.class);

    private Session session;
    private final MgrProvider mgrProvider;
    private final FilterProvider filterProvider;
    private Filter filter;

    private AuthorizationConfiguration authorizationConfiguration;

    private int importBehavior;

    private boolean initialized;

    private PrincipalPolicyImpl policy;
    private Entry entry;

    PrincipalPolicyImporter(@NotNull FilterProvider filterProvider, @NotNull MgrProvider mgrProvider) {
        this.filterProvider = filterProvider;
        this.mgrProvider = mgrProvider;
    }

    //----------------------------------------------< ProtectedItemImporter >---
    @Override
    public boolean init(@NotNull Session session, @NotNull Root root, @NotNull NamePathMapper namePathMapper, boolean isWorkspaceImport, int uuidBehavior, @NotNull ReferenceChangeTracker referenceTracker, @NotNull SecurityProvider securityProvider) {
        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }
        this.session = session;
        mgrProvider.reset(root, namePathMapper);
        filter = filterProvider.getFilter(mgrProvider.getSecurityProvider(), root, namePathMapper);

        authorizationConfiguration = securityProvider.getConfiguration(AuthorizationConfiguration.class);
        importBehavior = ImportBehavior.valueFromString(authorizationConfiguration.getParameters().getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT));

        initialized = true;
        return initialized;
    }

    @Override
    public void processReferences() {
        // nothing to do
    }

    //------------------------------------------< ProtectedPropertyImporter >---
    @Override
    public boolean handlePropInfo(@NotNull Tree parent, @NotNull PropInfo protectedPropInfo, @NotNull PropertyDefinition def) throws RepositoryException {
        Validate.checkState(initialized);

        if (!Utils.isPrincipalPolicyTree(parent) || !isValidPrincipalProperty(protectedPropInfo, def)) {
            return false;
        }
        String accessControlledPath = PathUtils.getParentPath(parent.getPath());
        if (!filterProvider.handlesPath(accessControlledPath)) {
            log.debug("Unable to import principal policy. Access controlled path '{}' outside of path supported by FilterProvider.", accessControlledPath);
            return false;
        }

        String principalName = protectedPropInfo.getTextValue().getString();
        Principal principal = filter.getValidPrincipal(accessControlledPath);
        if (principal == null) {
            log.debug("Unable to lookup principal by path = {}. Creating by name {}.", accessControlledPath, principalName);
            principal = new PrincipalImpl(principalName);
        } else if (!principalName.equals(principal.getName())) {
            log.error("Principal name mismatch expected '{}' but was '{}'.", principalName, principal.getName());
            throw new ConstraintViolationException("Principal name mismatch.");
        }
        if (!Utils.canHandle(principal, filter, importBehavior)) {
            log.debug("Cannot handle principal {} with name = {}", principal, principalName);
            return false;
        }
        // make sure editing session has ability to read access control
        session.checkPermission(accessControlledPath, Permissions.getString(Permissions.READ_ACCESS_CONTROL));
        policy = new PrincipalPolicyImpl(principal, accessControlledPath, mgrProvider);
        return true;
    }

    @Override
    public void propertiesCompleted(@NotNull Tree protectedParent) throws RepositoryException {
        Validate.checkState(initialized);

        // make sure also an empty policy (with entries) is being written (see also #end(Tree) below)
        if (policy != null) {
            if (isValidProtectedParent(protectedParent, policy)) {
                getAccessControlManager().setPolicy(policy.getPath(), policy);
            } else {
                log.warn("Protected parent {} does not match path of PrincipalAccessControlList {}.", protectedParent.getPath(), policy.getOakPath());
                getAccessControlManager().removePolicy(policy.getPath(), policy);
            }
        }
    }

    //----------------------------------------------< ProtectedNodeImporter >---
    @Override
    public boolean start(@NotNull Tree protectedParent) {
        Validate.checkState(initialized);

        // the policy node was added during the regular import (it's parent must not be protected)
        // and the principal-name property must have been processed by the ProtectedPropertyImporter
        return policy != null && isValidProtectedParent(protectedParent, policy);
    }

    @Override
    public void end(@NotNull Tree protectedParent) throws RepositoryException {
        Validate.checkState(policy != null);

        if (isValidProtectedParent(protectedParent, policy)) {
            getAccessControlManager().setPolicy(policy.getPath(), policy);
        } else {
            log.warn("Protected parent {} does not match path of PrincipalAccessControlList {}.", protectedParent.getPath(), policy.getOakPath());
            getAccessControlManager().removePolicy(policy.getPath(), policy);
        }
        policy = null;
    }

    @Override
    public void startChildInfo(@NotNull NodeInfo childInfo, @NotNull List<PropInfo> propInfos) throws RepositoryException {
        Validate.checkState(policy != null);
        String ntName = getOakName(childInfo.getPrimaryTypeName());
        if (NT_REP_PRINCIPAL_ENTRY.equals(ntName)) {
            if (entry != null) {
                throw new ConstraintViolationException("Invalid child node sequence: Entries must not be nested.");
            }
            entry = new Entry(propInfos);
        } else if (NT_REP_RESTRICTIONS.equals(ntName)) {
            if (entry == null) {
                throw new ConstraintViolationException("Invalid child node sequence: Restriction must be associated with an Entry");
            }
            entry.addRestrictions(propInfos);
        } else {
            throw new ConstraintViolationException("Invalid child node '"+childInfo.getName()+"' with type " + ntName);
        }
    }

    @Override
    public void endChildInfo() throws RepositoryException {
        Validate.checkState(policy != null);
        if (entry != null) {
            entry.applyTo(policy);
            // reset the child entry
            entry = null;
        }
    }

    //------------------------------------------------------------< private >---

    private boolean isValidPrincipalProperty(@NotNull PropInfo propertyInfo, @NotNull PropertyDefinition def) {
        return REP_PRINCIPAL_NAME.equals(getOakName(propertyInfo.getName())) &&
                !def.isMultiple() &&
                NT_REP_PRINCIPAL_POLICY.equals(getOakName(def.getDeclaringNodeType().getName()));
    }

    private static boolean isValidProtectedParent(@NotNull Tree protectedParent, @NotNull PrincipalPolicyImpl policy) {
        String accessControlledPath = PathUtils.getParentPath(protectedParent.getPath());
        return accessControlledPath.equals(policy.getOakPath());
    }

    @Nullable
    private String getOakName(@Nullable String name) {
        return (name == null) ? null : getNamePathMapper().getOakNameOrNull(name);
    }

    private AccessControlManager getAccessControlManager() {
        return authorizationConfiguration.getAccessControlManager(mgrProvider.getRoot(), mgrProvider.getNamePathMapper());
    }

    private NamePathMapper getNamePathMapper() {
        return mgrProvider.getNamePathMapper();
    }

    //--------------------------------------------------------------------------
    private final class Entry {

        private String effectivePath;

        private final Iterable<Privilege> privileges;
        private final Map<String, Value> restrictions = new HashMap<>();
        private final Map<String, Value[]> mvRestrictions = new HashMap<>();

        private Entry(@NotNull List<PropInfo> propInfos) throws RepositoryException {
            Iterable<Privilege> privs = null;
            for (PropInfo prop : propInfos) {
                String oakName = getOakName(prop.getName());
                if (REP_EFFECTIVE_PATH.equals(oakName) && PropertyType.PATH == prop.getType()) {
                    effectivePath = extractEffectivePath(prop);
                } else if (REP_PRIVILEGES.equals(oakName) && PropertyType.NAME == prop.getType()) {
                    privs = getPrivileges(Iterables.transform(prop.getTextValues(), TextValue::getString));
                } else {
                    throw new ConstraintViolationException("Unsupported property '"+oakName+"' with type "+prop.getType()+" within policy entry of type rep:PrincipalEntry");
                }
            }
            if (privs == null) {
                throw new ConstraintViolationException("Entries for PrincipalAccessControlList must specify the privileges to be granted.");
            }
            privileges = privs;
        }

        private List<Privilege> getPrivileges(@NotNull Iterable<String> jcrPrivNames) throws RepositoryException {
            List<Privilege> privs = new ArrayList<>();
            PrivilegeManager privilegeManager = mgrProvider.getPrivilegeManager();
            for (String privName : jcrPrivNames) {
                privs.add(privilegeManager.getPrivilege(privName));
            }
            return privs;
        }

        private void addRestrictions(@NotNull List<PropInfo> propInfos) throws RepositoryException {
            Validate.checkState(restrictions.isEmpty() && mvRestrictions.isEmpty(), "Multiple restriction nodes.");
            for (PropInfo prop : propInfos) {
                String restrictionName = prop.getName();
                if (REP_NODE_PATH.equals(getOakName(restrictionName))) {
                    Validate.checkState(effectivePath == null, "Attempt to overwrite rep:effectivePath property with rep:nodePath restriction.");
                    log.debug("Extracting rep:effectivePath from rep:nodePath restriction.");
                    effectivePath = extractEffectivePath(prop);
                } else {
                    int targetType = policy.getRestrictionType(restrictionName);
                    List<Value> values = prop.getValues(targetType);
                    if (policy.isMultiValueRestriction(restrictionName)) {
                        mvRestrictions.put(restrictionName, values.toArray(new Value[0]));
                    } else {
                        restrictions.put(restrictionName, values.get(0));
                    }
                }
            }
        }

        /**
         * Work around the fact that {@link org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl#getJcrName(String)}
         * transforms an empty path value to the current element ("."), which is not a valid path for {@code rep:effectivePath},
         * which expects an absolute path or null.
         *
         * @param prop The prop info containing the effective path.
         * @return An empty string if the text value of the given property info is "." or the text value otherwise.
         * @throws RepositoryException
         */
        private String extractEffectivePath(@NotNull PropInfo prop) throws RepositoryException {
            String ep = prop.getTextValue().getString();
            return (PathUtils.denotesCurrent(ep)) ? "" : ep;
        }

        private void applyTo(@NotNull PrincipalPolicyImpl policy) throws RepositoryException {
            if (effectivePath == null) {
                log.error("Missing rep:effectivePath for entry {} of policy at {}", this, policy.getOakPath());
                throw new ConstraintViolationException("Entries for PrincipalAccessControlList must specify an effective path.");
            }
            policy.addEntry(Strings.emptyToNull(effectivePath), Iterables.toArray(privileges, Privilege.class), restrictions, mvRestrictions);
        }
    }
}
