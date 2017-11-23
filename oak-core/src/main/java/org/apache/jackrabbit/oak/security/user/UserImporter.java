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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * {@code UserImporter} implements both {@code ode>ProtectedPropertyImporter}
 * and {@code ProtectedNodeImporter} and provides import facilities for protected
 * user and group content defined and used by this user management implementation.<p>
 * <p>
 * The importer is intended to be used by applications that import user content
 * extracted from another repository instance and immediately persist the
 * imported content using {@link javax.jcr.Session#save()}. Omitting the
 * save call will lead to transient, semi-validated user content and eventually
 * to inconsistencies.
 * <p>
 * Note the following restrictions:
 * <ul>
 * <li>The importer will only be initialized if the user manager exposed by
 * the session is an instance of {@code UserManagerImpl}.
 * </li>
 * <li>The importer will only be initialized if the editing session starting
 * this import is the same as the UserManager's Session instance.
 * </li>
 * <li>The jcr:uuid property of user and groups is defined to represent the
 * hashed authorizable id as calculated by the UserManager. This importer
 * is therefore not able to handle imports with
 * {@link ImportUUIDBehavior#IMPORT_UUID_CREATE_NEW}.</li>
 * <li>Importing user/group nodes outside of the hierarchy defined by the two
 * configuration options
 * {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_GROUP_PATH}
 * and {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_USER_PATH}
 * will fail upon {@code Root#commit()}. The same may
 * be true in case of {@link ImportUUIDBehavior#IMPORT_UUID_COLLISION_REPLACE_EXISTING}
 * inserting the user/group node at some other place in the node hierarchy.</li>
 * <li>The same commit hook will make sure that authorizables are never nested
 * and are created below a hierarchy of nt:AuthorizableFolder nodes. This isn't
 * enforced by means of node type constraints but only by the API. This importer
 * itself currently doesn't perform such a validation check.</li>
 * <li>Any attempt to import conflicting data will cause the import to fail
 * either immediately or upon calling {@link javax.jcr.Session#save()} with the
 * following exceptions:
 * <ul>
 * <li>{@code rep:members} : Group membership</li>
 * <li>{@code rep:impersonators} : Impersonators of a User.</li>
 * </ul>
 * The import behavior of these two properties is defined by the {@link #PARAM_IMPORT_BEHAVIOR}
 * configuration parameter, which can be set to
 * <ul>
 * <li>{@link ImportBehavior#NAME_IGNORE ignore}: A warning is logged.</li>
 * <li>{@link ImportBehavior#NAME_BESTEFFORT best effort}: A warning is logged
 * and the importer tries to fix the problem.</li>
 * <li>{@link ImportBehavior#NAME_ABORT abort}: The import is immediately
 * aborted with a ConstraintViolationException. (<strong>default</strong>)</li>
 * </ul>
 * </li>
 * </ul>
 */
class UserImporter implements ProtectedPropertyImporter, ProtectedNodeImporter, UserConstants {

    private static final Logger log = LoggerFactory.getLogger(UserImporter.class);

    private final int importBehavior;

    private Root root;
    private NamePathMapper namePathMapper;
    private ReferenceChangeTracker referenceTracker;
    private UserManagerImpl userManager;
    private IdentifierManager identifierManager;

    private boolean initialized = false;

    /**
     * Container used to collect group members stored in protected nodes.
     */
    private Membership currentMembership;

    /**
     * map holding the processed memberships. this is needed as both, the property and the node importer, can provide
     * memberships during processing. if both would be handled only via the reference tracker {@link Membership#process()}
     * would remove the members from the property importer.
     */
    private Map<String, Membership> memberships = new HashMap<String, Membership>();

    /**
     * Temporary store for the pw an imported new user to be able to call
     * the creation actions irrespective of the order of protected properties
     */
    private String currentPw;

    /**
     * Remember all new principals for impersonation handling.
     */
    private Map<String, Principal> principals = new HashMap<String, Principal>();

    UserImporter(ConfigurationParameters config) {
        importBehavior = UserUtil.getImportBehavior(config);
    }

    //----------------------------------------------< ProtectedItemImporter >---
    @Override
    public boolean init(@Nonnull Session session, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper,
            boolean isWorkspaceImport, int uuidBehavior,
            @Nonnull ReferenceChangeTracker referenceTracker, @Nonnull SecurityProvider securityProvider) {

        if (!(session instanceof JackrabbitSession)) {
            log.debug("Importing protected user content requires a JackrabbitSession");
            return false;
        }

        this.root = root;
        this.namePathMapper = namePathMapper;
        this.referenceTracker = referenceTracker;

        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }
        if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW) {
            log.debug("ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW isn't supported when importing users or groups.");
            return false;
        }

        if (!canInitUserManager((JackrabbitSession) session, isWorkspaceImport, securityProvider)) {
            return false;
        }

        userManager = new UserManagerImpl(root, namePathMapper, securityProvider);

        initialized = true;
        return initialized;
    }

    private static boolean canInitUserManager(@Nonnull JackrabbitSession session, boolean isWorkspaceImport, @Nonnull SecurityProvider securityProvider) {
        try {
            if (!isWorkspaceImport && session.getUserManager().isAutoSave()) {
                log.warn("Session import cannot handle user content: UserManager is in autosave mode.");
                return false;
            }
        } catch (RepositoryException e) {
            // failed to access user manager or to set the autosave behavior
            // -> return false (not initialized) as importer can't operate.
            log.error("Failed to initialize UserImporter: ", e);
            return false;
        }
        return true;
    }

    // -----------------------------------------< ProtectedPropertyImporter >---
    @Override
    public boolean handlePropInfo(@Nonnull Tree parent, @Nonnull PropInfo propInfo, @Nonnull PropertyDefinition def) throws RepositoryException {
        checkInitialized();

        String propName = propInfo.getName();
        if (isPwdNode(parent)) {
            // overwrite any properties generated underneath the rep:pwd node
            // by "UserManagerImpl#setPassword" by the properties defined by
            // the XML to be imported. see OAK-1943 for the corresponding discussion.
            return importPwdNodeProperty(parent, propInfo, def);
        } else {
            Authorizable a = userManager.getAuthorizable(parent);
            if (a == null) {
                log.debug("Cannot handle protected PropInfo " + propInfo + ". Node " + parent + " doesn't represent an Authorizable.");
                return false;
            }

            if (REP_AUTHORIZABLE_ID.equals(propName)) {
                if (!isValid(def, NT_REP_AUTHORIZABLE, false)) {
                    return false;
                }
                String id = propInfo.getTextValue().getString();
                Authorizable existing = userManager.getAuthorizable(id);
                if (existing == null) {
                    String msg = "Cannot handle protected PropInfo " + propInfo + ". Invalid rep:authorizableId.";
                    log.warn(msg);
                    throw new ConstraintViolationException(msg);
                }

                if (a.getPath().equals(existing.getPath())) {
                    parent.setProperty(REP_AUTHORIZABLE_ID, id);
                } else {
                    throw new AuthorizableExistsException(id);
                }
                return true;

            } else if (REP_PRINCIPAL_NAME.equals(propName)) {
                if (!isValid(def, NT_REP_AUTHORIZABLE, false)) {
                    return false;
                }

                String principalName = propInfo.getTextValue().getString();
                Principal principal = new PrincipalImpl(principalName);
                userManager.checkValidPrincipal(principal, a.isGroup());
                userManager.setPrincipal(parent, principal);

            /*
             Remember principal of new user/group for further processing
             of impersonators
             */
                if (principals == null) {
                    principals = new HashMap<String, Principal>();
                }
                principals.put(principalName, a.getPrincipal());

                return true;
            } else if (REP_PASSWORD.equals(propName)) {
                if (a.isGroup() || !isValid(def, NT_REP_USER, false)) {
                    log.warn("Unexpected authorizable or definition for property rep:password");
                    return false;
                }
                if (((User) a).isSystemUser()) {
                    log.warn("System users may not have a password set.");
                    return false;
                }

                String pw = propInfo.getTextValue().getString();
                userManager.setPassword(parent, a.getID(), pw, false);
                currentPw = pw;

                return true;

            } else if (REP_IMPERSONATORS.equals(propName)) {
                if (a.isGroup() || !isValid(def, MIX_REP_IMPERSONATABLE, true)) {
                    log.warn("Unexpected authorizable or definition for property rep:impersonators");
                    return false;
                }

                // since impersonators may be imported later on, postpone processing
                // to the end.
                // see -> process References
                referenceTracker.processedReference(new Impersonators(parent.getPath(), propInfo.getTextValues()));
                return true;

            } else if (REP_DISABLED.equals(propName)) {
                if (a.isGroup() || !isValid(def, NT_REP_USER, false)) {
                    log.warn("Unexpected authorizable or definition for property rep:disabled");
                    return false;
                }

                ((User) a).disable(propInfo.getTextValue().getString());
                return true;

            } else if (REP_MEMBERS.equals(propName)) {
                if (!a.isGroup() || !isValid(def, NT_REP_MEMBER_REFERENCES, true)) {
                    return false;
                }
                // since group-members are references to user/groups that potentially
                // are to be imported later on -> postpone processing to the end.
                // see -> process References
                getMembership(a.getPath()).addMembers(propInfo.getTextValues());
                return true;

            } // another protected property -> return false
        }

        // neither rep:pwd nor authorizable node -> not covered by this importer.
        return false;
    }

    @Override
    public void propertiesCompleted(@Nonnull Tree protectedParent) throws RepositoryException {
        if (isCacheNode(protectedParent)) {
            // remove the cache if present
            protectedParent.remove();
        } else {
            Authorizable a = userManager.getAuthorizable(protectedParent);
            if (a == null) {
                // not an authorizable
                return;
            }

            // make sure the authorizable ID property is always set even if the
            // authorizable defined by the imported XML didn't provide rep:authorizableID
            if (!protectedParent.hasProperty(REP_AUTHORIZABLE_ID)) {
                protectedParent.setProperty(REP_AUTHORIZABLE_ID, a.getID(), Type.STRING);
            }

            /*
            Execute authorizable actions for a NEW user at this point after
            having set the password and the principal name (all protected properties
            have been processed now).
            */
            if (protectedParent.getStatus() == Tree.Status.NEW) {
                if (a.isGroup()) {
                    userManager.onCreate((Group) a);
                } else {
                    userManager.onCreate((User) a, currentPw);
                }
            }
            currentPw = null;
        }
    }

    @Override
    public void processReferences() throws RepositoryException {
        checkInitialized();

        // add all collected memberships to the reference tracker.
        for (Membership m: memberships.values()) {
            referenceTracker.processedReference(m);
        }
        memberships.clear();

        List<Object> processed = new ArrayList<Object>();
        for (Iterator<Object> it = referenceTracker.getProcessedReferences(); it.hasNext(); ) {
            Object reference = it.next();
            if (reference instanceof Membership) {
                ((Membership) reference).process();
                processed.add(reference);
            } else if (reference instanceof Impersonators) {
                ((Impersonators) reference).process();
                processed.add(reference);
            }
        }
        // successfully processed this entry of the reference tracker
        // -> remove from the reference tracker.
        referenceTracker.removeReferences(processed);
    }

    // ---------------------------------------------< ProtectedNodeImporter >---
    @Override
    public boolean start(@Nonnull Tree protectedParent) throws RepositoryException {
        Authorizable auth = null;
        if (isMemberNode(protectedParent)) {
            Tree groupTree = protectedParent;
            while (isMemberNode(groupTree) && !groupTree.isRoot()) {
                groupTree = groupTree.getParent();
            }
            auth = userManager.getAuthorizable(groupTree);
        } else if (isMemberReferencesListNode(protectedParent)) {
            auth = userManager.getAuthorizable(protectedParent.getParent());

        } // else: parent node is not of type rep:Members or rep:MemberReferencesList

        if (auth == null || !auth.isGroup()) {
            log.debug("Cannot handle protected node " + protectedParent + ". It nor one of its parents represent a valid Group.");
            return false;
        } else {
            currentMembership = getMembership(auth.getPath());
            return true;
        }
    }

    @Override
    public void startChildInfo(@Nonnull NodeInfo childInfo, @Nonnull List<PropInfo> propInfos) throws RepositoryException {
        checkState(currentMembership != null);

        String ntName = childInfo.getPrimaryTypeName();
        //noinspection deprecation
        if (NT_REP_MEMBERS.equals(ntName)) {
            for (PropInfo prop : propInfos) {
                for (TextValue tv : prop.getTextValues()) {
                    currentMembership.addMember(tv.getString());
                }
            }
        } else if (NT_REP_MEMBER_REFERENCES.equals(ntName)) {
            for (PropInfo prop : propInfos) {
                if (REP_MEMBERS.equals(prop.getName())) {
                    currentMembership.addMembers(prop.getTextValues());
                }
            }
        } else {
            //noinspection deprecation
            log.warn("{} is not of type " + NT_REP_MEMBERS + " or " + NT_REP_MEMBER_REFERENCES, childInfo.getName());
        }
    }

    @Override
    public void endChildInfo() throws RepositoryException {
        // nothing to do
    }

    @Override
    public void end(@Nonnull Tree protectedParent) throws RepositoryException {
        currentMembership = null;
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private IdentifierManager getIdentifierManager() {
        if (identifierManager == null) {
            identifierManager = new IdentifierManager(root);
        }
        return identifierManager;
    }

    @Nonnull
    private PrincipalManager getPrincipalManager() throws RepositoryException {
        return userManager.getPrincipalManager();
    }

    @Nonnull
    private Membership getMembership(@Nonnull String authId) {
        Membership membership = memberships.get(authId);
        if (membership == null) {
            membership = new Membership(authId);
            memberships.put(authId, membership);
        }
        return membership;
    }

    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
    }

    private boolean isValid(@Nonnull PropertyDefinition definition, @Nonnull String oakNodeTypeName, boolean multipleStatus) {
        return multipleStatus == definition.isMultiple() &&
                definition.getDeclaringNodeType().isNodeType(namePathMapper.getJcrName(oakNodeTypeName));
    }

    private static boolean isMemberNode(@Nonnull Tree tree) {
        //noinspection deprecation
        return tree.exists() && NT_REP_MEMBERS.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    private static boolean isMemberReferencesListNode(@Nonnull Tree tree) {
        return tree.exists() && NT_REP_MEMBER_REFERENCES_LIST.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    private static boolean isPwdNode(@Nonnull Tree tree) {
        return REP_PWD.equals(tree.getName()) && NT_REP_PASSWORD.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    private static boolean importPwdNodeProperty(@Nonnull Tree parent, @Nonnull PropInfo propInfo, @Nonnull PropertyDefinition def) throws RepositoryException {
        String propName = propInfo.getName();
        if (propName == null) {
            propName = def.getName();
            if (propName == null || NodeTypeConstants.RESIDUAL_NAME.equals(propName)) {
                return false;
            }
        }

        // overwrite any properties generated underneath the rep:pwd node
        // by "UserManagerImpl#setPassword" by the properties defined by
        // the XML to be imported. see OAK-1943 for the corresponding discussion.
        int targetType = def.getRequiredType();
        if (targetType == PropertyType.UNDEFINED) {
            targetType = (REP_PASSWORD_LAST_MODIFIED.equals(propName)) ? PropertyType.LONG : PropertyType.STRING;
        }
        PropertyState property;
        if (def.isMultiple()) {
            property = PropertyStates.createProperty(propName, propInfo.getValues(targetType));
        } else {
            property = PropertyStates.createProperty(propName, propInfo.getValue(targetType));
        }
        parent.setProperty(property);
        return true;
    }

    private static boolean isCacheNode(@Nonnull Tree tree) {
        return tree.exists() && CacheConstants.REP_CACHE.equals(tree.getName()) && CacheConstants.NT_REP_CACHE.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    /**
     * Handling the import behavior
     *
     * @param msg The message to log a warning in case of {@link ImportBehavior#IGNORE}
     *            or {@link ImportBehavior#BESTEFFORT}
     * @throws javax.jcr.nodetype.ConstraintViolationException
     *          If the import
     *          behavior is {@link ImportBehavior#ABORT}.
     */
    private void handleFailure(String msg) throws ConstraintViolationException {
        switch (importBehavior) {
            case ImportBehavior.IGNORE:
            case ImportBehavior.BESTEFFORT:
                log.warn(msg);
                break;
            case ImportBehavior.ABORT:
                throw new ConstraintViolationException(msg);
            default:
                // no other behavior. nothing to do.

        }
    }

    //------------------------------------------------------< inner classes >---

    /**
     * Inner class used to postpone import of group membership to the very end
     * of the import. This allows to import membership of user/groups that
     * are only being created during this import.
     *
     * @see ImportBehavior For additional configuration options.
     */
    private final class Membership {

        private final String authorizablePath;

        private final Set<String> members = new TreeSet<String>();

        Membership(String authorizablePath) {
            this.authorizablePath = authorizablePath;
        }

        void addMember(String id) {
            members.add(id);
        }

        void addMembers(List<? extends TextValue> tvs) {
            for (TextValue tv : tvs) {
                addMember(tv.getString());
            }
        }

        void process() throws RepositoryException {
            Authorizable a = userManager.getAuthorizableByPath(authorizablePath);
            if (a == null || !a.isGroup()) {
                throw new RepositoryException(authorizablePath + " does not represent a valid group.");
            }
            Group gr = (Group) a;

            // 1. collect members to add and to remove.
            Map<String, Authorizable> toRemove = new HashMap<String, Authorizable>();
            for (Iterator<Authorizable> declMembers = gr.getDeclaredMembers(); declMembers.hasNext(); ) {
                Authorizable dm = declMembers.next();
                toRemove.put(dm.getID(), dm);
            }

            Map<String, Authorizable> toAdd = Maps.newHashMapWithExpectedSize(members.size());
            Map<String, String> nonExisting = Maps.newHashMap();

            for (String contentId : members) {
                String remapped = referenceTracker.get(contentId);
                String memberContentId = (remapped == null) ? contentId : remapped;

                Authorizable member = null;
                try {
                    Tree n = getIdentifierManager().getTree(memberContentId);
                    member = userManager.getAuthorizable(n);
                } catch (RepositoryException e) {
                    // no such node or failed to retrieve authorizable
                    // warning is logged below.
                }
                if (member != null) {
                    if (toRemove.remove(member.getID()) == null) {
                        toAdd.put(member.getID(), member);
                    } // else: no need to remove from rep:members
                } else {
                    handleFailure("New member of " + gr + ": No such authorizable (NodeID = " + memberContentId + ')');
                    if (importBehavior == ImportBehavior.BESTEFFORT) {
                        log.debug("ImportBehavior.BESTEFFORT: Remember non-existing member for processing.");
                        /* since we ignore the set of failed ids later on and
                           don't know the real memberId => use fake memberId as
                           value in the map */
                        nonExisting.put(contentId, "-");
                    }
                }
            }

            // 2. adjust members of the group
            if (!toRemove.isEmpty()) {
                Set<String> failed = gr.removeMembers(toRemove.keySet().toArray(new String[toRemove.size()]));
                if (!failed.isEmpty()) {
                    handleFailure("Failed removing members " + Iterables.toString(failed) + " to " + gr);
                }
            }

            if (!toAdd.isEmpty()) {
                Set<String> failed = gr.addMembers(toAdd.keySet().toArray(new String[toAdd.size()]));
                if (!failed.isEmpty()) {
                    handleFailure("Failed add members " + Iterables.toString(failed) + " to " + gr);
                }
            }

            // handling non-existing members in case of best-effort
            if (!nonExisting.isEmpty()) {
                log.debug("ImportBehavior.BESTEFFORT: Found " + nonExisting.size() + " entries of rep:members pointing to non-existing authorizables. Adding to rep:members.");
                Tree groupTree = root.getTree(gr.getPath());

                MembershipProvider membershipProvider = userManager.getMembershipProvider();

                Set<String> memberContentIds = Sets.newHashSet(nonExisting.keySet());
                Set<String> failedContentIds = membershipProvider.addMembers(groupTree, nonExisting);
                memberContentIds.removeAll(failedContentIds);

                userManager.onGroupUpdate(gr, false, true, memberContentIds, failedContentIds);
            }
        }
    }

    /**
     * Inner class used to postpone import of impersonators to the very end
     * of the import. This allows to import impersonation values pointing
     * to user that are only being created during this import.
     *
     * @see ImportBehavior For additional configuration options.
     */
    private final class Impersonators {

        private final String userPath;
        private final Set<String> principalNames = new HashSet<String>();

        private Impersonators(String userPath, List<? extends TextValue> values) {
            this.userPath = userPath;
            for (TextValue v : values) {
                principalNames.add(v.getString());
            }
        }

        private void process() throws RepositoryException {
            Authorizable a = userManager.getAuthorizableByOakPath(userPath);
            if (a == null || a.isGroup()) {
                throw new RepositoryException(userPath + " does not represent a valid user.");
            }

            Impersonation imp = checkNotNull(((User) a).getImpersonation());

            // 1. collect principals to add and to remove.
            Map<String, Principal> toRemove = new HashMap<String, Principal>();
            for (PrincipalIterator pit = imp.getImpersonators(); pit.hasNext(); ) {
                Principal p = pit.nextPrincipal();
                toRemove.put(p.getName(), p);
            }

            List<String> toAdd = new ArrayList<String>();
            for (final String principalName : principalNames) {
                if (toRemove.remove(principalName) == null) {
                    // add it to the list of new impersonators to be added.
                    toAdd.add(principalName);
                } // else: no need to revoke impersonation for the given principal.
            }

            // 2. adjust set of impersonators
            for (Principal p : toRemove.values()) {
                if (!imp.revokeImpersonation(p)) {
                    String principalName = p.getName();
                    handleFailure("Failed to revoke impersonation for " + principalName + " on " + a);
                }
            }
            List<String> nonExisting = new ArrayList<String>();
            for (String principalName : toAdd) {
                Principal principal = (principals.containsKey(principalName)) ?
                        principals.get(principalName) :
                        new PrincipalImpl(principalName);
                if (!imp.grantImpersonation(principal)) {
                    handleFailure("Failed to grant impersonation for " + principalName + " on " + a);
                    if (importBehavior == ImportBehavior.BESTEFFORT &&
                            getPrincipalManager().getPrincipal(principalName) == null) {
                        log.debug("ImportBehavior.BESTEFFORT: Remember non-existing impersonator for special processing.");
                        nonExisting.add(principalName);
                    }
                }
            }

            if (!nonExisting.isEmpty()) {
                Tree userTree = checkNotNull(root.getTree(a.getPath()));
                // copy over all existing impersonators to the nonExisting list
                PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
                if (impersonators != null) {
                    for (String existing : impersonators.getValue(STRINGS)) {
                        nonExisting.add(existing);
                    }
                }
                // and write back the complete list including those principal
                // names that are unknown to principal provider.
                userTree.setProperty(REP_IMPERSONATORS, nonExisting, Type.STRINGS);
            }
        }
    }
}