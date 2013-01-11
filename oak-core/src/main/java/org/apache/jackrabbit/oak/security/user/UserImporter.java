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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * {@code UserImporter} implements both {@code ode>ProtectedPropertyImporter}
 * and {@code ProtectedNodeImporter} and provides import facilities for protected
 * user and group content defined and used by this user management implementation.<p/>
 * <p/>
 * The importer is intended to be used by applications that import user content
 * extracted from another repository instance and immediately persist the
 * imported content using {@link javax.jcr.Session#save()}. Omitting the
 * save call will lead to transient, semi-validated user content and eventually
 * to inconsistencies.
 * <p/>
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
public class UserImporter implements ProtectedPropertyImporter, ProtectedNodeImporter {

    private static final Logger log = LoggerFactory.getLogger(UserImporter.class);

    /**
     * Parameter name for the import behavior configuration option.
     */
    public static final String PARAM_IMPORT_BEHAVIOR = "importBehavior";

    private final int importBehavior;

    private JackrabbitSession session;
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
     * Temporary store for the pw an imported new user to be able to call
     * the creation actions irrespective of the order of protected properties
     */
    private Map<String, String> currentPw = new HashMap<String, String>(1);

    UserImporter(ConfigurationParameters config) {
        // TODO: review definition of import-behavior configuration
        String importBehaviorStr = config.getConfigValue(PARAM_IMPORT_BEHAVIOR, ImportBehavior.nameFromValue(ImportBehavior.IGNORE));
        importBehavior = ImportBehavior.valueFromString(importBehaviorStr);
    }

    //----------------------------------------------< ProtectedItemImporter >---
    @Override
    public boolean init(Session session, Root root, NamePathMapper namePathMapper,
                        boolean isWorkspaceImport, int uuidBehavior,
                        ReferenceChangeTracker referenceTracker) {

        if (!(session instanceof JackrabbitSession)) {
            log.debug("Importing protected user content requires a JackrabbitSession");
            return false;
        }

        this.session = (JackrabbitSession) session;
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

        if (!initUserManager(isWorkspaceImport)) {
            return false;
        }

        initialized = true;
        return initialized;
    }

    private boolean initUserManager(boolean isWorkspaceImport) {
        try {
            UserManager uMgr = session.getUserManager();
            if (uMgr instanceof UserManagerImpl) {
                UserManagerImpl impl = (UserManagerImpl) uMgr;
                if (isWorkspaceImport) {
                    // use a separate user manager that is not associated with
                    // transient session modifications.
                    userManager = new UserManagerImpl(root, namePathMapper, impl.getSecurityProvider());
                    return true;
                } else {
                    if (impl.isAutoSave()) {
                        log.warn("Session import cannot handle user content: UserManager is in autosave mode.");
                    } else {
                        userManager = impl;
                        return true;
                    }
                }
            } else {
                log.debug("Failed to initialize UserImporter: Instanceof UserManagerImpl expected.");
            }
        } catch (RepositoryException e) {
            // failed to access user manager or to set the autosave behavior
            // -> return false (not initialized) as importer can't operate.
            log.error("Failed to initialize UserImporter: ", e);
        }
        return false;
    }

    // -----------------------------------------< ProtectedPropertyImporter >---
    @Override
    public boolean handlePropInfo(Tree parent, PropInfo propInfo, PropertyDefinition def) throws RepositoryException {
        checkInitialized();

        Authorizable a = userManager.getAuthorizable(parent);
        if (a == null) {
            log.warn("Cannot handle protected PropInfo " + propInfo + ". Node " + parent + " doesn't represent a valid Authorizable.");
            return false;
        }

        String propString = propInfo.getName();
        if (UserConstants.REP_PRINCIPAL_NAME.equals(propString)) {
            if (!isValid(propString, def, UserConstants.NT_REP_AUTHORIZABLE, false)) {
                return false;
            }

            String principalName = propInfo.getTextValue().getString();
            userManager.setPrincipal(parent, new TreeBasedPrincipal(principalName, parent, namePathMapper));
            /*
            Execute authorizable actions for a NEW group as this is the
            same place in the userManager#createGroup that the actions
            are called.
            In case of a NEW user the actions are executed if the password
            has been imported before.
            */
            if (parent.getStatus() == Tree.Status.NEW) {
                if (a.isGroup()) {
                    userManager.onCreate((Group) a);
                } else if (currentPw.containsKey(a.getID())) {
                    userManager.onCreate((User) a, currentPw.remove(a.getID()));
                }
            }
            return true;
        } else if (UserConstants.REP_PASSWORD.equals(propString)) {
            if (a.isGroup() || !isValid(propString, def, UserConstants.NT_REP_USER, false)) {
                log.warn("Unexpected authorizable or definition for property rep:password");
                return false;
            }

            String pw = propInfo.getTextValue().getString();
            userManager.setPassword(parent, pw, false);

            /*
            Execute authorizable actions for a NEW user at this point after
            having set the password if the principal name has already been
            processed, otherwise postpone it.
            */
            if (parent.getStatus() == Tree.Status.NEW) {
                if (parent.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
                    userManager.onCreate((User) a, pw);
                } else {
                    // principal name not yet available -> remember the pw
                    currentPw.clear();
                    currentPw.put(a.getID(), pw);
                }
            }
            return true;

        } else if (UserConstants.REP_IMPERSONATORS.equals(propString)) {
            if (a.isGroup() || !isValid(propString, def, UserConstants.MIX_REP_IMPERSONATABLE, true)) {
                log.warn("Unexpected authorizable or definition for property rep:impersonators");
                return false;
            }
            // since impersonators may be imported later on, postpone processing
            // to the end.
            // see -> process References
            TextValue[] tvs = propInfo.getTextValues();
            referenceTracker.processedReference(new Impersonators(a.getID(), tvs));
            return true;

        } else if (UserConstants.REP_DISABLED.equals(propString)) {
            if (a.isGroup() || !isValid(propString, def, UserConstants.NT_REP_USER, false)) {
                log.warn("Unexpected authorizable or definition for property rep:disabled");
                return false;
            }

            ((User) a).disable(propInfo.getTextValue().getString());
            return true;

        } else if (UserConstants.REP_MEMBERS.equals(propString)) {
            if (!a.isGroup() || !isValid(propString, def, UserConstants.NT_REP_GROUP, true)) {
                return false;
            }
            // since group-members are references to user/groups that potentially
            // are to be imported later on -> postpone processing to the end.
            // see -> process References
            Membership membership = new Membership(a.getID());
            membership.addMembers(propInfo.getTextValues());
            referenceTracker.processedReference(membership);
            return true;

        } // else: cannot handle -> return false

        return false;
    }

    @Override
    public void processReferences() throws RepositoryException {
        checkInitialized();

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
    public boolean start(Tree protectedParent) throws RepositoryException {
        String repMembers = namePathMapper.getJcrName(UserConstants.NT_REP_MEMBERS);
        NodeUtil parentNode = new NodeUtil(protectedParent);
        if (parentNode.hasPrimaryNodeTypeName(repMembers)) {
            NodeUtil groupNode = parentNode;
            while (!groupNode.isRoot() && groupNode.hasPrimaryNodeTypeName(repMembers)) {
                groupNode = groupNode.getParent();
            }
            Authorizable auth = userManager.getAuthorizable(groupNode.getTree());
            if (auth == null) {
                log.debug("Cannot handle protected node " + protectedParent + ". It nor one of its parents represent a valid Authorizable.");
                return false;
            } else {
                currentMembership = new Membership(auth.getID());
                return true;
            }
        } // else: parent node is not of type rep:Members

        return false;
    }

    @Override
    public void startChildInfo(NodeInfo childInfo, List<PropInfo> propInfos) throws RepositoryException {
        checkNotNull(currentMembership);

        if (UserConstants.NT_REP_MEMBERS.equals(childInfo.getPrimaryTypeName())) {
            for (PropInfo prop : propInfos) {
                for (TextValue tv : prop.getTextValues()) {
                    String name = namePathMapper.getJcrName(prop.getName());
                    currentMembership.addMember(name, tv.getString());
                }
            }
        } else {
            log.warn("{} is not of type {}", childInfo.getString(), UserConstants.NT_REP_MEMBERS);
        }
    }

    @Override
    public void endChildInfo() throws RepositoryException {
        // nothing to do
    }

    @Override
    public void end(Tree protectedParent) throws RepositoryException {
        referenceTracker.processedReference(currentMembership);
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
    private PrincipalProvider getPrincipalProvider() throws RepositoryException {
        return userManager.getPrincipalProvider();
    }

    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
    }

    private boolean isValid(String propertyName, PropertyDefinition definition,
                            String oakNodeTypeName, boolean multipleStatus) {
        return multipleStatus == definition.isMultiple() &&
               definition.getDeclaringNodeType().isNodeType(namePathMapper.getJcrName(oakNodeTypeName));
    }

    /**
     * Handling the import behavior
     *
     * @param msg The message to log a warning in case of {@link ImportBehavior#IGNORE}
     * or {@link ImportBehavior#BESTEFFORT}
     * @throws javax.jcr.nodetype.ConstraintViolationException If the import
     * behavior is {@link ImportBehavior#ABORT}.
     */
    private void handleFailure(String msg) throws ConstraintViolationException{
        switch(importBehavior){
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

        private final String groupId;
        private final List<Member> members = new LinkedList<Member>();

        Membership(String groupId) {
            this.groupId = groupId;
        }

        void addMember(String name, String id) {
            members.add(new Member(name, id));
        }

        void addMembers(TextValue[] tvs) {
            for (TextValue tv : tvs) {
                addMember(null, tv.getString());
            }
        }

        void process() throws RepositoryException {
            Authorizable a = userManager.getAuthorizable(groupId);
            if (a == null || !a.isGroup()) {
                throw new RepositoryException(groupId + " does not represent a valid group.");
            }

            Group gr = (Group) a;
            // 1. collect members to add and to remove.
            Map<String, Authorizable> toRemove = new HashMap<String, Authorizable>();
            for (Iterator<Authorizable> declMembers = gr.getDeclaredMembers(); declMembers.hasNext(); ) {
                Authorizable dm = declMembers.next();
                toRemove.put(dm.getID(), dm);
            }

            List<Authorizable> toAdd = new ArrayList<Authorizable>();
            List<Membership.Member> nonExisting = new ArrayList<Membership.Member>();

            for (Membership.Member memberEntry : members) {
                String remapped = referenceTracker.get(memberEntry.contentId);
                String memberContentId = (remapped == null) ? memberEntry.contentId : remapped;

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
                        toAdd.add(member);
                    } // else: no need to remove from rep:members
                } else {
                    handleFailure("New member of " + gr + ": No such authorizable (NodeID = " + memberContentId + ')');
                    if (importBehavior == ImportBehavior.BESTEFFORT) {
                        log.info("ImportBehavior.BESTEFFORT: Remember non-existing member for processing.");
                        nonExisting.add(memberEntry);
                    }
                }
            }

            // 2. adjust members of the group
            for (Authorizable m : toRemove.values()) {
                if (!gr.removeMember(m)) {
                    handleFailure("Failed remove existing member (" + m + ") from " + gr);
                }
            }
            for (Authorizable m : toAdd) {
                if (!gr.addMember(m)) {
                    handleFailure("Failed add member (" + m + ") to " + gr);
                }
            }

            // handling non-existing members in case of best-effort
            if (!nonExisting.isEmpty()) {
                log.info("ImportBehavior.BESTEFFORT: Found " + nonExisting.size() + " entries of rep:members pointing to non-existing authorizables. Adding to rep:members.");
                Tree groupTree = root.getTree(gr.getPath());

                MembershipProvider membershipProvider = userManager.getMembershipProvider();
                for (Membership.Member member : nonExisting) {
                    membershipProvider.addMember(groupTree, member.name, member.contentId);
                }
            }
        }

        private class Member {
            private final String name;
            private final String contentId;

            public Member(String name, String contentId) {
                super();
                this.name = name;
                this.contentId = contentId;
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

        private final String userId;
        private final Set<String> principalNames = new HashSet<String>();

        private Impersonators(String userId, TextValue[] values) {
            this.userId = userId;
            for (TextValue v : values) {
                principalNames.add(v.getString());
            }
        }

        private void process() throws RepositoryException {
            Authorizable a = userManager.getAuthorizable(userId);
            if (a == null || a.isGroup()) {
                throw new RepositoryException(userId + " does not represent a valid user.");
            }

            Impersonation imp = ((User) a).getImpersonation();

            // 1. collect principals to add and to remove.
            Map<String, Principal> toRemove = new HashMap<String, Principal>();
            for (PrincipalIterator pit = imp.getImpersonators(); pit.hasNext(); ) {
                Principal princ = pit.nextPrincipal();
                toRemove.put(princ.getName(), princ);
            }

            List<String> toAdd = new ArrayList<String>();
            for (final String principalName : principalNames) {
                if (toRemove.remove(principalName) == null) {
                    // add it to the list of new impersonators to be added.
                    toAdd.add(principalName);
                } // else: no need to revoke impersonation for the given principal.
            }

            // 2. adjust set of impersonators
            for (Principal princicpal : toRemove.values()) {
                if (!imp.revokeImpersonation(princicpal)) {
                    String principalName = princicpal.getName();
                    handleFailure("Failed to revoke impersonation for " + principalName + " on " + a);
                }
            }
            List<String> nonExisting = new ArrayList<String>();
            for (String principalName : toAdd) {
                if (!imp.grantImpersonation(new PrincipalImpl(principalName))) {
                    handleFailure("Failed to grant impersonation for " + principalName + " on " + a);
                    if (importBehavior == ImportBehavior.BESTEFFORT &&
                            getPrincipalProvider().getPrincipal(principalName) == null) {
                        log.info("ImportBehavior.BESTEFFORT: Remember non-existing impersonator for special processing.");
                        nonExisting.add(principalName);
                    }
                }
            }

            if (!nonExisting.isEmpty()) {
                Tree userTree = root.getTree(a.getPath());
                // copy over all existing impersonators to the nonExisting list
                PropertyState impersonators = userTree.getProperty(UserConstants.REP_PRINCIPAL_NAME);
                for (String existing : impersonators.getValue(STRINGS)) {
                    nonExisting.add(existing);
                }
                // and write back the complete list including those principal
                // names that are unknown to principal provider.
                userTree.setProperty(UserConstants.REP_PRINCIPAL_NAME, nonExisting, Type.STRINGS);
            }
        }
    }

    /**
     * Inner class defining the treatment of membership or impersonator
     * values pointing to non-existing authorizables.
     */
    public static final class ImportBehavior {

        /**
         * If a member or impersonator value cannot be set due to constraints
         * enforced by the API implementation, the failure is logged as
         * warning but otherwise ignored.
         */
        public static final int IGNORE = 1;
        /**
         * Same as {@link #IGNORE} but in addition tries to circumvent the
         * problem. This option should only be used with validated and trusted
         * XML passed to the SessionImporter.
         */
        public static final int BESTEFFORT = 2;
        /**
         * Aborts the import as soon as invalid values are detected throwing
         * a {@code ConstraintViolationException}.
         */
        public static final int ABORT = 3;

        public static final String NAME_IGNORE = "ignore";
        public static final String NAME_BESTEFFORT = "besteffort";
        public static final String NAME_ABORT = "abort";

        public static int valueFromString(String behaviorString) {
            if (NAME_IGNORE.equalsIgnoreCase(behaviorString)) {
                return IGNORE;
            } else if (NAME_BESTEFFORT.equalsIgnoreCase(behaviorString)) {
                return BESTEFFORT;
            } else if (NAME_ABORT.equalsIgnoreCase(behaviorString)) {
                return ABORT;
            } else {
                log.error("Invalid behavior " + behaviorString + " -> Using default: ABORT.");
                return ABORT;
            }
        }

        public static String nameFromValue(int importBehavior) {
            switch (importBehavior) {
                case ImportBehavior.IGNORE:
                    return NAME_IGNORE;
                case ImportBehavior.ABORT:
                    return NAME_ABORT;
                case ImportBehavior.BESTEFFORT:
                    return NAME_BESTEFFORT;
                default:
                    throw new IllegalArgumentException("Invalid import behavior: " + importBehavior);
            }
        }
    }
}