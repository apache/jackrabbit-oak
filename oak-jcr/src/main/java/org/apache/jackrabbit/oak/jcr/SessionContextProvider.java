package org.apache.jackrabbit.oak.jcr;

import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.observation.ObservationManager;
import javax.jcr.security.AccessControlManager;
import javax.jcr.version.VersionManager;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SessionContextProvider {
    private static final Map<SessionDelegate, SessionContext> CONTEXTS = Maps.newConcurrentMap();

    private SessionContextProvider() { }

    @Nonnull
    public static Session newSession(@Nonnull SessionDelegate delegate, @Nonnull RepositoryImpl repository) {
        SessionContext context = SessionContext.create(delegate, repository);
        CONTEXTS.put(delegate, context);
        return context.getSession();
    }

    public static void remove(SessionDelegate delegate) {
        SessionContext context = CONTEXTS.remove(delegate);
        if (context == null) {
            throw new IllegalStateException();
        }
        context.dispose();
    }

    @Nonnull
    public static SessionImpl getSession(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getSession();
    }

    @Nonnull
    public static RepositoryImpl getRepository(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getRepository();
    }

    @Nonnull
    public static WorkspaceImpl getWorkspace(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getWorkspace();
    }

    @Nonnull
    public static LockManager getLockManager(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getLockManager();
    }

    @Nonnull
    public static NodeTypeManager getNodeTypeManager(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getNodeTypeManager();
    }

    @Nonnull
    public static VersionManager getVersionManager(@Nonnull SessionDelegate delegate)
            throws RepositoryException {
        return getSessionContext(delegate).getVersionManager();
    }

    @Nonnull
    public static EffectiveNodeTypeProvider getEffectiveNodeTypeProvider(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getEffectiveNodeTypeProvider();
    }

    @Nonnull
    public static DefinitionProvider getDefinitionProvider(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getDefinitionProvider();
    }

    @Nonnull
    public static NamePathMapperImpl getNamePathMapper(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getNamePathMapper();
    }

    @Nonnull
    public static ValueFactoryImpl getValueFactory(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).getValueFactory();
    }

    @Nonnull
    public static String getOakName(@Nonnull SessionDelegate delegate, String jcrName)
            throws RepositoryException {
        return getSessionContext(delegate).getOakName(jcrName);
    }

    @CheckForNull
    public static String getOakPathOrNull(@Nonnull SessionDelegate delegate, String jcrPath) {
        return getSessionContext(delegate).getOakPathOrNull(jcrPath);
    }

    @Nonnull
    public static String getOakPathKeepIndexOrThrowNotFound(@Nonnull SessionDelegate delegate, String jcrPath)
            throws PathNotFoundException {
        return getSessionContext(delegate).getOakPathKeepIndexOrThrowNotFound(jcrPath);
    }

    @Nonnull
    public static String getOakPathOrThrowNotFound(@Nonnull SessionDelegate delegate, String jcrPath)
            throws PathNotFoundException {
        return getSessionContext(delegate).getOakPathOrThrowNotFound(jcrPath);
    }

    @Nonnull
    public static String getOakPath(@Nonnull SessionDelegate delegate, String jcrPath)
            throws RepositoryException {
        return getSessionContext(delegate).getOakPath(jcrPath);
    }

    @Nonnull
    public static AccessControlManager getAccessControlManager(SessionDelegate delegate) {
        return getSessionContext(delegate).getAccessControlManager(delegate);
    }

    @Nonnull
    public static PermissionProvider getPermissionProvider(SessionDelegate delegate) {
        return getSessionContext(delegate).getPermissionProvider(delegate);
    }

    @Nonnull
    public static PrincipalManager getPrincipalManager(SessionDelegate delegate) {
        return getSessionContext(delegate).getPrincipalManager(delegate);
    }

    @Nonnull
    public static UserManager getUserManager(SessionDelegate delegate) {
        return getSessionContext(delegate).getUserManager(delegate);
    }

    @Nonnull
    public static PrivilegeManager getPrivilegeManager(SessionDelegate delegate) {
        return getSessionContext(delegate).getPrivilegeManager(delegate);
    }

    @Nonnull
    public static UserConfiguration getUserConfiguration(SessionDelegate delegate) {
        return getSessionContext(delegate).getUserConfiguration();
    }

    @Nonnull
    public static AccessControlConfiguration getAccessControlConfiguration(SessionDelegate delegate) {
        return getSessionContext(delegate).getAccessControlConfiguration();
    }

    @Nonnull
    public static ObservationManager getObservationManager(SessionDelegate delegate) {
        return getSessionContext(delegate).getObservationManager(delegate);
    }

    public static boolean hasPendingEvents(SessionDelegate delegate) {
        return getSessionContext(delegate).hasPendingEvents();
    }

    //------------------------------------------------------------< private >---

    @Nonnull
    private static SessionContext getSessionContext(SessionDelegate delegate) {
        SessionContext context = CONTEXTS.get(checkNotNull(delegate));
        if (context == null) {
            throw new IllegalStateException();
        }
        return context;
    }

}
