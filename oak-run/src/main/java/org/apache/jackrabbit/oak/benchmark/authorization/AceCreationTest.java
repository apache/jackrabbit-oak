package org.apache.jackrabbit.oak.benchmark.authorization;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.benchmark.AbstractTest;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import javax.security.auth.Subject;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

public class AceCreationTest extends AbstractTest {

    public static final int NUMBER_OF_INITIAL_ACE_DEFAULT = 0;
    private final int numberOfAce;
    private final int numberOfInitialAce;
    private final boolean transientWrites;
    private String nodePath;

    private Session transientSession;

    public AceCreationTest(int numberOfAce, int numberOfInitialAce, boolean transientWrites) {
        super();
        this.numberOfAce = numberOfAce;
        this.numberOfInitialAce = numberOfInitialAce;
        this.transientWrites = transientWrites;
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        Session session = createOrGetSystemSession();
        nodePath = session.getRootNode().addNode("test" + new Random().nextInt()).getPath();

        save(session, transientWrites);
        logout(session, transientWrites);
    }

    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();

        Session session = createOrGetSystemSession();
        createAce(session, numberOfInitialAce);

        save(session, transientWrites);
        logout(session, transientWrites);
    }

    @Override
    protected void afterTest() throws Exception {
        Session session = createOrGetSystemSession();

        AccessControlManager acm = session.getAccessControlManager();
        for (AccessControlPolicy policy : acm.getPolicies(nodePath)) {
            acm.removePolicy(nodePath, policy);
        }
        save(session, transientWrites);

        super.afterTest();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            if (transientSession != null) {
                transientSession.logout();
            }
        } finally {
            super.tearDown();
        }
    }

    @Override
    protected void runTest() throws Exception {
        Session session = createOrGetSystemSession();

        createAce(session, numberOfAce);
        save(session, transientWrites);
        logout(session, transientWrites);
    }

    private void createAce(Session session, int count) throws RepositoryException {
        AccessControlManager acManager = session.getAccessControlManager();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acManager, nodePath);

        for (int i = 0; i < count; i++) {
            ImmutableMap<String, Value> restrictions = ImmutableMap.of(AccessControlConstants.REP_GLOB, session.getValueFactory().createValue(i + ""));
            acl.addEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acManager, Privilege.JCR_ADD_CHILD_NODES), true, restrictions);
        }

        acManager.setPolicy(nodePath, acl);
    }

    private static void save(Session session, boolean transientWrites) throws RepositoryException {
        if (!transientWrites) {
            session.save();
        }
    }

    private static void logout(Session session, boolean transientWrites) {
        if (!transientWrites) {
            session.logout();
        }
    }

    private Session createOrGetSystemSession() throws PrivilegedActionException {
        if(transientWrites && transientSession != null) {
            return transientSession;
        }

        return (transientSession = Subject.doAsPrivileged(SystemSubject.INSTANCE, new PrivilegedExceptionAction<Session>() {
            @Override
            public Session run() throws Exception {
                return getRepository().login(null, null);
            }
        }, null));
    }
}
