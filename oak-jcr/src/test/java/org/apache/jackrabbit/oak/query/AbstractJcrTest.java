package org.apache.jackrabbit.oak.query;


import com.google.common.io.Closer;
import org.junit.After;
import org.junit.Before;

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.QueryManager;
import java.io.IOException;

public abstract class AbstractJcrTest {

    private Repository jcrRepository;
    protected Session adminSession;
    protected Session anonymousSession;
    protected QueryManager qm;
    protected Closer closer;

    @Before
    public void before() throws Exception {
        closer = Closer.create();
        jcrRepository = createJcrRepository();

        adminSession = jcrRepository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        // we'd always query anonymously
        anonymousSession = jcrRepository.login(new GuestCredentials(), null);
        anonymousSession.refresh(true);
        anonymousSession.save();
        qm = anonymousSession.getWorkspace().getQueryManager();
    }

    @After
    public void after() throws IOException {
        closer.close();
        adminSession.logout();
        anonymousSession.logout();
    }

    abstract protected Repository createJcrRepository() throws RepositoryException;
}
