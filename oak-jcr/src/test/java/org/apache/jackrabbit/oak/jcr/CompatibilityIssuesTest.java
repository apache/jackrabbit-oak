package org.apache.jackrabbit.oak.jcr;

import org.junit.After;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import static org.junit.Assert.fail;

/**
 * This class contains test cases which demonstrate changes in behaviour wrt. to Jackrabbit 2.
 * See OAK-14: Identify and document changes in behaviour wrt. Jackrabbit 2
 */
public class CompatibilityIssuesTest extends AbstractRepositoryTest {

    @After
    public void tearDown() throws RepositoryException {
        logout();
    }

    /**
     * Trans-session isolation differs from Jackrabbit 2. Snapshot isolation can
     * result in write skew as this test demonstrates: the check method enforces
     * an application logic constraint which says that the sum of the properties
     * p1 and p2 must not be negative. While session1 and session2 each enforce
     * this constraint before saving, the constraint might not hold globally as
     * can be seen in session3.
     *
     * @see <a href="http://wiki.apache.org/jackrabbit/Transactional%20model%20of%20the%20Microkernel%20based%20Jackrabbit%20prototype">
     *     Transactional model of the Microkernel based Jackrabbit prototype</a>
     */
    @Test
    public void sessionIsolation() throws RepositoryException {
        Repository repository = getRepository();

        Session session0 = repository.login();
        Node testNode = session0.getNode("/").addNode("testNode");
        testNode.setProperty("p1", 1);
        testNode.setProperty("p2", 1);
        session0.save();
        check(getSession());

        Session session1 = repository.login();
        Session session2 = repository.login();

        session1.getNode("/testNode").setProperty("p1", -1);
        check(session1);
        session1.save();

        session2.getNode("/testNode").setProperty("p2", -1);
        check(session2);      // Throws on JR2, not on JR3
        session2.save();

        Session session3 = repository.login();
        try {
            check(session3);  // Throws on JR3
            fail();
        }
        catch (AssertionError e) {
            // expected
        }

        session0.logout();
        session1.logout();
        session2.logout();
        session3.logout();
    }

    private static void check(Session session) throws RepositoryException {
        if (session.getNode("/testNode").getProperty("p1").getLong() +
                session.getNode("/testNode").getProperty("p2").getLong() < 0) {
            fail("p1 + p2 < 0");
        }
    }

}
