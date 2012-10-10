package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class SecurityIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new SecurityIT();
    }

    public SecurityIT() {
        super("JCR security tests");
        addTest(org.apache.jackrabbit.test.api.security.TestAll.suite());
    }
}
