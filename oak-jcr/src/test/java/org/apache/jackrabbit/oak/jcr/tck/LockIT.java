package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class LockIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new LockIT();
    }

    public LockIT() {
        super("JCR lock tests");
        addTest(org.apache.jackrabbit.test.api.lock.TestAll.suite());
    }
}
