package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class NodetypeIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new NodetypeIT();
    }

    public NodetypeIT() {
        super("JCR node type tests");
        addTest(org.apache.jackrabbit.test.api.nodetype.TestAll.suite());
    }
}
