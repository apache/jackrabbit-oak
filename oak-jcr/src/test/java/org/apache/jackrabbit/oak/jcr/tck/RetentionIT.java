package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class RetentionIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new RetentionIT();
    }

    public RetentionIT() {
        super("JCR retention tests");
        addTest(org.apache.jackrabbit.test.api.retention.TestAll.suite());
    }
}
