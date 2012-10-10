package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class ApiIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new ApiIT();
    }

    public ApiIT() {
        super("JCR API tests");
        addTest(org.apache.jackrabbit.test.api.TestAll.suite());
    }
}
