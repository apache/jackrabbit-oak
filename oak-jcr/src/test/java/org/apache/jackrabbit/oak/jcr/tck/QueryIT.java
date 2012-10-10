package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class QueryIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new QueryIT();
    }

    public QueryIT() {
        super("JCR query tests");
        addTest(org.apache.jackrabbit.test.api.query.TestAll.suite());
        addTest(org.apache.jackrabbit.test.api.query.qom.TestAll.suite());
    }
}
