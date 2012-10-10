package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class VersionIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new VersionIT();
    }

    public VersionIT() {
        super("JCR version tests");
        addTest(org.apache.jackrabbit.test.api.version.TestAll.suite());
        addTest(org.apache.jackrabbit.test.api.version.simple.TestAll.suite());    }
}
