package org.apache.jackrabbit.oak.jcr.tck;

import junit.framework.Test;
import org.apache.jackrabbit.test.ConcurrentTestSuite;

public class ObservationIT extends ConcurrentTestSuite {

    public static Test suite() {
        return new ObservationIT();
    }

    public ObservationIT() {
        super("JCR observation tests");
        addTest(org.apache.jackrabbit.test.api.observation.TestAll.suite());
    }
}
