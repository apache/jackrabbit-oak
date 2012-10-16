package org.apache.jackrabbit.mongomk.impl;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * FIXME - Look into these tests and see if we want to fix them somehow.
 *
 * Tests for MongoMicroKernel limits.
 */
public class MongoMKLimitsTest extends BaseMongoMicroKernelTest {


    /**
     * This test currently fails due to 1000 char limit in property sizes in MongoDB
     * which affects path property. It also slows down as the test progresses.
     */
    @Test
    @Ignore
    public void pathLimit() throws Exception {
        String path = "/";
        String baseNodeName = "testingtestingtesting";
        int numberOfCommits = 100;
        String jsonDiff;
        String message;

        for (int i = 0; i < numberOfCommits; i++) {
            jsonDiff = "+\"" + baseNodeName + i + "\" : {}";
            message = "Add node n" + i;
            mk.commit(path, jsonDiff, null, message);
            if (!PathUtils.denotesRoot(path)) {
                path += "/";
            }
            path += baseNodeName + i;
        }
    }
}