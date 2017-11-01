package org.apache.jackrabbit.oak.benchmark;

import java.io.ByteArrayInputStream;
import java.util.Calendar;
import java.util.Random;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Simple test case which adds a basic hierarchy of 3 nodes (nt:folder, nt:file
 * and nt:resource) and 3 properties (jcr:mimeType, jcr:lastModified and
 * jcr:data). The binary data accounts for 5MB and is randomly generated for
 * each run.
 */
public class BasicWriteTest extends AbstractTest {
    private static final int MB = 1024 * 1024;
    private Session session;
    private Node root;

    /**
     * Iteration counter used to avoid the slit document edge case in
     * DocumentMK.
     */
    private int iteration = 0;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
    }

    @Override
    public void beforeTest() throws RepositoryException {
        root = session.getRootNode().addNode(TEST_ID + iteration++, "nt:folder");
        session.save();
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void runTest() throws Exception {
        final int blobSize = 5 * MB;
        
        Node file = root.addNode("server", "nt:file");

        byte[] data = new byte[blobSize];
        new Random().nextBytes(data);

        Node content = file.addNode("jcr:content", "nt:resource");
        content.setProperty("jcr:mimeType", "application/octet-stream");
        content.setProperty("jcr:lastModified", Calendar.getInstance());
        content.setProperty("jcr:data", new ByteArrayInputStream(data));

        session.save();
    }

    @Override
    public void afterTest() throws RepositoryException {
        root.remove();
        session.save();
    }
}
