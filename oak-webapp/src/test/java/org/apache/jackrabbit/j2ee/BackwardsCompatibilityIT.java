/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.j2ee;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.SessionImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackwardsCompatibilityIT extends TestCase {

    /**
     * Logger instance
     */
    private static final Logger log =
        LoggerFactory.getLogger(BackwardsCompatibilityIT.class);

    public void testBackwardsCompatibility() throws Exception {
        // Force loading of the Derby JDBC driver
        new EmbeddedDriver();

        File target = new File("target/backwards-compatibility-test");
        FileUtils.deleteDirectory(target);
        target.mkdirs();

        File source = new File("src/test/resources/compatibility.zip");
        unpack(source, target);

        for (File dir : target.listFiles()) {
            if (dir.isDirectory()) {
                log.info("Testing backwards compatibility with {}", dir);
                checkJackrabbitRepository(dir);

                NodeStore store = new SegmentNodeStore();
                RepositoryUpgrade.copy(dir, store);
                checkRepositoryContent(
                        new Jcr(new Oak(store)).createRepository());
            }
        }
    }

    private void checkJackrabbitRepository(File directory) throws Exception {
        File configuration = new File(directory, "repository.xml");

        try {
            RepositoryConfig config = RepositoryConfig.create(
                    configuration.getPath(), directory.getPath());
            RepositoryImpl repository = RepositoryImpl.create(config);
            try {
                checkRepositoryContent(repository);
            } finally {
                repository.shutdown();
            }
        } catch (RepositoryException e) {
            String message = "Unable to access repository " + directory;
            log.error(message, e);
            fail(message);
        }
    }

    private void checkRepositoryContent(Repository repository)
            throws Exception {
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        try {
            assertTestData(session);
        } finally {
            session.logout();
        }
    }

    private void assertTestData(Session session) throws Exception {
        Node root = session.getRootNode();

        assertTrue(root.hasNode("test"));
        Node test = root.getNode("test");

        Node versionable = assertVersionable(test);
        assertProperties(test, versionable);
        if (session instanceof SessionImpl) {
            // FIXME: Not yet supported by Oak
            assertVersionableCopy(test, versionable);
        }
        assertLock(test);
        assertUsers(session);
    }

    @SuppressWarnings("deprecation")
    private Node assertVersionable(Node test) throws RepositoryException {
        assertTrue(test.hasNode("versionable"));
        Node versionable = test.getNode("versionable");
        assertTrue(versionable.isNodeType("nt:myversionable"));
        assertTrue(versionable.isNodeType("nt:unstructured"));
        assertTrue(versionable.isNodeType("mix:versionable"));
        assertFalse(versionable.isCheckedOut());

        VersionHistory history = versionable.getVersionHistory();
        Version versionB = versionable.getBaseVersion();
        String[] labels = history.getVersionLabels(versionB);
        assertEquals(1, labels.length);
        assertEquals("labelB", labels[0]);
        Version versionA = history.getVersionByLabel("labelA");
        versionable.restore(versionA, true);
        assertEquals("A", versionable.getProperty("foo").getString());
        versionable.restore(versionB, true);
        assertEquals("B", versionable.getProperty("foo").getString());
        return versionable;
    }

    @SuppressWarnings("deprecation")
    private void assertProperties(Node test, Node versionable)
            throws RepositoryException, PathNotFoundException,
            ValueFormatException, IOException {
        assertTrue(test.hasNode("properties"));
        Node properties = test.getNode("properties");
        assertTrue(properties.isNodeType("nt:unstructured"));

        assertEquals(true, properties.getProperty("boolean").getBoolean());
        assertEquals(0.123456789, properties.getProperty("double").getDouble());
        assertEquals(1234567890, properties.getProperty("long").getLong());
        Node reference = properties.getProperty("reference").getNode();
        assertTrue(reference.isSame(versionable));
        assertEquals("test", properties.getProperty("string").getString());

        Value[] multiple = properties.getProperty("multiple").getValues();
        assertEquals(3, multiple.length);
        assertEquals("a", multiple[0].getString());
        assertEquals("b", multiple[1].getString());
        assertEquals("c", multiple[2].getString());

        Calendar calendar = properties.getProperty("date").getDate();
        assertEquals(1234567890, calendar.getTimeInMillis());

        InputStream stream = properties.getProperty("binary").getStream();
        try {
            byte[] binary = new byte[100 * 1000];
            new Random(1234567890).nextBytes(binary);
            assertTrue(Arrays.equals(binary, IOUtils.toByteArray(stream)));
        } finally {
            stream.close();
        }
    }

    @SuppressWarnings("deprecation")
    private void assertVersionableCopy(Node test, Node versionable)
            throws RepositoryException, IOException {
        test.getSession().getWorkspace().copy(
                versionable.getPath(),
                versionable.getPath() + "-copy");
        Node copy = test.getNode(versionable.getName() + "-copy");
        copy.remove();
        test.save();
    }

    private void assertLock(Node test) throws RepositoryException {
        Node lock = test.getNode("lock");
        assertTrue(lock.isLocked());
        assertTrue(lock.hasProperty("jcr:lockOwner"));
    }

    private void assertUsers(Session session) throws RepositoryException {
    }

    private void unpack(File archive, File dir) throws IOException {
        ZipFile zip = new ZipFile(archive);
        try {
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.getName().startsWith("META-INF")) {
                } else if (entry.isDirectory()) {
                    new File(dir, entry.getName()).mkdirs();
                } else {
                    File file = new File(dir, entry.getName());
                    file.getParentFile().mkdirs();
                    InputStream input = zip.getInputStream(entry);
                    try {
                        OutputStream output = new FileOutputStream(file);
                        try {
                            IOUtils.copy(input, output);
                        } finally {
                            output.close();
                        }
                    } finally {
                        input.close();
                    }
                }
            }
        } finally {
            zip.close();
        }
    }

}

