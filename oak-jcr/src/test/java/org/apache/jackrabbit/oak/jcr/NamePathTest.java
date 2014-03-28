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
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamePathTest extends AbstractRepositoryTest {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NamePathTest.class);

    public NamePathTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void testSlashInPath() throws RepositoryException {
        List<String> paths = ImmutableList.of(
                "//jcr:content",
                "//content"
        );
        testPaths(paths, getAdminSession());
    }

    @Test
    public void testSlashInName() throws RepositoryException {
        List<String> names = ImmutableList.of(
                "/jcr:content",
                "/content",
                "jcr:con/ent",
                "jc/r:content",
                "con/ent"
        );
        testNames(names, getAdminSession());
    }

    @Test
    public void testColonInPath() throws RepositoryException {
        List<String> paths = ImmutableList.of(
                "/jcr:con:ent"
        );
        testPaths(paths, getAdminSession());
    }

    @Test
    public void testColonInName() throws RepositoryException {
        List<String> names = ImmutableList.of(
                "jcr:con:ent"
        );
        testNames(names, getAdminSession());
    }

    @Test
    public void testSquareBracketsInPath() throws RepositoryException {
        List<String> paths = ImmutableList.of(
                "//jcr:content",
                "/jcr:con]ent",
                "/con]ent"
        );
        testPaths(paths, getAdminSession());
    }

    @Test
    public void testSquareBracketsInName() throws RepositoryException {
        List<String> names = ImmutableList.of(
                "jcr:content[1]",
                "content[1]",
                "jcr:conten[t]",
                "conten[t]",

                "jcr:con[]ent",
                "jcr[]:content",
                "con[]ent",
                "jcr:con[t]ent",
                "jc[t]r:content",
                "con[t]ent",

                "jcr:con]ent",
                "jc]r:content",
                "con]ent",

                "jcr:con[ent",
                "jc[r:content",
                "con[ent"
        );
        testNames(names, getAdminSession());
    }

    @Test
    public void testAsteriskInPath() throws RepositoryException {
        List<String> paths = ImmutableList.of(
                "/jcr:con*ent",
                "/jcr:*ontent",
                "/jcr:conten*",
                "/con*ent",
                "/*ontent",
                "/conten*"
        );
        testPaths(paths, getAdminSession());
    }

    @Test
    public void testAsteriskInName() throws RepositoryException {
        List<String> names = ImmutableList.of(
                "jcr:con*ent",
                "jcr:*ontent",
                "jcr:conten*",
                "con*ent",
                "*ontent", // TODO fails
                "conten*"
        );
        testNames(names, getAdminSession());
    }

    @Test
    public void testVerticalLineInPath() throws Exception {
        List<String> paths = ImmutableList.of(
                "/jcr:con|ent",
                "/jcr:|ontent",
                "/jcr:conten|",
                "/|ontent",
                "/conten|",
                "/con|ent"
                );
        testPaths(paths, getAdminSession());
    }

    @Test
    public void testVerticalLineInName() throws Exception {
        List<String> names = ImmutableList.of(
                "jcr:con|ent",
                "jcr:|ontent",
                "jcr:conten|",
                "con|ent",
                "|ontent", //TODO fails
                "conten|"
        );
        testNames(names, getAdminSession());
    }

    @Test
    public void testWhitespaceInPath() throws Exception {
        List<String> paths = ImmutableList.of(
                "/content ",
                "/ content",
                "/content\t",
                "/\tcontent",
                "/jcr:con\tent",
                "con\tent"
        );

        testPaths(paths, getAdminSession());
    }

    @Test
    public void testWhitespaceInName() throws Exception {
        List<String> names = ImmutableList.of(
//                "jcr:content ",  // FIXME OAK-1174
                "content ",
                " content",
                "jcr:content\t",
                "content\t",
                "\tcontent",
                "con\tent"
        );
        testNames(names, getAdminSession());
    }

    @Test
    public void testSpaceInNames() throws RepositoryException {
        Session session = getAdminSession();
        Node n = session.getRootNode().addNode("c o n t e n t");
        Node n2 = session.getNode(n.getPath());
    }


    private static void testPaths(List<String> paths, Session session) throws RepositoryException {
        for (String path : paths) {
            testPath(path, session);
        }
    }

    private static void testPath(String path, Session session) throws RepositoryException {
        RepositoryException exception = null;
        try {
            session.itemExists(path);
        } catch (RepositoryException e) {
            exception = e;
        }

        session.setNamespacePrefix("foo", "http://foo.bar");
        try {
            session.itemExists(path);
            assertNull("path = " + path, exception);
        } catch (RepositoryException e) {
            assertNotNull("path = " + path, exception);
        }
    }

    private static void testNames(List<String> names, Session session) throws RepositoryException {
        for (String name : names) {
            testName(name, session);
        }
    }

    private static void testName(String name, Session session) throws RepositoryException {
        Exception exception = null;
        try {
            session.getRootNode().addNode(name);
        } catch (RepositoryException e) {
            exception = e;
        } finally {
            session.refresh(false);
        }

        session.setNamespacePrefix("foo", "http://foo.bar");
        try {
            session.getRootNode().addNode(name);
            assertNull("name = " + name, exception);
        } catch (RepositoryException e) {
            assertNotNull("name = " + name, exception);
        }
    }
}