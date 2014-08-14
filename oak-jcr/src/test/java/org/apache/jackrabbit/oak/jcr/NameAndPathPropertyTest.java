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

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.junit.Test;

import static org.junit.Assert.fail;

public class NameAndPathPropertyTest extends AbstractRepositoryTest {

    public NameAndPathPropertyTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void testMVNameProperty() throws Exception {
        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("testRoot");
        try {
            testRootNode.setProperty("testNameProperty", new String[]{"foobar:test"}, PropertyType.NAME);
            session.save();
            fail("adding a MV name property without registered namespace must fail.");
        } catch (RepositoryException e) {
            // ok.
        }
    }

    @Test
    public void testMVPathProperty() throws Exception {
        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("testRoot");
        try {
            testRootNode.setProperty("testPathProperty", new String[]{
                    "/foobar:test",
                    "/foobar:test/a",
                    "/a/foobar:test",
            }, PropertyType.PATH);
            session.save();
            fail("adding a MV path property without registered namespace must fail.");
        } catch (RepositoryException e) {
            // ok.
        }
    }


    @Test
    public void testPathProperty() throws Exception {
        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("testRoot");
        try {
            testRootNode.setProperty("testPathProperty", "/foobar:test", PropertyType.PATH);
            session.save();
            fail("adding a  path property without registered namespace must fail.");
        } catch (RepositoryException e) {
            // ok.
        }
    }

    @Test
    public void testInvalidPathProperty() throws Exception {
        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("testRoot");
        try {
            testRootNode.setProperty("testPathProperty", "/*/dfsdf", PropertyType.PATH);
            session.save();
            fail("adding a  path property without registered namespace must fail.");
        } catch (RepositoryException e) {
            // ok.
        }
    }


    @Test
    public void testInvalidMVPathProperty() throws Exception {
        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("testRoot");
        try {
            testRootNode.setProperty("testPathProperty", new String[]{"/*/dfsdf"}, PropertyType.PATH);
            session.save();
            fail("adding a  path property without registered namespace must fail.");
        } catch (RepositoryException e) {
            // ok.
        }
    }


    private void traverse(Node node) throws RepositoryException {
        System.out.println(node.getPath());
        PropertyIterator iter = node.getProperties();
        while (iter.hasNext()) {
            Property p = iter.nextProperty();
            System.out.println(p.getPath());
            p.getDefinition();
        }
        NodeIterator niter = node.getNodes();
        while (niter.hasNext()) {
            traverse(niter.nextNode());
        }
    }
}