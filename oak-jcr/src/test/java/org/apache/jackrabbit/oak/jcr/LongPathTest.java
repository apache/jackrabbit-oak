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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Ignore;
import org.junit.Test;

public class LongPathTest extends AbstractRepositoryTest {

    public LongPathTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void testLongPath() throws Exception {
        Session s = getAdminSession();

        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            buff.append("0123456789");
        }
        String longName = "n" + buff.toString();
        Node n = s.getRootNode();
        ArrayList<String> paths = new ArrayList<String>();
        for (int i = 0; i < 30; i++) {
            n = n.addNode(longName + "_" + i);
            paths.add(n.getPath());
        }
        s.save();

        Session s2 = createAdminSession();
        Node n2 = s2.getRootNode();
        for (int i = 0; i < 30; i++) {
            n2 = n2.getNode(longName + "_" + i);
            assertEquals(paths.get(i), n2.getPath());
        }
        s2.logout();
    }

    @Test
    @Ignore("OAK-1629")
    public void testLongName() throws Exception {

        try {
            Session s = getAdminSession();

            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                buff.append("0123456789");
            }
            String longName = "n" + buff.toString();
            Node n = s.getRootNode().addNode(longName);
            s.save();

            Session s2 = createAdminSession();
            Node n2 = s2.getRootNode().getNode(longName);
            assertEquals(n.getPath(), n2.getPath());

            s2.logout();
        } catch (ConstraintViolationException ex) {
            // acceptable
        }
    }
}