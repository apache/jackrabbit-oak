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
package org.apache.jackrabbit.oak.jcr.random;


import java.util.Random;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Session;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("OAK-884")
public class SimpleAclRandomizedTest extends AbstractRandomizedTest {

    private  int depth;

    private Table<Integer, Integer, String> tree = HashBasedTable.create();
    {
        tree.put(0, 0, "/");
        tree.put(1, 0, "/n1");
        tree.put(1, 1, "/n2");
        tree.put(2, 0, "/n1/n3");
        tree.put(2, 1, "/n1/n4");
        tree.put(2, 2, "/n1/n5");
        tree.put(3, 0, "/n1/n3/n6");
        tree.put(3, 1, "/n1/n3/n7");
        tree.put(3, 2, "/n1/n3/n8");
        tree.put(3, 3, "/n1/n3/n9");
    }

    protected void setupTree(Session session) throws Exception {
        depth = 4;
        Node n1 = session.getRootNode().addNode("n1");
        session.getRootNode().addNode("n2");
        Node n3 = n1.addNode("n3");
        n1.addNode("n4");
        n1.addNode("n5");
        n3.addNode("n6");
        n3.addNode("n7");
        n3.addNode("n8");
        n3.addNode("n9");
        session.save();
    }

    protected void clearTree(Session session) throws Exception {       
        session.getRootNode().getNode("n1").remove();
        session.getRootNode().getNode("n2").remove();
        session.save();
    }

    @Test
    public void testReadAcl() throws Exception {

        setupPermission(jackrabbitWriterSession, jackrabbitPrincipal, "/", true, PrivilegeConstants.JCR_READ);
        setupPermission(oakWriterSession, oakPrincipal, "/", true,PrivilegeConstants.JCR_READ);

        for (int j = 0; j < 1; j++) {

            Random r = new Random(j);
            int operations = 1000;
            int depthToApply;
            int index;
            boolean allow;
            int principalIndex;

            for (int i = 0; i < operations; i++) {
                allow = r.nextBoolean();
                depthToApply = r.nextInt(depth);
                String path;
                principalIndex = r.nextInt(jackrabbitPrincipals.size());

                if (depthToApply == 0) {
                    path = "/";
                    continue;
                } else {
                    index = r.nextInt(depthToApply + 1);
                    path = getPath(depthToApply, index);
                }

                setupPermission(jackrabbitWriterSession,
                        jackrabbitPrincipals.get(principalIndex), path, allow, PrivilegeConstants.JCR_READ);

                setupPermission(oakWriterSession, oakPrincipals.get(principalIndex),
                        path, allow, PrivilegeConstants.JCR_READ);

                check();
            }

        }
    }

    private String getPath(int depth, int index) throws Exception {
        if (depth == 0) {
            return "/";
        }
        return tree.get(depth, index);
    }

    public void check() throws Exception {
        boolean mustThrow;
        boolean thrown;

        try {
            for (String path : tree.values()) {
                mustThrow = false;
                thrown = false;
                Node njr = null, noak = null;
                try {
                    njr = jackrabbitReaderSession.getNode(path);
                } catch (PathNotFoundException pnf) {
                    mustThrow = true;
                }

                try {
                    noak = oakReaderSession.getNode(path);
                } catch (PathNotFoundException pnf) {
                    thrown = true;
                }

                if (mustThrow != thrown) {
                    Assert.fail("did not throw for both for path " + path);
                }

                if (!mustThrow) {
                    if (!path.equals(njr.getPath())
                            || !njr.getPath().equals(noak.getPath())) {
                        Assert.fail("did not resolved the same node");
                    }
                }
            }

        } catch (Exception e) {
            throw new Exception(e);
        }
    }

}
