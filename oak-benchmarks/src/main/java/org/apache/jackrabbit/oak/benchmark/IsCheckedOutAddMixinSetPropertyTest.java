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
package org.apache.jackrabbit.oak.benchmark;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.jetbrains.annotations.NotNull;

import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import java.util.List;

public class IsCheckedOutAddMixinSetPropertyTest extends ReadDeepTreeTest {
    
    public IsCheckedOutAddMixinSetPropertyTest(boolean runAsAdmin, int itemsToRead, boolean doReport) {
        super(runAsAdmin, itemsToRead, doReport);
    }

    @Override
    protected void randomRead(Session testSession, List<String> allPaths, int cnt) throws RepositoryException {
        boolean logout = false;
        if (testSession == null) {
            testSession = getTestSession();
            logout = true;
        }
        try {
            Cnt accessCnt = new Cnt();
            long start = System.currentTimeMillis();
            for (int i = 0; i < cnt; i++) {
                readItem(testSession, getRandom(allPaths), accessCnt);
            }
            long end = System.currentTimeMillis();
            if (doReport) {
                System.out.println("Session " + testSession.getUserID() + " reading " + (cnt-accessCnt.noAccess) + " (Nodes: "+ accessCnt.nodeCnt +"; Properties: "+accessCnt.propertyCnt+"), Node writes "+accessCnt.nodeWrites+" completed in " + (end - start));
            }
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }

    private void readItem(Session testSession, String path, Cnt accessCnt) throws RepositoryException {
        for (int i = 0; i < repeatedRead; i++) {
            JackrabbitSession s = (JackrabbitSession) testSession;
            Item item = s.getItemOrNull(path);
            if (item instanceof Node) {
                accessCnt.nodeCnt++;
                additionalNodeOperation((Node) item, accessCnt);
            } else if (item instanceof Property) {
                accessCnt.propertyCnt++;
                Node parent = s.getParentOrNull(item);
                if (parent != null) {
                    additionalNodeOperation(parent, accessCnt);
                }
            } else {
                accessCnt.noAccess++;
            }
        }
    }
    
    private void additionalNodeOperation(@NotNull Node node, Cnt accessCnt) {
        try {
            Validate.checkState(node.isCheckedOut());
            if (node.canAddMixin("mix:language")) {
                accessCnt.nodeWrites++;
                node.addMixin("mix:language");
                node.setProperty("jcr:language", "en");
                node.getSession().refresh(false);
            }
        } catch (RepositoryException repositoryException) {
            throw new RuntimeException(repositoryException.getMessage());
        }
    }

    private static final class Cnt {
        private int nodeCnt = 0;
        private int propertyCnt = 0;
        private int nodeWrites = 0;
        private int noAccess = 0;
    }
}