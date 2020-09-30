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
package org.apache.jackrabbit.oak.benchmark.authorization.principalbased;

import org.jetbrains.annotations.NotNull;

import javax.jcr.Item;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.List;

public class HasItemGetItemIsModifiedTest extends PrinicipalBasedReadTest {

    public HasItemGetItemIsModifiedTest(int itemsToRead, int numberOfACEs, int subjectSize, boolean entriesForEachPrincipal, boolean testDefault, @NotNull String compositionType, boolean useAggregationFilter, boolean doReport) {
        super(itemsToRead, numberOfACEs, subjectSize, entriesForEachPrincipal, testDefault, compositionType, useAggregationFilter, doReport);
    }

    @Override
    protected void randomRead(Session testSession, List<String> allPaths, int cnt) throws RepositoryException {
        boolean logout = false;
        if (testSession == null) {
            testSession = getTestSession();
            logout = true;
        }
        try {
            int nodeCnt = 0;
            int propertyCnt = 0;
            int noAccess = 0;
            int size = allPaths.size();
            long start = System.currentTimeMillis();
            for (int i = 0; i < cnt; i++) {
                double rand = size * Math.random();
                int index = (int) Math.floor(rand);
                String path = allPaths.get(index);
                if (testSession.itemExists(path)) {
                    Item item = testSession.getItem(path);
                    if (item.isNode()) {
                        nodeCnt++;
                    } else {
                        propertyCnt++;
                    }
                    item.isModified();
                } else {
                    noAccess++;
                }
            }
            long end = System.currentTimeMillis();
            if (doReport) {
                System.out.println("Session " + testSession.getUserID() + " reading " + cnt + " (Nodes: "+ nodeCnt +"; Properties: "+propertyCnt+"; no access: "+noAccess+") completed in " + (end - start));
            }
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }

    @NotNull
    @Override
    protected String getTestNodeName() {
        return "HasItemGetItemIsModifiedTest";
    }
}