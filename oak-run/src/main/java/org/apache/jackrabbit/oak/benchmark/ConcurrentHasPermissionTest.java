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

import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;

/**
 * Concurrently calls Session#hasPermission on the deep tree:
 * - the path argument a random path out of the deep tree
 * - the actions are randomly selected from the combinations listed in {@link #ACTIONS}
 */
public class ConcurrentHasPermissionTest extends ConcurrentReadDeepTreeTest {

    private static final List<String> ACTIONS = ImmutableList.of(
            Session.ACTION_READ,
            Session.ACTION_ADD_NODE,
            Session.ACTION_SET_PROPERTY,
            Session.ACTION_REMOVE,
            Session.ACTION_READ + "," + Session.ACTION_ADD_NODE + "," + Session.ACTION_SET_PROPERTY + "," + Session.ACTION_REMOVE,
            Session.ACTION_ADD_NODE + "," + Session.ACTION_SET_PROPERTY + "," + Session.ACTION_REMOVE,
            Session.ACTION_ADD_NODE + "," + Session.ACTION_REMOVE,
            Session.ACTION_SET_PROPERTY + "," + Session.ACTION_REMOVE,
            Session.ACTION_READ + "," + Session.ACTION_ADD_NODE,
            Session.ACTION_READ + "," + Session.ACTION_SET_PROPERTY
    );

    protected ConcurrentHasPermissionTest(boolean runAsAdmin, int itemsToRead, boolean doReport) {
        super(runAsAdmin, itemsToRead, doReport);
    }

    protected void randomRead(Session testSession, List<String> allPaths, int cnt) throws RepositoryException {
        boolean logout = false;
        if (testSession == null) {
            testSession = getTestSession();
            logout = true;
        }
        try {
            int allows = 0;
            int denies = 0;
            int size = allPaths.size();
            long start = System.currentTimeMillis();
            for (int i = 0; i < cnt; i++) {
                double rand = size * Math.random();
                int index = (int) Math.floor(rand);
                String path = allPaths.get(index);

                String actions = getRandomActions();
                if (testSession.hasPermission(path, actions)) {
                    allows++;
                } else {
                    denies++;
                }
            }
            long end = System.currentTimeMillis();
            if (doReport) {
                System.out.println("Session " + testSession.getUserID() + " calling #hasPermission (Allows: "+ allows +"; Denies: "+ denies +") completed in " + (end - start));
            }
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }

    private static String getRandomActions() {
        int index = (int) Math.floor(10 * Math.random());
        return ACTIONS.get(index);
    }
}