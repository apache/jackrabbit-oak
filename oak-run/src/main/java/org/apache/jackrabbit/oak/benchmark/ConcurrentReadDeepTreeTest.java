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

import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Concurrently reads random items from the deep tree.
 */
public class ConcurrentReadDeepTreeTest extends AbstractDeepTreeTest {

    private int bgReaders = 50;
    private int cnt = 10000;

    public ConcurrentReadDeepTreeTest(boolean runAsAdmin) {
        super(runAsAdmin);
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        for (int i = 0; i < bgReaders; i++) {
            addBackgroundJob(new RandomRead(getTestSession()));
        }
    }

    @Override
    protected void runTest() throws Exception {
        Session testSession = getTestSession();
        RandomRead randomRead = new RandomRead(testSession);
        randomRead.run();
        testSession.logout();
    }

    private class RandomRead implements Runnable {

        private final Session testSession;

        private RandomRead(Session testSession) {
            this.testSession = testSession;
        }

        public void run() {
            try {
                randomRead(testSession, allPaths, cnt, true);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }
    }
}