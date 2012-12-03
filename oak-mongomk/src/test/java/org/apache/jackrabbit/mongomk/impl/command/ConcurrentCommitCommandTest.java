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
package org.apache.jackrabbit.mongomk.impl.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Test;

public class ConcurrentCommitCommandTest extends BaseMongoMicroKernelTest {

    @Test
    public void testConflictingConcurrentUpdate() throws Exception {
        int numOfConcurrentThreads = 5;
        final Object waitLock = new Object();

        // create the commands
        List<CommitCommandNew> commands = new ArrayList<CommitCommandNew>(numOfConcurrentThreads);
        for (int i = 0; i < numOfConcurrentThreads; ++i) {
            Commit commit = CommitBuilder.build("/", "+\"" + i + "\" : {}",
                    "This is a concurrent commit");
            CommitCommandNew command = new CommitCommandNew(getNodeStore(), commit) {
                @Override
                protected boolean saveAndSetHeadRevision() throws Exception {
                    try {
                        synchronized (waitLock) {
                            waitLock.wait();
                        }

                        return super.saveAndSetHeadRevision();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return false;
                    }
                };
            };
            commands.add(command);
        }

        // execute the commands
        final CommandExecutor commandExecutor = new DefaultCommandExecutor();
        ExecutorService executorService = Executors.newFixedThreadPool(numOfConcurrentThreads);
        final List<Long> revisionIds = new LinkedList<Long>();
        for (int i = 0; i < numOfConcurrentThreads; ++i) {
            final CommitCommandNew command = commands.get(i);
            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    try {
                        Long revisionId = commandExecutor.execute(command);
                        revisionIds.add(revisionId);
                    } catch (Exception e) {
                        e.printStackTrace();
                        revisionIds.add(null);
                    }
                }
            };
            executorService.execute(runnable);
        }

        // notify the wait lock to execute the command concurrently
        do {
            Thread.sleep(1500);
            synchronized (waitLock) {
                waitLock.notifyAll();
            }
        } while (revisionIds.size() < numOfConcurrentThreads);

        // Verify the result by sorting the revision ids and verifying that all
        // children are contained in the next revision
        Collections.sort(revisionIds, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        });
        List<String> lastChildren = new LinkedList<String>();
        for (int i = 0; i < numOfConcurrentThreads; ++i) {
            Long revisionId = revisionIds.get(i);
            GetNodesCommandNew command2 = new GetNodesCommandNew(getNodeStore(),
                    "/", revisionId);
            Node root = command2.execute();

            for (String lastChild : lastChildren) {
                boolean contained = false;
                for (Iterator<Node> it = root.getChildNodeEntries(0, -1); it.hasNext(); ) {
                    Node childNode = it.next();
                    String childName = PathUtils.getName(childNode.getPath());
                    if (childName.equals(lastChild)) {
                        contained = true;
                        break;
                    }
                }
                assertTrue(contained);
            }
            lastChildren.clear();
            for (Iterator<Node> it = root.getChildNodeEntries(0, -1); it.hasNext(); ) {
                Node childNode = it.next();
                String childName = PathUtils.getName(childNode.getPath());
                lastChildren.add(childName);
            }
        }

        // Assert number of successful commits.
        List<MongoCommit> commits = new FetchCommitsAction(getNodeStore()).execute();
        assertEquals(numOfConcurrentThreads + 1, commits.size());
    }
}
