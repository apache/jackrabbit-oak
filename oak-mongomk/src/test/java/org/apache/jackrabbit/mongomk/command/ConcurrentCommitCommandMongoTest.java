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
package org.apache.jackrabbit.mongomk.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.command.CommandExecutorImpl;
import org.apache.jackrabbit.mongomk.impl.model.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.CommitImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class ConcurrentCommitCommandMongoTest extends BaseMongoTest {

    @Test
    public void testConflictingConcurrentUpdate() throws Exception {
        int numOfConcurrentThreads = 5;
        final Object waitLock = new Object();

        // create the commands
        List<CommitCommandMongo> commands = new ArrayList<CommitCommandMongo>(numOfConcurrentThreads);
        for (int i = 0; i < numOfConcurrentThreads; ++i) {
            List<Instruction> instructions = new LinkedList<Instruction>();
            instructions.add(new AddNodeInstructionImpl("/", String.valueOf(i)));
            Commit commit = new CommitImpl("/", "+" + i + " : {}",
                    "This is a concurrent commit", instructions);
            CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit) {
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
        final CommandExecutor commandExecutor = new CommandExecutorImpl();
        ExecutorService executorService = Executors.newFixedThreadPool(numOfConcurrentThreads);
        final List<Long> revisionIds = new LinkedList<Long>();
        for (int i = 0; i < numOfConcurrentThreads; ++i) {
            final CommitCommandMongo command = commands.get(i);
            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    try {
                        Long revisionId = commandExecutor.execute(command);
                        revisionIds.add(revisionId);
                    } catch (Exception e) {
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
            GetNodesCommandMongo command2 = new GetNodesCommandMongo(mongoConnection, "/", revisionId, 0);
            Node root = command2.execute();
            Set<Node> children = root.getChildren();
            for (String lastChild : lastChildren) {
                boolean contained = false;
                for (Node childNode : children) {
                    if (childNode.getName().equals(lastChild)) {
                        contained = true;
                        break;
                    }
                }
                Assert.assertTrue(contained);
            }
            lastChildren.clear();
            for (Node childNode : children) {
                lastChildren.add(childNode.getName());
            }
        }

        // TODO Assert the number of commits
    }
}
