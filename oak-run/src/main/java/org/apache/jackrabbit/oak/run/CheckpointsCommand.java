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

package org.apache.jackrabbit.oak.run;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.google.common.io.Closer;

import org.apache.jackrabbit.oak.checkpoint.Checkpoints;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.run.commons.Command;


class CheckpointsCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {

        CheckpointsOptions options = new CheckpointsOptions("checkpoints {<path>|<mongo-uri>|<jdbc-uri>} [list|rm-all|rm-unreferenced|rm <checkpoint>|info <checkpoint>|set <checkpoint> <name> [<value>]] [--segment]").parse(args);;
        if (options.isHelp()) {
            options.printHelpOn(System.out);
            System.exit(0);
        }

        String storeArg = options.getStoreArg();
        if (storeArg == null || storeArg.length() == 0) {
            System.err.println("Missing nodestore path/URI");
            System.exit(1);
        }
        List<String> opArg = options.getOtherArgs();

        boolean success = false;
        Checkpoints cps;
        Closer closer = Utils.createCloserWithShutdownHook();
        try {
            String op = "list"; //default operation
            if (opArg.size() > 0) {
                op = opArg.get(0);
                if (!"list".equals(op) && !"rm-all".equals(op) && !"rm-unreferenced".equals(op) && !"rm".equals(op) && !"info".equals(op) && !"set".equals(op)) {
                    failWith("Unknown operation: " + op);
                }
            }

            boolean isDocumentNodeStore = storeArg.startsWith("jdbc:") || storeArg.startsWith("mongodb:");

            if (isDocumentNodeStore) {
                DocumentNodeStoreBuilder<?> builder = Utils.createDocumentMKBuilder(options, closer);
                if (builder == null) {
                    System.err.println("Could not create DocumentNodeStoreBuilder");
                    System.exit(1);
                }
                builder.setClusterInvisible(true);
                DocumentNodeStore nodeStore = builder.build();
                closer.register(Utils.asCloseable(nodeStore));
                cps = Checkpoints.onDocumentMK(nodeStore);
            } else {
                cps = Checkpoints.onSegmentTar(new File(storeArg), closer);
            }

            System.out.println("Checkpoints " + storeArg);
            if ("list".equals(op)) {
                int cnt = 0;
                for (Checkpoints.CP cp : cps.list()) {
                    System.out.printf("- %s created %s expires %s%n",
                            cp.id,
                            new Timestamp(cp.created),
                            new Timestamp(cp.expires));
                    cnt++;
                }
                System.out.println("Found " + cnt + " checkpoints");
            } else if ("rm-all".equals(op)) {
                long time = System.currentTimeMillis();
                long cnt = cps.removeAll();
                time = System.currentTimeMillis() - time;
                if (cnt != -1) {
                    System.out.println("Removed " + cnt + " checkpoints in " + time + "ms.");
                } else {
                    failWith("Failed to remove all checkpoints.");
                }
            } else if ("rm-unreferenced".equals(op)) {
                long time = System.currentTimeMillis();
                long cnt = cps.removeUnreferenced();
                time = System.currentTimeMillis() - time;
                if (cnt != -1) {
                    System.out.println("Removed " + cnt + " checkpoints in " + time + "ms.");
                } else {
                    failWith("Failed to remove unreferenced checkpoints.");
                }
            } else if ("rm".equals(op)) {
                if (opArg.size() < 2) {
                    failWith("Missing checkpoint id");
                } else {
                    String cp = opArg.get(1);
                    long time = System.currentTimeMillis();
                    int cnt = cps.remove(cp);
                    time = System.currentTimeMillis() - time;
                    if (cnt != 0) {
                        if (cnt == 1) {
                            System.out.println("Removed checkpoint " + cp + " in "
                                    + time + "ms.");
                        } else {
                            failWith("Failed to remove checkpoint " + cp);
                        }
                    } else {
                        failWith("Checkpoint '" + cp + "' not found.");
                    }
                }
            } else if ("info".equals(op)) {
                if (opArg.size() < 2) {
                    failWith("Missing checkpoint id");
                } else {
                    String cp = opArg.get(1);
                    Map<String, String> info = cps.getInfo(cp);
                    if (info != null) {
                        for (Map.Entry<String, String> e : info.entrySet()) {
                            System.out.println(e.getKey() + '\t' + e.getValue());
                        }
                    } else {
                        failWith("Checkpoint '" + cp + "' not found.");
                    }
                }
            } else if ("set".equals(op)) {
                if (opArg.size() < 3) {
                    failWith("Missing checkpoint id");
                } else {
                    String cp = opArg.get(1);
                    String name = opArg.get(2);
                    String value = null;
                    if (opArg.size() >= 4) {
                        value = opArg.get(3);
                    }
                    long time = System.currentTimeMillis();
                    int cnt = cps.setInfoProperty(cp, name, value);
                    time = System.currentTimeMillis() - time;
                    if (cnt != 0) {
                        if (cnt == 1) {
                            System.out.println("Updated checkpoint " + cp + " in "
                                    + time + "ms.");
                        } else {
                            failWith("Failed to remove checkpoint " + cp);
                        }
                    } else {
                        failWith("Checkpoint '" + cp + "' not found.");
                    }
                }
            }
            success = true;
        } catch (Throwable t) {
            System.err.println(t.getMessage());
        } finally {
            closer.close();
        }
        if (!success) {
            System.exit(1);
        }
    }

    private static void failWith(String message) {
        throw new RuntimeException(message);
    }

    private static final class CheckpointsOptions extends Utils.NodeStoreOptions {

        CheckpointsOptions(String usage) {
            super(usage);
        }

        @Override
        public CheckpointsCommand.CheckpointsOptions parse(String[] args) {
            super.parse(args);
            return this;
        }

       boolean isHelp() {
            return options.has(help);
        }
    }
}