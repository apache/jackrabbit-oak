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

import org.apache.jackrabbit.oak.checkpoint.Checkpoints;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;

import com.google.common.io.Closer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;

class CheckpointsCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSet options = parser.parse(args);

        if (options.nonOptionArguments().isEmpty()) {
            System.out.println("usage: checkpoints {<path>|<mongo-uri>} [list|rm-all|rm-unreferenced|rm <checkpoint>|info <checkpoint>|set <checkpoint> <name> [<value>]] [--segment]");
            System.exit(1);
        }

        boolean success = false;
        Checkpoints cps;
        Closer closer = Closer.create();
        try {
            String op = "list";
            if (options.nonOptionArguments().size() >= 2) {
                op = options.nonOptionArguments().get(1).toString();
                if (!"list".equals(op) && !"rm-all".equals(op) && !"rm-unreferenced".equals(op) && !"rm".equals(op) && !"info".equals(op) && !"set".equals(op)) {
                    failWith("Unknown command.");
                }
            }

            String connection = options.nonOptionArguments().get(0).toString();
            if (connection.startsWith(MongoURI.MONGODB_PREFIX)) {
                MongoClientURI uri = new MongoClientURI(connection);
                MongoClient client = new MongoClient(uri);
                final DocumentNodeStore store = newMongoDocumentNodeStoreBuilder()
                        .setMongoDB(client.getDB(uri.getDatabase()))
                        .build();
                closer.register(Utils.asCloseable(store));
                cps = Checkpoints.onDocumentMK(store);
            } else {
                cps = Checkpoints.onSegmentTar(new File(connection), closer);
            }

            System.out.println("Checkpoints " + connection);
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
                if (options.nonOptionArguments().size() < 3) {
                    failWith("Missing checkpoint id");
                } else {
                    String cp = options.nonOptionArguments().get(2).toString();
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
                if (options.nonOptionArguments().size() < 3) {
                    failWith("Missing checkpoint id");
                } else {
                    String cp = options.nonOptionArguments().get(2).toString();
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
                if (options.nonOptionArguments().size() < 4) {
                    failWith("Missing checkpoint id");
                } else {
                    List<?> l = options.nonOptionArguments();
                    String cp = l.get(2).toString();
                    String name = l.get(3).toString();
                    String value = null;
                    if (l.size() >= 5) {
                        value = l.get(4).toString();
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

}