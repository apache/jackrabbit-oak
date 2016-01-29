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

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.openFileStore;

import java.sql.Timestamp;

import com.google.common.io.Closer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import org.apache.jackrabbit.oak.checkpoint.Checkpoints;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;

class CheckpointsCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        if (args.length == 0) {
            System.out
                    .println("usage: checkpoints {<path>|<mongo-uri>} [list|rm-all|rm-unreferenced|rm <checkpoint>]");
            System.exit(1);
        }
        boolean success = false;
        Checkpoints cps;
        Closer closer = Closer.create();
        try {
            String op = "list";
            if (args.length >= 2) {
                op = args[1];
                if (!"list".equals(op) && !"rm-all".equals(op) && !"rm-unreferenced".equals(op) && !"rm".equals(op)) {
                    failWith("Unknown command.");
                }
            }

            if (args[0].startsWith(MongoURI.MONGODB_PREFIX)) {
                MongoClientURI uri = new MongoClientURI(args[0]);
                MongoClient client = new MongoClient(uri);
                final DocumentNodeStore store = new DocumentMK.Builder()
                        .setMongoDB(client.getDB(uri.getDatabase()))
                        .getNodeStore();
                closer.register(Utils.asCloseable(store));
                cps = Checkpoints.onDocumentMK(store);
            } else {
                FileStore store = openFileStore(args[0]);
                closer.register(Utils.asCloseable(store));
                cps = Checkpoints.onTarMK(store);
            }

            System.out.println("Checkpoints " + args[0]);
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
                if (args.length != 3) {
                    failWith("Missing checkpoint id");
                } else {
                    String cp = args[2];
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
