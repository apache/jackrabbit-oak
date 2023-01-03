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

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.jetbrains.annotations.NotNull;

import javax.jcr.SimpleCredentials;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Generate a report with the list of affected versionHistory nodes containing
 * empty version nodes or an incorrect primaryType.
 * This is something that shouldn't happen when Oak is strictly used, but some
 * external tools may introduce this kind of inconsistencies.
 */
public class GenerateVersionInconsistencyReport {

    private static String VERSION_STORAGE_PATH = "/jcr:system/jcr:versionStorage";
    private static int PROGRESS_WAITING_TIME_MILLIS = 20 * 1000;

    private volatile int count = 0;
    private volatile int emptyNodesCount = 0;
    private volatile int wrongNodesCount = 0;
    private volatile boolean runningJob = false;

    /**
     * The tool is not integrated in the Oak-run Command framework as it's not
     * intended for frequent or generic use.
     * @param args At least 2 arguments are expected:
     *             - MongoDB URI
     *             - Repository admin username (optional)
     *             - Repository admin password
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        String adminUser = "admin";
        String adminPwd = "";
        if (args.length == 2) {
            adminPwd = args[1];
        } else if (args.length == 3) {
            adminUser = args[1];
            adminPwd = args[2];
        } else {
            System.out.println("Required arguments: <mongo uri> [repository admin username] <repository admin password>");
            System.out.println("If no username is specified, it will default to 'admin'.");
            System.out.println("Examples:");
            System.out.println("    \"mongodb://127.0.0.1:27017/database\" \"oak123456\"");
            System.out.println("    \"mongodb://127.0.0.1:27017/database\" \"admin\" \"oak123456\"");
            System.out.println();
            System.exit(1);
        }

        GenerateVersionInconsistencyReport reportGenerator = new GenerateVersionInconsistencyReport();
        reportGenerator.generateReport(adminUser, adminPwd, Arrays.copyOf(args, 1));
    }

    private void generateReport(String adminUser, String adminPwd, String...args) throws IOException {
        Closer closer = Utils.createCloserWithShutdownHook();
        String h = "mongodb://host:port/database|jdbc:...";

        DocumentNodeStore dns = null;
        try {
            DocumentNodeStoreBuilder<?> builder = Utils.createDocumentMKBuilder(args, closer, h);

            dns = builder.setClusterInvisible(true).setReadOnlyMode().build();
            closer.register(Utils.asCloseable(dns));

            Oak.OakDefaultComponents defs = new Oak.OakDefaultComponents();
            Oak oak = new Oak(dns).with(new OpenSecurityProvider());

            for (QueryIndexProvider qip : defs.queryIndexProviders()) {
                oak.with(qip);
            }

            ContentRepository cr = oak.createContentRepository();
            ContentSession contentSession = cr.login(new SimpleCredentials(adminUser, adminPwd.toCharArray()), null);
            Root root = contentSession.getLatestRoot();
            Tree tree = root.getTree(VERSION_STORAGE_PATH);

            ReadOnlyNodeTypeManager readOnlyNodeTypeManager = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);
            ReadOnlyVersionManager readOnlyVersionManager = new ReadOnlyVersionManager() {
                @Override
                protected @NotNull Tree getVersionStorage() {  //jcr:system/jcr:versionStorage
                    return root.getTree(VERSION_STORAGE_PATH);
                }

                @Override
                protected @NotNull Root getWorkspaceRoot() {
                    return root;
                }

                @Override
                protected @NotNull ReadOnlyNodeTypeManager getNodeTypeManager() {
                    return readOnlyNodeTypeManager;
                }
            };

            HashSet<Tree> emptyNodes = new HashSet<>();
            HashMap<String, String> wrongNodes = new HashMap<>();

            System.out.println("Searching for inconsistencies in version history...");
            runningJob = true;
            Thread progressThread = new Thread(new ProgressThread());
            progressThread.start();
            iterateAndFindEmptyAndWrongVersionNodes(tree, 3, readOnlyVersionManager, emptyNodes, wrongNodes);
            runningJob = false;
            System.out.println("Job Finished. Traversed: " + count + " nodes. Found " + emptyNodesCount + " empty and " + wrongNodesCount + " wrong nodes.");
            System.out.println();

            System.out.println("=== List 1: Empty nodes ===");
            for (Tree node : emptyNodes) {
                System.out.println(node.getPath());
            }
            System.out.println("Total empty nodes: " + emptyNodes.size());
            System.out.println("=== End of empty nodes list ===");
            System.out.println();

            System.out.println("=== List 2: versionHistory nodes with wrong primaryType ===");
            for (Map.Entry<String, String> entry : wrongNodes.entrySet()) {
                String nodePath = entry.getKey();
                System.out.println(nodePath + " => " + entry.getValue());
            }
            System.out.println("Total wrong nodes: " + wrongNodes.size());
            System.out.println("=== End of versionHistory nodes with wrong primaryType list ===");

            dns.dispose();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            closer.close();
        }
    }

    private void iterateAndFindEmptyAndWrongVersionNodes(Tree root, int level, ReadOnlyVersionManager readOnlyVersionManager, HashSet<Tree> emptyNodes, HashMap<String, String> wrongNodes) {
        for (Tree treeNode : root.getChildren()) {
            count++;
            if (treeNode.getPropertyCount() == 0) {
                emptyNodes.add(treeNode);
                emptyNodesCount++;
            }
            if (level == 6) {
                PropertyState prop = treeNode.getProperty("jcr:primaryType");
                if (prop == null) {
                    wrongNodes.put(treeNode.getPath(), "is null");
                    wrongNodesCount++;
                } else if (Type.STRING.equals(prop.getType())) {
                    wrongNodes.put(treeNode.getPath(), "not a String");
                    wrongNodesCount++;
                } else if (!"nt:versionHistory".equals(prop.getValue(Type.STRING))) {
                    wrongNodes.put(treeNode.getPath(), "is set to " + prop.getValue(Type.STRING));
                    wrongNodesCount++;
                }
            }
            iterateAndFindEmptyAndWrongVersionNodes(treeNode, level+1, readOnlyVersionManager, emptyNodes, wrongNodes);
        }
    }

    private class ProgressThread implements Runnable {
        public void run() {
            try {
                while (runningJob) {
                    System.out.println("Job is running... Traversed " + count + " nodes. Found " + emptyNodesCount + " empty and " + wrongNodesCount + " wrong nodes.");
                    Thread.sleep(PROGRESS_WAITING_TIME_MILLIS);
                }
            } catch (InterruptedException e) {
                System.out.println("ProgressThread was interrupted");
                e.printStackTrace();
            }
        }
    }
}
