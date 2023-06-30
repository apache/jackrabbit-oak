/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.console.commands


import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.console.ConsoleSession
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.apache.jackrabbit.oak.plugins.document.DocumentRevisionCleanupHelper
import org.apache.jackrabbit.oak.plugins.document.DocumentStore
import org.apache.jackrabbit.oak.plugins.document.Revision
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

import java.util.stream.Collectors

import static java.lang.Integer.getInteger

@CompileStatic
class RevisionCommand extends CommandSupport {
    public static final String COMMAND_NAME = 'revision'

    private static final int REVISION_CAP = getInteger("oak.revision.cap", 250)

    private static final String ARGS_SUBCOMMAND_LIST = "list"
    private static final String ARGS_SUBCOMMAND_INIT = "init"
    private static final String ARGS_SUBCOMMAND_SHOW = "show"
    private static final String ARGS_SUBCOMMAND_CLEANUP = "cleanup"

    private static final String ARGS_NO_CAP_RESULTS = "--nocap"
    private static final String ARGS_NO_CAP_RESULTS_SHORT = "-n"
    private static final String ARGS_ORDER_DESC = "--desc"
    private static final String ARGS_ORDER_DESC_SHORT = "-d"

    private static final int ORDER_ASC = 0
    private static final int ORDER_DESC = 1

    private static int order = ORDER_ASC
    private static boolean capResults = true
    private static boolean showPropertiesUse = false

    private DocumentRevisionCleanupHelper cleanupHelper;

    RevisionCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'rev')
    }

    @Override
    Object execute(List<String> args) {
        if (args.isEmpty()) {
            io.err.println("Missing subcommand: [<list>|<init>|<show>|<cleanup>]")
            return null
        }

        parseArgs(args)
        switch(args[0].toLowerCase()) {
            case ARGS_SUBCOMMAND_LIST:
                list()
                break
            case ARGS_SUBCOMMAND_INIT:
                initialize()
                break
            case ARGS_SUBCOMMAND_SHOW:
                show(args.contains("candidates"), args.contains("blocked"))
                break
            case ARGS_SUBCOMMAND_CLEANUP:
                io.err.println("This command is read-only. To execute the cleanup, use 'oak-run revisions' command:")
                io.err.println("  oak-run revisions {<jdbc-uri> | <mongodb-uri>} cleanup [options]")
                io.err.println("  Options:")
                io.err.println("    --path <path>  Path to the node to cleanup. This is required.")
                io.err.println("    --number <number>  Number of revisions to cleanup")
                io.err.println("    --clusterId <clusterId>  ClusterId to cleanup")
                io.err.println("  Example:")
                io.err.println("    oak-run revisions {<jdbc-uri> | <mongodb-uri>} cleanup --path /content --number 10 --clusterId 1")
                io.err.println()
                break
            default:
                io.err.println("Unrecognized subcommand: " + args[0])
        }

        io.out.flush()
        return null
    }

    /**
     * List command.
     * Prints the list of revisions for the working document.
     */
    void list() {
        int count = 0
        NavigableMap<Revision, String> allRevisions = cleanupHelper.getAllRevisions();

        for (Map.Entry<Revision, String> revisionEntry : order == ORDER_ASC ? allRevisions.entrySet() : allRevisions.descendingMap().entrySet()) {
            Revision revision = revisionEntry.getKey()
            String value = revisionEntry.getValue()
            io.out.println(revision.toReadableString() + " " + value)
            count++
            if (capResults && count >= REVISION_CAP) {
                io.out.printf("-- Reached revision cap of %d elements. This document has %d revisions --%n", REVISION_CAP, allRevisions.size())
                break
            }
        }
    }

    /**
     * Outputs the candidate and blocked revision sets. These sets are populated during a previous execution of
     * the info() command.
     * @param args
     */
    void show(boolean candidates, boolean blocked) {
        if (candidates) {
            SortedMap<Integer, TreeSet<Revision>> candidateRevisionsToClean = cleanupHelper.getCandidateRevisionsToClean()
            if (candidateRevisionsToClean.isEmpty()) {
                io.out.println("-- Candidates revision list is empty. Run 'rev init' before --")
            }
            for (Map.Entry<Integer, TreeSet<Revision>> clusterEntry : candidateRevisionsToClean) {
                int count = 0
                io.out.println("ClusterId " + clusterEntry.key)
                for (Revision revision : order == ORDER_ASC ? clusterEntry.value : clusterEntry.value.descendingSet()) {
                    io.out.println("  " + revision.toReadableString())
                    count++
                    if (capResults && count >= REVISION_CAP) {
                        io.out.printf("-- Reached revision cap of %d elements. There are %d revisions in the list --%n", REVISION_CAP, clusterEntry.value.size())
                        break
                    }
                }
            }
        }

        if (blocked) {
            SortedMap<Integer, TreeSet<Revision>> usedRevisionsToKeep = cleanupHelper.getBlockedRevisionsToKeep()
            if (cleanupHelper.getBlockedRevisionsToKeep().isEmpty()) {
                io.out.println("-- Blocked revision list is empty. Run 'rev init' before --")
            }
            for (Map.Entry<Integer, TreeSet<Revision>> clusterEntry : usedRevisionsToKeep) {
                int count = 0
                io.out.println("ClusterId " + clusterEntry.key)
                for (Revision revision : order == ORDER_ASC ? clusterEntry.value : clusterEntry.value.descendingSet()) {
                    io.out.println(revision.toReadableString())
                    count++
                    if (capResults && count >= REVISION_CAP) {
                        io.out.printf("-- Reached revision cap of %d elements. There are %d revisions in the list --%n", REVISION_CAP, clusterEntry.value.size())
                        break
                    }
                }
            }
        }
    }

    /**
     * Performs the cleanup of a certain number of revisions for the specified clusterId, starting from oldest.
     * @param numberToCleanup
     * @param clusterToCleanup
     */
    void cleanup(int numberToCleanup, int clusterToCleanup) {
         io.out.println("This will delete the following revisions and the property values permanently:")
        SortedMap<Integer, TreeSet<Revision>> candidateRevisionsToClean = cleanupHelper.getCandidateRevisionsToClean()
        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = cleanupHelper.getPropertiesModifiedByRevision()
        TreeSet<Revision> revisions = candidateRevisionsToClean.get(clusterToCleanup)
        if (revisions != null) {
            int count = 0
            for (Revision revision : revisions) {
                io.out.println(revision.toReadableString() + " => " + propertiesModifiedByRevision.get(revision))
                count++
                if (count >= numberToCleanup) {
                    break
                }
            }
        } else {
            io.err.println("Invalid clusterId")
            return
        }

        io.out.println("Are you sure to proceed? [y/N]")
        int confirmation = io.in.read()
        io.in.readLine()
        if (confirmation == (int)('y' as char)) {
            // Start the cleanup
            int count = 0
            for (Revision revision : candidateRevisionsToClean.get(clusterToCleanup)) {
                // Remove the revision from _revisions and all the properties
                /*UpdateOp update = new UpdateOp(workingDocument.path.toString(), false)
                update.removeMapEntry("_revisions", revision)
                for (String property : propertiesModifiedByRevision.get(revision)) {
                    update.removeMapEntry(property, revision)
                }
                io.out.println("Executing UpdateOp: " + update)
                try {
                    documentStore.findAndUpdate(NODES, update)
                } catch (DocumentStoreException ex) {
                    io.out.println("Operation failed: " + ex)
                }*/

                count++
                if (count >= numberToCleanup) {
                    break
                }
            }
            io.out.println("-- Executed " + count + " operations --")
        }
    }

    void initialize() {
        NodeStore nodeStore = getSession().getStore()
        assert nodeStore instanceof DocumentNodeStore
        DocumentStore documentStore = nodeStore.getDocumentStore()
        DocumentNodeStore documentNodeStore = (DocumentNodeStore) nodeStore

        cleanupHelper = new DocumentRevisionCleanupHelper(documentStore, documentNodeStore, session.getWorkingPath())

        cleanupHelper.initializeCleanupProcess()

        SortedMap<Revision, String> allRevisions = cleanupHelper.getAllRevisions()
        TreeMap<Integer, Integer> revisionsByClusterId = allRevisions.keySet().groupBy { it.clusterId }
                .collectEntries { clusterId, revisions ->
                    [clusterId, revisions.size()]
                } as TreeMap<Integer, Integer>

        io.out.println("=== Last Revision by clusterId ===")
        for (Map.Entry<Integer, Revision> entry : cleanupHelper.getLastRev()) {
            io.out.printf("  [%d] -> %s%n", entry.key, entry.value.toReadableString())
        }
        io.out.println()

        io.out.println("=== Sweep Revision by clusterId ===")
        for (Map.Entry<Integer, Revision> entry : cleanupHelper.getSweepRev()) {
            io.out.printf("  [%d] -> %s%n", entry.key, entry.value.toReadableString())
        }
        io.out.println()

        io.out.println("=== Total Revisions by clusterId ===")
        for (Map.Entry<Integer, Integer> entry : revisionsByClusterId) {
            io.out.printf("  [%d] -> %d revisions%n", entry.key, entry.value)
        }
        io.out.println()

        io.out.println("=== Candidates to cleanup ===")
        cleanupHelper.getCandidateRevisionsToClean().each { clusterId, revisions ->
            int blocked = cleanupHelper.getBlockedRevisionsToKeep().get(clusterId)?.size() ?: 0
            io.out.printf("ClusterId [%d] has %d Candidates and %d Blocked%n", clusterId, revisions.size(), blocked)
        }

        io.out.println("The lists are stored temporarily. You can print them using:")
        io.out.println("  > rev show candidates")
        io.out.println("  > rev show blocked")
        io.out.println()
    }

    void parseArgs(List<String> args) {
        // Defaults
        order = ORDER_ASC
        capResults = true
        showPropertiesUse = false

        for (String arg : args) {
            switch(arg) {
                case ARGS_NO_CAP_RESULTS:
                case ARGS_NO_CAP_RESULTS_SHORT:
                    capResults = false
                    break
                case ARGS_ORDER_DESC:
                case ARGS_ORDER_DESC_SHORT:
                    order = ORDER_DESC
                    break
            }
        }
    }

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }
}
