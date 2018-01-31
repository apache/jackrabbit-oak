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

package org.apache.jackrabbit.oak.segment.file.tooling;

import static java.text.DateFormat.getDateTimeInstance;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.IOMonitorAdapter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Utility for checking the files of a
 * {@link FileStore} for inconsistency and
 * reporting that latest consistent revision.
 */
public class ConsistencyChecker implements Closeable {

    private static final String CHECKPOINT_INDENT = "  ";

    private static final String NO_INDENT = "";

    private static class StatisticsIOMonitor extends IOMonitorAdapter {

        private final AtomicLong ioOperations = new AtomicLong(0);

        private final AtomicLong readBytes = new AtomicLong(0);

        private final AtomicLong readTime = new AtomicLong(0);

        @Override
        public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
            ioOperations.incrementAndGet();
            readBytes.addAndGet(length);
            readTime.addAndGet(elapsed);
        }

    }

    private final StatisticsIOMonitor statisticsIOMonitor = new StatisticsIOMonitor();

    private final ReadOnlyFileStore store;

    private final long debugInterval;
    
    private final PrintWriter outWriter;
    
    private final PrintWriter errWriter;

    private int nodeCount;
    
    private int propertyCount;
    
    private int checkCount;

    /**
     * Run a full traversal consistency check.
     *
     * @param directory  directory containing the tar files
     * @param journalFileName  name of the journal file containing the revision history
     * @param debugInterval    number of seconds between printing progress information to
     *                         the console during the full traversal phase.
     * @param checkBinaries    if {@code true} full content of binary properties will be scanned
     * @param checkHead        if {@code true} will check the head
     * @param checkpoints      collection of checkpoints to be checked 
     * @param filterPaths      collection of repository paths to be checked                         
     * @param ioStatistics     if {@code true} prints I/O statistics gathered while consistency 
     *                         check was performed
     * @param outWriter        text output stream writer
     * @param errWriter        text error stream writer                        
     * @throws IOException
     * @throws InvalidFileStoreVersionException
     */
    public static void checkConsistency(
            File directory,
            String journalFileName,
            long debugInterval,
            boolean checkBinaries,
            boolean checkHead,
            Set<String> checkpoints,
            Set<String> filterPaths,
            boolean ioStatistics,
            PrintWriter outWriter,
            PrintWriter errWriter
    ) throws IOException, InvalidFileStoreVersionException {
        try (
                JournalReader journal = new JournalReader(new File(directory, journalFileName));
                ConsistencyChecker checker = new ConsistencyChecker(directory, debugInterval, ioStatistics, outWriter, errWriter)
        ) {
            Set<String> checkpointsSet = Sets.newLinkedHashSet();
            List<PathToCheck> headPaths = new ArrayList<>();
            Map<String, List<PathToCheck>> checkpointPaths = new HashMap<>();
            
            int revisionCount = 0;
            
            if (!checkpoints.isEmpty()) {
                checkpointsSet.addAll(checkpoints);

                if (checkpointsSet.remove("/checkpoints")) {
                    checkpointsSet = Sets
                            .newLinkedHashSet(SegmentNodeStoreBuilders.builder(checker.store).build().checkpoints());
                }
            }
            
            for (String path : filterPaths) {
                if (checkHead) {
                    headPaths.add(new PathToCheck(path, null));
                    checker.checkCount++;
                }
                
                for (String checkpoint : checkpointsSet) {
                    List<PathToCheck> pathList = checkpointPaths.get(checkpoint);
                    if (pathList == null) {
                        pathList = new ArrayList<>();
                        checkpointPaths.put(checkpoint, pathList);
                    }

                    pathList.add(new PathToCheck(path, checkpoint));
                    checker.checkCount++;
                }
            }

            int initialCount = checker.checkCount;
            JournalEntry lastValidJournalEntry = null;
            
            while (journal.hasNext() && checker.checkCount > 0) {
                JournalEntry journalEntry = journal.next();
                String revision = journalEntry.getRevision();
                
                try {
                    revisionCount++;
                    checker.store.setRevision(revision);
                    boolean overallValid = true;
                    
                    SegmentNodeStore sns = SegmentNodeStoreBuilders.builder(checker.store).build();
                    
                    checker.print("\nChecking revision {0}", revision);

                    if (checkHead) {
                        boolean mustCheck = headPaths.stream().anyMatch(p -> p.journalEntry == null);
                        
                        if (mustCheck) {
                            checker.print("\nChecking head\n");
                            NodeState root = sns.getRoot();
                            overallValid = overallValid && checker.checkPathsAtRoot(headPaths, root, journalEntry, checkBinaries);
                        }
                    }
                    
                    if (!checkpointsSet.isEmpty()) {
                        Map<String, Boolean> checkpointsToCheck = checkpointPaths.entrySet().stream().collect(Collectors.toMap(
                                Map.Entry::getKey, e -> e.getValue().stream().anyMatch(p -> p.journalEntry == null)));
                        boolean mustCheck = checkpointsToCheck.values().stream().anyMatch(v -> v == true);
                        
                        if (mustCheck) {
                            checker.print("\nChecking checkpoints");

                            for (String checkpoint : checkpointsSet) {
                                if (checkpointsToCheck.get(checkpoint)) {
                                    checker.print("\nChecking checkpoint {0}", checkpoint);

                                    List<PathToCheck> pathList = checkpointPaths.get(checkpoint);
                                    NodeState root = sns.retrieve(checkpoint);

                                    if (root == null) {
                                        checker.printError("Checkpoint {0} not found in this revision!", checkpoint);
                                        overallValid = false;
                                    } else {
                                        overallValid = overallValid && checker.checkPathsAtRoot(pathList, root,
                                                journalEntry, checkBinaries);
                                    }
                                }
                            }
                        }
                    }
                    
                    if (overallValid) {
                        lastValidJournalEntry = journalEntry;
                    }
                } catch (IllegalArgumentException e) {
                    checker.printError("Skipping invalid record id {0}", revision);
                }
            }
            
            checker.print("\nSearched through {0} revisions and {1} checkpoints", revisionCount, checkpointsSet.size());
            
            if (initialCount == checker.checkCount) {
                checker.print("No good revision found");
            } else {
                if (checkHead) {
                    checker.print("\nHead");
                    checker.printResults(headPaths, NO_INDENT);
                }
                
                if (!checkpointsSet.isEmpty()) {
                    checker.print("\nCheckpoints");
                    
                    for (String checkpoint : checkpointsSet) {
                        List<PathToCheck> pathList = checkpointPaths.get(checkpoint);
                        checker.print("- {0}", checkpoint);
                        checker.printResults(pathList, CHECKPOINT_INDENT);
                    }
                }
                
                checker.print("\nOverall");
                checker.printOverallResults(lastValidJournalEntry);
            }

            if (ioStatistics) {
                checker.print(
                        "[I/O] Segment read: Number of operations: {0}",
                        checker.statisticsIOMonitor.ioOperations
                );
                checker.print(
                        "[I/O] Segment read: Total size: {0} ({1} bytes)",
                        humanReadableByteCount(checker.statisticsIOMonitor.readBytes.get()),
                        checker.statisticsIOMonitor.readBytes
                );
                checker.print(
                        "[I/O] Segment read: Total time: {0} ns",
                        checker.statisticsIOMonitor.readTime
                );
            }
        }
    }

    private void printResults(List<PathToCheck> pathList, String indent) {
        for (PathToCheck ptc : pathList) {
            String revision = ptc.journalEntry != null ? ptc.journalEntry.getRevision() : null;
            long timestamp = ptc.journalEntry != null ? ptc.journalEntry.getTimestamp() : -1L;
            
            print("{0}Latest good revision for path {1} is {2} from {3}", indent, ptc.path,
                    toString(revision), toString(timestamp));
        }
    }
    
    private void printOverallResults(JournalEntry journalEntry) {
        String revision = journalEntry != null ? journalEntry.getRevision() : null;
        long timestamp = journalEntry != null ? journalEntry.getTimestamp() : -1L;
        
        print("Latest good revision for paths and checkpoints checked is {0} from {1}", toString(revision), toString(timestamp));
    }

    private static String toString(String revision) {
        if (revision != null) {
            return revision;
        } else {
            return "none";
        }
    }
    
    private static String toString(long timestamp) {
        if (timestamp != -1L) {
            return getDateTimeInstance().format(new Date(timestamp));
        } else {
            return "unknown date";
        }
    }
    
    /**
     * Create a new consistency checker instance
     *
     * @param directory        directory containing the tar files
     * @param debugInterval    number of seconds between printing progress information to
     *                         the console during the full traversal phase.
     * @param ioStatistics     if {@code true} prints I/O statistics gathered while consistency 
     *                         check was performed
     * @param outWriter        text output stream writer
     * @param errWriter        text error stream writer                        
     * @throws IOException
     */
    public ConsistencyChecker(File directory, long debugInterval, boolean ioStatistics, PrintWriter outWriter,
            PrintWriter errWriter) throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder builder = fileStoreBuilder(directory);
        if (ioStatistics) {
            builder.withIOMonitor(statisticsIOMonitor);
        }
        this.store = builder.buildReadOnly();
        this.debugInterval = debugInterval;
        this.outWriter = outWriter;
        this.errWriter = errWriter;
    }

    /**
     * Checks for consistency a list of paths, relative to the same root.
     * 
     * @param paths             paths to check
     * @param root              root relative to which the paths are retrieved
     * @param journalEntry      entry containing the current revision checked
     * @param checkBinaries     if {@code true} full content of binary properties will be scanned
     * @return                  {@code true}, if the whole list of paths is consistent
     */
    private boolean checkPathsAtRoot(List<PathToCheck> paths, NodeState root, JournalEntry journalEntry,
            boolean checkBinaries) {
        boolean result = true;
        
        for (PathToCheck ptc : paths) {
            if (ptc.journalEntry == null) {
                String corruptPath = checkPathAtRoot(ptc, root, checkBinaries);

                if (corruptPath == null) {
                    print("Path {0} is consistent", ptc.path);
                    ptc.journalEntry = journalEntry;
                    checkCount--;
                } else {
                    result = false;
                    ptc.corruptPaths.add(corruptPath);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Checks the consistency of the supplied {@code ptc} relative to the given {@code root}. 
     * 
     * @param ptc           path to check, provided there are no corrupt paths.
     * @param root          root relative to which the path is retrieved
     * @param checkBinaries if {@code true} full content of binary properties will be scanned
     * @return              {@code null}, if the content tree rooted at path (possibly under a checkpoint) 
     *                      is consistent in this revision or the path of the first inconsistency otherwise.  
     */
    private String checkPathAtRoot(PathToCheck ptc, NodeState root, boolean checkBinaries) {
        String result = null;

        for (String corruptPath : ptc.corruptPaths) {
            try {
                NodeWrapper wrapper = NodeWrapper.deriveTraversableNodeOnPath(root, corruptPath);
                result = checkNode(wrapper.node, wrapper.path, checkBinaries);

                if (result != null) {
                    return result;
                }
            } catch (IllegalArgumentException e) {
                debug("Path {0} not found", corruptPath);
            }
        }

        nodeCount = 0;
        propertyCount = 0;

        print("Checking {0}", ptc.path);
        
        try {        
            NodeWrapper wrapper = NodeWrapper.deriveTraversableNodeOnPath(root, ptc.path);
            result = checkNodeAndDescendants(wrapper.node, wrapper.path, checkBinaries);
            print("Checked {0} nodes and {1} properties", nodeCount, propertyCount);
            
            return result;
        } catch (IllegalArgumentException e) {
            printError("Path {0} not found", ptc.path);
            return ptc.path;
        } 
    }
    
    /**
     * Checks the consistency of a node and its properties at the given path.
     * 
     * @param node              node to be checked
     * @param path              path of the node
     * @param checkBinaries     if {@code true} full content of binary properties will be scanned
     * @return                  {@code null}, if the node is consistent, 
     *                          or the path of the first inconsistency otherwise.
     */
    private String checkNode(NodeState node, String path, boolean checkBinaries) {
        try {
            debug("Traversing {0}", path);
            nodeCount++;
            for (PropertyState propertyState : node.getProperties()) {
                Type<?> type = propertyState.getType();
                boolean checked = false;
                
                if (type == BINARY) {
                    checked = traverse(propertyState.getValue(BINARY), checkBinaries);
                } else if (type == BINARIES) {
                    for (Blob blob : propertyState.getValue(BINARIES)) {
                        checked = checked | traverse(blob, checkBinaries);
                    }
                } else {
                    propertyState.getValue(type);
                    propertyCount++;
                    checked = true;
                }
                
                if (checked) {
                    debug("Checked {0}/{1}", path, propertyState);
                }
            }
            
            return null;
        } catch (RuntimeException | IOException e) {
            printError("Error while traversing {0}: {1}", path, e);
            return path;
        }
    }
    
    /**
     * Recursively checks the consistency of a node and its descendants at the given path.
     * @param node          node to be checked
     * @param path          path of the node
     * @param checkBinaries if {@code true} full content of binary properties will be scanned
     * @return              {@code null}, if the node is consistent, 
     *                      or the path of the first inconsistency otherwise.
     */
    private String checkNodeAndDescendants(NodeState node, String path, boolean checkBinaries) {
        String result = checkNode(node, path, checkBinaries);
        if (result != null) {
            return result;
        }
        
        try {
            for (ChildNodeEntry cne : node.getChildNodeEntries()) {
                String childName = cne.getName();
                NodeState child = cne.getNodeState();
                result = checkNodeAndDescendants(child, concat(path, childName), checkBinaries);
                if (result != null) {
                    return result;
                }
            }

            return null;
        } catch (RuntimeException e) {
            printError("Error while traversing {0}: {1}", path, e.getMessage());
            return path;
        }
    }
    
    static class NodeWrapper {
        final NodeState node;
        final String path;
        
        NodeWrapper(NodeState node, String path) {
            this.node = node;
            this.path = path;
        }
        
        static NodeWrapper deriveTraversableNodeOnPath(NodeState root, String path) {
            String parentPath = getParentPath(path);
            String name = getName(path);
            NodeState parent = getNode(root, parentPath);
            
            if (!denotesRoot(path)) {
                if (!parent.hasChildNode(name)) {
                    throw new IllegalArgumentException("Invalid path: " + path);
                }
                
                return new NodeWrapper(parent.getChildNode(name), path);
            } else {
                return new NodeWrapper(parent, parentPath);
            }
        }
    }
    
    static class PathToCheck {
        final String path;
        final String checkpoint;
        
        JournalEntry journalEntry;
        Set<String> corruptPaths = new LinkedHashSet<>();
        
        PathToCheck(String path, String checkpoint) {
            this.path = path;
            this.checkpoint = checkpoint;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((checkpoint == null) ? 0 : checkpoint.hashCode());
            result = prime * result + ((path == null) ? 0 : path.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            } else if (object instanceof PathToCheck) {
                PathToCheck that = (PathToCheck) object;
                return path.equals(that.path) && checkpoint.equals(that.checkpoint);
            } else {
                return false;
            }
        }
    }

    private boolean traverse(Blob blob, boolean checkBinaries) throws IOException {
        if (checkBinaries && !isExternal(blob)) {
            InputStream s = blob.getNewStream();
            try {
                byte[] buffer = new byte[8192];
                int l = s.read(buffer, 0, buffer.length);
                while (l >= 0) {
                    l = s.read(buffer, 0, buffer.length);
                }
            } finally {
                s.close();
            }
            
            propertyCount++;
            return true;
        }
        
        return false;
    }

    private static boolean isExternal(Blob b) {
        if (b instanceof SegmentBlob) {
            return ((SegmentBlob) b).isExternal();
        }
        return false;
    }

    @Override
    public void close() {
        store.close();
    }

    private void print(String format) {
        outWriter.println(format);
    }

    private void print(String format, Object arg) {
        outWriter.println(MessageFormat.format(format, arg));
    }

    private void print(String format, Object arg1, Object arg2) {
        outWriter.println(MessageFormat.format(format, arg1, arg2));
    }
    
    private void print(String format, Object arg1, Object arg2, Object arg3, Object arg4) {
        outWriter.println(MessageFormat.format(format, arg1, arg2, arg3, arg4));
    }
    
    private void printError(String format, Object arg) {
        errWriter.println(MessageFormat.format(format, arg));
    }

    private void printError(String format, Object arg1, Object arg2) {
        errWriter.println(MessageFormat.format(format, arg1, arg2));
    }

    private long ts;

    private void debug(String format, Object arg) {
        if (debug()) {
            print(format, arg);
        }
    }

    private void debug(String format, Object arg1, Object arg2) {
        if (debug()) {
            print(format, arg1, arg2);
        }
    }

    private boolean debug() {
        // Avoid calling System.currentTimeMillis(), which is slow on some systems.
        if (debugInterval == Long.MAX_VALUE) {
            return false;
        } else if (debugInterval == 0) {
            return true;
        }

        long ts = System.currentTimeMillis();
        if ((ts - this.ts) / 1000 > debugInterval) {
            this.ts = ts;
            return true;
        } else {
            return false;
        }
    }
}
