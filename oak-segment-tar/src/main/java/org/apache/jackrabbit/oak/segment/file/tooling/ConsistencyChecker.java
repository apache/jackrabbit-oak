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

import static com.google.common.collect.Maps.newHashMap;
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
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.tar.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Utility for checking the files of a
 * {@link FileStore} for inconsistency and
 * reporting that latest consistent revision.
 */
public class ConsistencyChecker implements Closeable {

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

    /**
     * Run a full traversal consistency check.
     *
     * @param directory  directory containing the tar files
     * @param journalFileName  name of the journal file containing the revision history
     * @param debugInterval    number of seconds between printing progress information to
     *                         the console during the full traversal phase.
     * @param checkBinaries    if {@code true} full content of binary properties will be scanned
     * @param filterPaths      collection of repository paths to be checked                         
     * @param ioStatistics     if {@code true} prints I/O statistics gathered while consistency 
     *                         check was performed
     * @param outWriter        text output stream writer
     * @param errWriter        text error stream writer                        
     * @throws IOException
     */
    public static void checkConsistency(
            File directory,
            String journalFileName,
            long debugInterval,
            boolean checkBinaries,
            Set<String> filterPaths,
            boolean ioStatistics,
            PrintWriter outWriter,
            PrintWriter errWriter
    ) throws IOException, InvalidFileStoreVersionException {
        try (
                JournalReader journal = new JournalReader(new File(directory, journalFileName));
                ConsistencyChecker checker = new ConsistencyChecker(directory, debugInterval, ioStatistics, outWriter, errWriter)
        ) {
            Map<String, JournalEntry> pathToJournalEntry = newHashMap();
            Map<String, Set<String>> pathToCorruptPaths = newHashMap();
            for (String path : filterPaths) {
                pathToCorruptPaths.put(path, new HashSet<String>());
            }
            
            int count = 0;
            int revisionCount = 0;

            while (journal.hasNext() && count < filterPaths.size()) {
                JournalEntry journalEntry = journal.next();
                checker.print("Checking revision {0}", journalEntry.getRevision());
                
                try {
                    revisionCount++;
                    
                    for (String path : filterPaths) {
                        if (pathToJournalEntry.get(path) == null) {
                            
                            Set<String> corruptPaths = pathToCorruptPaths.get(path);
                            String corruptPath = checker.checkPathAtRevision(journalEntry.getRevision(), corruptPaths, path, checkBinaries);

                            if (corruptPath == null) {
                                checker.print("Path {0} is consistent", path);
                                pathToJournalEntry.put(path, journalEntry);
                                count++;
                            } else {
                                corruptPaths.add(corruptPath);
                            }
                        }
                    }
                } catch (IllegalArgumentException e) {
                    checker.printError("Skipping invalid record id {0}", journalEntry.getRevision());
                }
            }
            
            checker.print("Searched through {0} revisions", revisionCount);
            
            if (count == 0) {
                checker.print("No good revision found");
            } else {
                for (String path : filterPaths) {
                    JournalEntry journalEntry = pathToJournalEntry.get(path);
                    String revision = journalEntry != null ? journalEntry.getRevision() : null;
                    long timestamp = journalEntry != null ? journalEntry.getTimestamp() : -1L;
                    
                    checker.print("Latest good revision for path {0} is {1} from {2}", path,
                            toString(revision), toString(timestamp));
                }
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
     * Checks the consistency of the supplied {@code paths} at the given {@code revision}, 
     * starting first with already known {@code corruptPaths}.
     * 
     * @param revision      revision to be checked
     * @param corruptPaths  already known corrupt paths from previous revisions
     * @param path          path on which to run consistency check, 
     *                      provided there are no corrupt paths.
     * @param checkBinaries if {@code true} full content of binary properties will be scanned
     * @return              {@code null}, if the content tree rooted at path is consistent 
     *                      in this revision or the path of the first inconsistency otherwise.  
     */
    public String checkPathAtRevision(String revision, Set<String> corruptPaths, String path, boolean checkBinaries) {
        String result = null;
        
        store.setRevision(revision);
        NodeState root = SegmentNodeStoreBuilders.builder(store).build().getRoot();

        for (String corruptPath : corruptPaths) {
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

        print("Checking {0}", path);
        
        try {        
            NodeWrapper wrapper = NodeWrapper.deriveTraversableNodeOnPath(root, path);
            result = checkNodeAndDescendants(wrapper.node, wrapper.path, checkBinaries);
            print("Checked {0} nodes and {1} properties", nodeCount, propertyCount);
            
            return result;
        } catch (IllegalArgumentException e) {
            printError("Path {0} not found", path);
            return path;
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
    
    private void print(String format, Object arg1, Object arg2, Object arg3) {
        outWriter.println(MessageFormat.format(format, arg1, arg2, arg3));
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
