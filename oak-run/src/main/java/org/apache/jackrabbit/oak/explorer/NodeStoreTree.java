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
package org.apache.jackrabbit.oak.explorer;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static com.google.common.escape.Escapers.builder;
import static java.util.Collections.sort;
import static javax.jcr.PropertyType.BINARY;
import static javax.jcr.PropertyType.STRING;
import static javax.swing.tree.TreeSelectionModel.SINGLE_TREE_SELECTION;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.json.JsopBuilder.prettyPrint;
import static org.apache.jackrabbit.oak.json.JsopDiff.diffToJsop;

import java.awt.*;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.swing.*;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class NodeStoreTree extends JPanel implements TreeSelectionListener, Closeable {

    private static final long serialVersionUID = 1L;

    private final static int MAX_CHAR_DISPLAY = Integer.getInteger("max.char.display", 60);

    private static final String newline = "\n";

    private static void printGcRoots(StringBuilder sb, Map<UUID, Set<Entry<UUID, String>>> links, UUID uuid, String space, String inc) {
        Set<Entry<UUID, String>> roots = links.remove(uuid);

        if (roots == null || roots.isEmpty()) {
            return;
        }

        // TODO is sorting by file name needed?
        for (Entry<UUID, String> r : roots) {
            sb.append(space).append(r.getKey()).append("[").append(r.getValue()).append("]").append(newline);
            printGcRoots(sb, links, r.getKey(), space + inc, inc);
        }
    }

    private static void printPaths(List<String> paths, StringBuilder sb) {
        if (paths.isEmpty()) {
            return;
        }

        sb.append("Repository content references:").append(newline);

        for (String p : paths) {
            sb.append(p).append(newline);
        }
    }

    private static void printPropertyState(ExplorerBackend store, PropertyState state, String parentFile, StringBuilder b) {
        if (store.isPersisted(state)) {
            printRecordId(store.getRecordId(state), store.getFile(state), parentFile, b);
        } else {
            printSimpleClassName(state, b);
        }
    }

    private static void printNodeState(ExplorerBackend store, NodeState state, String parentTarFile, StringBuilder b) {
        if (store.isPersisted(state)) {
            printRecordId(store.getRecordId(state), store.getFile(state), parentTarFile, b);
        } else {
            printSimpleClassName(state, b);
        }
    }

    private static void printRecordId(String recordId, String file, String parentFile, StringBuilder l) {
        l.append(" (").append(recordId);

        if (file != null && !file.equals(parentFile)) {
            l.append(" in ").append(file);
        }

        l.append(")");
    }

    private static void printSimpleClassName(Object o, StringBuilder l) {
        l.append(" (").append(o.getClass().getSimpleName()).append(")");
    }

    private final ExplorerBackend backend;

    private Map<String, Set<UUID>> index;

    private DefaultTreeModel treeModel;

    private final JTree tree;

    private final JTextArea log;

    private Map<String, Long[]> sizeCache;

    private final boolean skipSizeCheck;

    NodeStoreTree(ExplorerBackend backend, JTextArea log, boolean skipSizeCheck)
            throws IOException {
        super(new GridLayout(1, 0));
        this.backend = backend;
        this.log = log;
        this.skipSizeCheck = skipSizeCheck;

        tree = new JTree();
        tree.getSelectionModel().setSelectionMode(SINGLE_TREE_SELECTION);
        tree.setShowsRootHandles(true);
        tree.addTreeSelectionListener(this);
        tree.setExpandsSelectedPaths(true);

        refreshStore();
        refreshModel();

        JScrollPane scrollPane = new JScrollPane(tree);
        add(scrollPane);
    }

    private void refreshStore() throws IOException {
        backend.open();
    }

    private void refreshModel() {
        index = backend.getTarReaderIndex();
        sizeCache = newHashMap();
        DefaultMutableTreeNode rootNode = new DefaultMutableTreeNode(
                new NamePathModel("/", "/", backend.getHead(), sizeCache,
                        skipSizeCheck, backend), true);
        treeModel = new DefaultTreeModel(rootNode);
        addChildren(rootNode);
        tree.setModel(treeModel);
    }

    void reopen() throws IOException {
        close();
        refreshStore();
        refreshModel();
    }

    @Override
    public void valueChanged(TreeSelectionEvent e) {
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree
                .getLastSelectedPathComponent();
        if (node == null) {
            return;
        }
        // load child nodes:
        try {
            addChildren(node);
            updateStats(node);
        } catch (IllegalStateException ex) {
            ex.printStackTrace();

            StringBuilder sb = new StringBuilder();
            sb.append(ex.getMessage());
            sb.append(newline);

            NamePathModel model = (NamePathModel) node.getUserObject();
            NodeState state = model.getState();
            String recordId = backend.getRecordId(state);
            if (recordId != null) {
                sb.append("Record ");
                sb.append(recordId);
                sb.append(newline);
            }
            setText(sb.toString());
        }
    }

    private void setText(String s) {
        log.setText(s);
        log.setCaretPosition(0);
    }

    private void addChildren(DefaultMutableTreeNode parent) {
        NamePathModel model = (NamePathModel) parent.getUserObject();
        if (model.isLoaded()) {
            return;
        }

        List<NamePathModel> kids = newArrayList();
        for (ChildNodeEntry ce : model.getState().getChildNodeEntries()) {
            NamePathModel c = new NamePathModel(ce.getName(), concat(
                    model.getPath(), ce.getName()), ce.getNodeState(),
                    sizeCache, skipSizeCheck, backend);
            kids.add(c);
        }
        sort(kids);
        for (NamePathModel c : kids) {
            DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(c,
                    true);
            treeModel.insertNodeInto(childNode, parent, parent.getChildCount());
        }
        model.loaded();
    }

    private void updateStats(DefaultMutableTreeNode parent) {
        NamePathModel model = (NamePathModel) parent.getUserObject();

        StringBuilder sb = new StringBuilder();
        sb.append(model.getPath());
        sb.append(newline);

        NodeState state = model.getState();
        String tarFile = "";

        if (backend.isPersisted(state)) {
            String recordId = backend.getRecordId(state);
            sb.append("Record ").append(recordId);
            tarFile = backend.getFile(state);
            if (tarFile != null) {
                sb.append(" in ").append(tarFile);
            }
            sb.append(newline);

            String templateId = backend.getTemplateRecordId(state);
            String f = backend.getTemplateFile(state);
            sb.append("TemplateId ");
            sb.append(templateId);
            if (f != null && !f.equals(tarFile)) {
                sb.append(" in ").append(f);
            }
            sb.append(newline);
        }

        sb.append("Size: ");
        sb.append("  direct: ");
        sb.append(byteCountToDisplaySize(model.getSize()[0]));
        sb.append(";  linked: ");
        sb.append(byteCountToDisplaySize(model.getSize()[1]));
        sb.append(newline);

        sb.append("Properties (count: ").append(state.getPropertyCount()).append(")");
        sb.append(newline);
        Map<String, String> propLines = newTreeMap();
        for (PropertyState ps : state.getProperties()) {
            StringBuilder l = new StringBuilder();
            l.append("  - ").append(ps.getName()).append(" = {").append(ps.getType()).append("} ");
            if (ps.getType().isArray()) {
                int count = ps.count();
                l.append("(count ").append(count).append(") [");

                String separator = ", ";
                int max = 50;
                if (ps.getType() == Type.BINARIES) {
                    separator = newline + "      ";
                    max = Integer.MAX_VALUE;
                    l.append(separator);
                }
                for (int i = 0; i < Math.min(count, max); i++) {
                    if (i > 0) {
                        l.append(separator);
                    }
                    l.append(toString(ps, i, tarFile));
                }
                if (count > max) {
                    l.append(", ... (").append(count).append(" values)");
                }
                if (ps.getType() == Type.BINARIES) {
                    l.append(separator);
                }
                l.append("]");

            } else {
                l.append(toString(ps, 0, tarFile));
            }
            printPropertyState(backend, ps, tarFile, l);
            propLines.put(ps.getName(), l.toString());
        }

        for (String l : propLines.values()) {
            sb.append(l);
            sb.append(newline);
        }

        sb.append("Child nodes (count: ").append(state.getChildNodeCount(Long.MAX_VALUE)).append(")");
        sb.append(newline);
        Map<String, String> childLines = newTreeMap();
        for (ChildNodeEntry ce : state.getChildNodeEntries()) {
            StringBuilder l = new StringBuilder();
            l.append("  + ").append(ce.getName());
            NodeState c = ce.getNodeState();
            printNodeState(backend, c, tarFile, l);
            childLines.put(ce.getName(), l.toString());
        }
        for (String l : childLines.values()) {
            sb.append(l);
            sb.append(newline);
        }

        if ("/".equals(model.getPath())) {
            sb.append("File Reader Index");
            sb.append(newline);

            for (String path : backend.getTarFiles()) {
                sb.append(path);
                sb.append(newline);
            }
            sb.append("----------");
        }

        setText(sb.toString());
    }

    private String toString(PropertyState ps, int index, String tarFile) {
        if (ps.getType().tag() == BINARY) {
            Blob b = ps.getValue(Type.BINARY, index);
            String info = "<";
            info += b.getClass().getSimpleName() + ";";
            info += "ref:" + safeGetReference(b) + ";";
            info += "id:" + b.getContentIdentity() + ";";
            info += safeGetLength(b) + ">";

            for (Entry<UUID, String> e : backend.getBulkSegmentIds(b).entrySet()) {
                info += newline + "        Bulk Segment Id " + e.getKey();
                if (e.getValue() != null && !e.getValue().equals(tarFile)) {
                    info += " in " + e.getValue();
                }
            }

            return info;
        } else if (ps.getType().tag() == STRING) {
            return displayString(ps.getValue(Type.STRING, index));
        } else {
            return ps.getValue(Type.STRING, index);
        }
    }

    private static String displayString(String value) {
        if (MAX_CHAR_DISPLAY > 0 && value.length() > MAX_CHAR_DISPLAY) {
            value = value.substring(0, MAX_CHAR_DISPLAY) + "... ("
                    + value.length() + " chars)";
        }
        String escaped = builder().setSafeRange(' ', '~')
                .addEscape('"', "\\\"").addEscape('\\', "\\\\").build()
                .escape(value);
        return '"' + escaped + '"';
    }

    private String safeGetReference(Blob b) {
        try {
            return b.getReference();
        } catch (IllegalStateException e) {
            // missing BlobStore probably
        }
        return "[BlobStore not available]";
    }

    private String safeGetLength(Blob b) {
        try {
            return byteCountToDisplaySize(b.length());
        } catch (IllegalStateException e) {
            // missing BlobStore probably
        }
        return "[BlobStore not available]";
    }

    void printTarInfo(String file) {
        if (file == null || file.length() == 0) {
            return;
        }
        StringBuilder sb = new StringBuilder();

        Set<UUID> uuids = newHashSet();
        for (Entry<String, Set<UUID>> e : index.entrySet()) {
            if (e.getKey().endsWith(file)) {
                sb.append("SegmentNodeState references to ").append(e.getKey());
                sb.append(newline);
                uuids = e.getValue();
                break;
            }
        }

        Set<UUID> inMem = intersection(backend.getReferencedSegmentIds(), uuids);
        if (!inMem.isEmpty()) {
            sb.append("In Memory segment references: ");
            sb.append(newline);
            sb.append(inMem);
            sb.append(newline);
        }

        List<String> paths = newArrayList();
        filterNodeStates(uuids, paths, backend.getHead(), "/", backend);
        printPaths(paths, sb);

        sb.append(newline);
        try {
            Map<UUID, List<UUID>> graph = backend.getTarGraph(file);
            sb.append("Tar graph:").append(newline);
            for (Entry<UUID, List<UUID>> entry : graph.entrySet()) {
                sb.append(entry.getKey()).append('=').append(entry.getValue())
                        .append(newline);
            }
            sb.append(newline);
        } catch (IOException e) {
            sb.append("Error getting tar graph:").append(e).append(newline);
        }

        setText(sb.toString());
    }

    void printSegmentReferences(String sid) {
        if (sid == null || sid.length() == 0) {
            return;
        }
        UUID id = null;
        try {
            id = UUID.fromString(sid.trim());
        } catch (IllegalArgumentException e) {
            setText(e.getMessage());
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("References to segment ").append(id);
        sb.append(newline);
        for (Entry<String, Set<UUID>> e : index.entrySet()) {
            if (e.getValue().contains(id)) {
                sb.append("Tar file: ").append(e.getKey());
                sb.append(newline);
                break;
            }
        }

        List<String> paths = newArrayList();
        filterNodeStates(newHashSet(id), paths, backend.getHead(), "/", backend);
        printPaths(paths, sb);

        Map<UUID, Set<Entry<UUID, String>>> links = newHashMap();
        try {
            backend.getGcRoots(id, links);
        } catch (IOException e) {
            sb.append(newline);
            sb.append(e.getMessage());
        }
        if (!links.isEmpty()) {
            sb.append("Segment GC roots:");
            sb.append(newline);
            printGcRoots(sb, links, id, "  ", "  ");
        }

        setText(sb.toString());
    }

    private static void filterNodeStates(Set<UUID> uuids, List<String> paths, NodeState state, String path, ExplorerBackend store) {
        Set<String> localPaths = newTreeSet();
        for (PropertyState ps : state.getProperties()) {
            if (store.isPersisted(ps)) {
                String recordId = store.getRecordId(ps);
                UUID id = store.getSegmentId(ps);
                if (uuids.contains(id)) {
                    if (ps.getType().tag() == STRING) {
                        String val = "";
                        if (ps.count() > 0) {
                            // only shows the first value, do we need more?
                            val = displayString(ps.getValue(Type.STRING, 0));
                        }
                        localPaths.add(path + ps.getName() + " = " + val
                                + " [SegmentPropertyState<" + ps.getType()
                                + ">@" + recordId + "]");
                    } else {
                        localPaths.add(path + ps + " [SegmentPropertyState<"
                                + ps.getType() + ">@" + recordId + "]");
                    }

                }
                if (ps.getType().tag() == BINARY) {
                    // look for extra segment references
                    for (int i = 0; i < ps.count(); i++) {
                        Blob b = ps.getValue(Type.BINARY, i);
                        for (Entry<UUID, String> e : store.getBulkSegmentIds(b).entrySet()) {
                            if (!e.getKey().equals(id) && uuids.contains(e.getKey())) {
                                localPaths.add(path + ps
                                        + " [SegmentPropertyState<"
                                        + ps.getType() + ">@" + recordId + "]");
                            }
                        }
                    }
                }
            }
        }

        String stateId = store.getRecordId(state);
        if (uuids.contains(store.getSegmentId(state))) {
            localPaths.add(path + " [SegmentNodeState@" + stateId + "]");
        }

        String templateId = store.getTemplateRecordId(state);
        if (uuids.contains(store.getTemplateSegmentId(state))) {
            localPaths.add(path + "[Template@" + templateId + "]");
        }
        paths.addAll(localPaths);
        for (ChildNodeEntry ce : state.getChildNodeEntries()) {
            filterNodeStates(uuids, paths, ce.getNodeState(), path + ce.getName() + "/", store);
        }
    }

    void printDiff(String input) {
        StringBuilder sb = new StringBuilder();
        if (input == null || input.trim().length() == 0) {
            setText("Usage <recordId> <recordId> [<path>]");
            return;
        }

        String[] tokens = input.trim().split(" ");
        if (tokens.length != 2 && tokens.length != 3) {
            setText("Usage <recordId> <recordId> [<path>]");
            return;
        }
        NodeState node1;
        NodeState node2;
        try {
            node1 = backend.readNodeState(tokens[0]);
            node2 = backend.readNodeState(tokens[1]);
        } catch (IllegalArgumentException ex) {
            sb.append("Unknown argument: ");
            sb.append(input);
            sb.append(newline);
            sb.append("Error: ");
            sb.append(ex.getMessage());
            sb.append(newline);
            setText(sb.toString());
            return;
        }
        String path = "/";
        if (tokens.length == 3) {
            path = tokens[2];
        }

        for (String name : elements(path)) {
            node1 = node1.getChildNode(name);
            node2 = node2.getChildNode(name);
        }

        sb.append("SegmentNodeState diff ");
        sb.append(tokens[0]);
        sb.append(" vs ");
        sb.append(tokens[1]);
        sb.append(" at ");
        sb.append(path);
        sb.append(newline);
        sb.append("--------");
        sb.append(newline);
        sb.append(prettyPrint(diffToJsop(node1, node2)));
        setText(sb.toString());
    }

    boolean revert(String revision) {
        return safeRevert(revision, false);
    }

    private boolean safeRevert(String revision, boolean rollback) {
        String head = backend.getRecordId(backend.getHead());
        backend.setRevision(revision);
        try {
            refreshModel();
            if (!rollback) {
                setText("Switched head revision to " + revision);
            }
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unable to switch head revision to ");
            sb.append(revision);
            sb.append(newline);
            sb.append("    ");
            sb.append(e.getMessage());
            sb.append(newline);
            sb.append("Will rollback to ");
            sb.append(head);
            setText(sb.toString());
            return safeRevert(head, true);
        }
        return !rollback;
    }

    void printPCMInfo() {
        setText(backend.getPersistedCompactionMapStats());
    }

    private static class NamePathModel implements Comparable<NamePathModel> {

        private final ExplorerBackend backend;
        private final String name;
        private final String path;
        private final boolean skipSizeCheck;

        private boolean loaded = false;

        private Long[] size = {-1L, -1L};

        NamePathModel(String name, String path, NodeState state, Map<String, Long[]> sizeCache, boolean skipSizeCheck, ExplorerBackend backend) {
            this.backend = backend;
            this.name = name;
            this.path = path;
            this.skipSizeCheck = skipSizeCheck;
            if (!skipSizeCheck && backend.isPersisted(state)) {
                this.size = exploreSize(state, sizeCache, backend);
            }
        }

        void loaded() {
            loaded = true;
        }

        boolean isLoaded() {
            return loaded;
        }

        @Override
        public String toString() {
            if (skipSizeCheck) {
                return name;
            }
            if (size[1] > 0) {
                return name + " (" + byteCountToDisplaySize(size[0]) + ";"
                        + byteCountToDisplaySize(size[1]) + ")";
            }
            if (size[0] > 0) {
                return name + " (" + byteCountToDisplaySize(size[0]) + ")";
            }
            return name;
        }

        public String getPath() {
            return path;
        }

        public NodeState getState() {
            return loadState();
        }

        private NodeState loadState() {
            NodeState n = backend.getHead();
            for (String p : elements(path)) {
                n = n.getChildNode(p);
            }
            return n;
        }

        @Override
        public int compareTo(NamePathModel o) {
            int s = size[0].compareTo(o.size[0]);
            if (s != 0) {
                return -1 * s;
            }
            s = size[1].compareTo(o.size[1]);
            if (s != 0) {
                return -1 * s;
            }
            if ("root".equals(name)) {
                return 1;
            } else if ("root".equals(o.name)) {
                return -1;
            }
            return name.compareTo(o.name);
        }

        public Long[] getSize() {
            return size;
        }
    }

    private static Long[] exploreSize(NodeState ns, Map<String, Long[]> sizeCache, ExplorerBackend store) {
        String key = store.getRecordId(ns);
        if (sizeCache.containsKey(key)) {
            return sizeCache.get(key);
        }
        Long[] s = {0L, 0L};

        List<String> names = newArrayList(ns.getChildNodeNames());

        if (names.contains("root")) {
            List<String> temp = newArrayList();
            int poz = 0;
            // push 'root' to the beginning
            for (String n : names) {
                if (n.equals("root")) {
                    temp.add(poz, n);
                    poz++;
                } else {
                    temp.add(n);
                }
            }
            names = temp;
        }

        for (String n : names) {
            NodeState k = ns.getChildNode(n);
            String ckey = store.getRecordId(k);
            if (sizeCache.containsKey(ckey)) {
                // already been here, record size under 'link'
                Long[] ks = sizeCache.get(ckey);
                s[1] = s[1] + ks[0] + ks[1];
            } else {
                Long[] ks = exploreSize(k, sizeCache, store);
                s[0] = s[0] + ks[0];
                s[1] = s[1] + ks[1];
            }
        }
        for (PropertyState ps : ns.getProperties()) {
            for (int j = 0; j < ps.count(); j++) {
                if (ps.getType().tag() == Type.BINARY.tag()) {
                    Blob b = ps.getValue(Type.BINARY, j);
                    boolean skip = store.isExternal(b);
                    if (!skip) {
                        s[0] = s[0] + b.length();
                    }
                } else {
                    s[0] = s[0] + ps.size(j);
                }
            }
        }
        sizeCache.put(key, s);
        return s;
    }

    @Override
    public void close() throws IOException {
        backend.close();
    }

}
