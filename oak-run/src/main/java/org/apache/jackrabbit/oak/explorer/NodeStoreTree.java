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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.swing.*;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.awt.*;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.List;

import static java.util.Collections.sort;
import static javax.swing.tree.TreeSelectionModel.SINGLE_TREE_SELECTION;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

class NodeStoreTree implements TreeSelectionListener, Closeable {

    private final JPanel panel = new JPanel(new GridLayout(1, 0));

    private final ExplorerBackend backend;

    private Map<String, Set<UUID>> index;

    private DefaultTreeModel treeModel;

    private final JTree tree;

    private final LogPanel logPanel;

    private Map<String, Long[]> sizeCache;

    private final boolean skipSizeCheck;

    NodeStoreTree(ExplorerBackend backend, LogPanel logPanel, boolean skipSizeCheck)
            throws IOException {
        this.backend = backend;
        this.logPanel = logPanel;
        this.skipSizeCheck = skipSizeCheck;

        tree = new JTree();
        tree.getSelectionModel().setSelectionMode(SINGLE_TREE_SELECTION);
        tree.setShowsRootHandles(true);
        tree.addTreeSelectionListener(this);
        tree.setExpandsSelectedPaths(true);

        refreshStore();
        refreshModel();

        panel.add(new JScrollPane(tree));
    }

    private void refreshStore() throws IOException {
        backend.open();
    }

    private void refreshModel() {
        index = backend.getTarReaderIndex();
        sizeCache = new HashMap<>();
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
            sb.append(ReportUtils.newline);

            NamePathModel model = (NamePathModel) node.getUserObject();
            NodeState state = model.getState();
            String recordId = backend.getRecordId(state);
            if (recordId != null) {
                sb.append("Record ");
                sb.append(recordId);
                sb.append(ReportUtils.newline);
            }
            setPlainText(sb.toString());
        }
    }

    private void setPlainText(String text) {
        logPanel.setPlainContent(text);
    }

    private void setHtmlText(String s) {
        logPanel.setHtmlContent(s);
    }

    private void addChildren(DefaultMutableTreeNode parent) {
        NamePathModel model = (NamePathModel) parent.getUserObject();
        if (model.isLoaded()) {
            return;
        }

        List<NamePathModel> kids = new ArrayList<>();
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
        setHtmlText(NodeReport.createHtmlTextReport(model, backend));
    }

    void printTarInfo(String file) {
        if (file == null || file.length() == 0) {
            return;
        }
        setPlainText(TarReport.createPlainTextReport(file, backend, index));
    }

    void printSegmentReferences(String sid) {
        if (sid == null || sid.length() == 0) {
            return;
        }
        setPlainText(ReferenceReport.createPlainReport(sid, backend, index));
    }

    void printDiff(String input) {
        setPlainText(DiffReport.createPlainTextReport(input, backend));
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
                setHtmlText("Switched head revision to " + revision);
            }
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unable to switch head revision to ");
            sb.append(revision);
            sb.append(ReportUtils.newline);
            sb.append("    ");
            sb.append(e.getMessage());
            sb.append(ReportUtils.newline);
            sb.append("Will rollback to ");
            sb.append(head);
            setPlainText(sb.toString());
            return safeRevert(head, true);
        }
        return !rollback;
    }

    void printPCMInfo() {
        setPlainText(backend.getPersistedCompactionMapStats());
    }

    public JComponent getComponent() {
        return panel;
    }

    public static class NamePathModel implements Comparable<NamePathModel> {

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

        List<String> names = CollectionUtils.toList(ns.getChildNodeNames());

        if (names.contains("root")) {
            List<String> temp = new ArrayList<>();
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
