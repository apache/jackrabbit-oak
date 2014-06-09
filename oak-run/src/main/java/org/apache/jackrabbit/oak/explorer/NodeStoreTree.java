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

import java.awt.GridLayout;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.jcr.PropertyType;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeSelectionModel;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.kernel.JsopDiff;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Lists;
import com.google.common.escape.Escapers;

class NodeStoreTree extends JPanel implements TreeSelectionListener {

    private final FileStore store;

    private final DefaultTreeModel treeModel;
    private final JTree tree;
    private final JTextArea log;

    private final Map<String, Set<UUID>> index;
    private final Map<RecordId, Long[]> sizeCache;

    public NodeStoreTree(FileStore store, JTextArea log) {
        super(new GridLayout(1, 0));
        this.store = store;
        this.log = log;

        index = store.getTarReaderIndex();
        sizeCache = new HashMap<RecordId, Long[]>();

        DefaultMutableTreeNode rootNode = new DefaultMutableTreeNode(
                new NamePathModel("/", "/", store.getHead(), sizeCache), true);
        treeModel = new DefaultTreeModel(rootNode);
        addChildren(rootNode);

        tree = new JTree(treeModel);
        tree.getSelectionModel().setSelectionMode(
                TreeSelectionModel.SINGLE_TREE_SELECTION);
        tree.setShowsRootHandles(true);
        tree.addTreeSelectionListener(this);
        tree.setExpandsSelectedPaths(true);

        JScrollPane scrollPane = new JScrollPane(tree);
        add(scrollPane);
    }

    @Override
    public void valueChanged(TreeSelectionEvent e) {
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree
                .getLastSelectedPathComponent();
        if (node == null) {
            return;
        }
        // load child nodes:
        addChildren(node);
        updateStats(node);
    }

    private void addChildren(DefaultMutableTreeNode parent) {
        NamePathModel model = (NamePathModel) parent.getUserObject();
        if (model.isLoaded()) {
            return;
        }

        List<NamePathModel> kids = new ArrayList<NamePathModel>();
        for (ChildNodeEntry ce : model.getState().getChildNodeEntries()) {
            NamePathModel c = new NamePathModel(ce.getName(), PathUtils.concat(
                    model.getPath(), ce.getName()), ce.getNodeState(),
                    sizeCache);
            kids.add(c);
        }
        Collections.sort(kids);
        for (NamePathModel c : kids) {
            DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(c,
                    true);
            treeModel.insertNodeInto(childNode, parent, parent.getChildCount());
        }
        model.loaded();
    }

    private final static String newline = "\n";

    private void updateStats(DefaultMutableTreeNode parent) {
        NamePathModel model = (NamePathModel) parent.getUserObject();

        StringBuilder sb = new StringBuilder();
        sb.append(model.getPath());
        sb.append(newline);

        NodeState state = model.getState();

        if (state instanceof SegmentNodeState) {
            SegmentNodeState s = (SegmentNodeState) state;

            RecordId recordId = s.getRecordId();
            sb.append("Record " + recordId);
            String file = getFile(recordId);
            if (file.length() > 0) {
                sb.append(" in " + file);
            }
            sb.append(newline);

            sb.append("Size: ");
            sb.append("  direct: ");
            sb.append(FileUtils.byteCountToDisplaySize(model.getSize()[0]));
            sb.append(";  linked: ");
            sb.append(FileUtils.byteCountToDisplaySize(model.getSize()[1]));
            sb.append(newline);

            sb.append("Properties (count: " + s.getPropertyCount() + ")");
            sb.append(newline);
            for (PropertyState ps : s.getProperties()) {
                sb.append("  - " + ps.getName() + " = ");
                if (ps.getType().isArray()) {
                    sb.append("[");
                    int count = ps.count();
                    for (int i = 0; i < Math.min(count, 10); i++) {
                        if (i > 0) {
                            sb.append(",");
                        }
                        sb.append(" " + ps.getValue(Type.STRING, i));
                    }
                    if (count > 10) {
                        sb.append(", ... (" + count + " values)");
                    }
                    sb.append(" ]");
                } else {
                    sb.append(toString(ps, 0));
                }
                if (ps instanceof SegmentPropertyState) {
                    RecordId rid = ((SegmentPropertyState) ps).getRecordId();
                    sb.append(" (" + rid);
                    String f = getFile(rid);
                    if (!f.equals(file)) {
                        sb.append(" in " + f);
                    }
                    sb.append(")");
                } else {
                    sb.append(" (" + ps.getClass().getSimpleName() + ")");
                }
                sb.append(newline);
            }

            sb.append("Child nodes (count: "
                    + s.getChildNodeCount(Long.MAX_VALUE) + ")");
            sb.append(newline);
            for (ChildNodeEntry ce : s.getChildNodeEntries()) {
                sb.append("  + " + ce.getName());
                NodeState c = ce.getNodeState();
                if (c instanceof SegmentNodeState) {
                    RecordId rid = ((SegmentNodeState) c).getRecordId();
                    sb.append(" (" + rid);
                    String f = getFile(rid);
                    if (!f.equals(file)) {
                        sb.append(" in " + f);
                    }
                    sb.append(")");
                } else {
                    sb.append(" (" + c.getClass().getSimpleName() + ")");
                }
                sb.append(newline);
            }
        }

        if ("/".equals(model.getPath())) {
            sb.append("File Index");
            sb.append(newline);

            List<String> files = new ArrayList<String>(store
                    .getTarReaderIndex().keySet());
            Collections.sort(files);

            for (String path : files) {
                sb.append(path);
                sb.append(newline);
            }
            sb.append("----------");
        }

        log.setText(sb.toString());
    }

    private String toString(PropertyState ps, int index) {
        if (ps.getType().tag() == PropertyType.BINARY) {
            return "<"
                    + FileUtils.byteCountToDisplaySize(ps.getValue(Type.BINARY,
                            index).length()) + " >";
        } else if (ps.getType().tag() == PropertyType.STRING) {
            String value = ps.getValue(Type.STRING, index);
            if (value.length() > 60) {
                value = value.substring(0, 57) + "... (" + value.length()
                        + " chars)";
            }
            String escaped = Escapers.builder().setSafeRange(' ', '~')
                    .addEscape('"', "\\\"").addEscape('\\', "\\\\").build()
                    .escape(value);
            return '"' + escaped + '"';
        } else {
            return ps.getValue(Type.STRING, index);
        }
    }

    private String getFile(RecordId id) {
        SegmentId segmentId = id.getSegmentId();
        for (Entry<String, Set<UUID>> path2Uuid : index.entrySet()) {
            for (UUID uuid : path2Uuid.getValue()) {
                if (uuid.getMostSignificantBits() == segmentId
                        .getMostSignificantBits()
                        && uuid.getLeastSignificantBits() == segmentId
                                .getLeastSignificantBits()) {
                    return new File(path2Uuid.getKey()).getName();
                }
            }
        }
        return "";
    }

    public void printDependenciesToFile(String file) {
        if (file == null || file.length() == 0) {
            return;
        }
        StringBuilder sb = new StringBuilder();

        Set<UUID> uuids = new HashSet<UUID>();
        for (Entry<String, Set<UUID>> e : store.getTarReaderIndex().entrySet()) {
            if (e.getKey().endsWith(file)) {
                sb.append("SegmentNodeState references to " + e.getKey());
                sb.append(newline);
                uuids = e.getValue();
            }
        }
        List<String> paths = new ArrayList<String>();
        filterNodeStates(uuids, paths, store.getHead(), "/");
        for (String p : paths) {
            sb.append("    ");
            sb.append(p);
            sb.append(newline);
        }

        log.setText(sb.toString());
    }

    private static void filterNodeStates(Set<UUID> uuids, List<String> paths,
            SegmentNodeState state, String path) {
        for (PropertyState ps : state.getProperties()) {
            if (ps instanceof SegmentPropertyState) {
                SegmentPropertyState sps = (SegmentPropertyState) ps;
                SegmentId id = sps.getRecordId().getSegmentId();
                if (contains(id, uuids)) {
                    paths.add(path + "@" + ps);
                }
            }
        }
        for (ChildNodeEntry ce : state.getChildNodeEntries()) {
            NodeState c = ce.getNodeState();
            if (c instanceof SegmentNodeState) {
                filterNodeStates(uuids, paths, (SegmentNodeState) c,
                        path + ce.getName() + "/");
            }
        }
    }

    private static boolean contains(SegmentId id, Set<UUID> uuids) {
        for (UUID u : uuids) {
            if (id.getMostSignificantBits() == u.getMostSignificantBits()
                    && id.getLeastSignificantBits() == u
                            .getLeastSignificantBits()) {
                return true;
            }
        }
        return false;
    }

    public void printDiff(String input) {
        StringBuilder sb = new StringBuilder();
        if (input == null || input.trim().length() == 0) {
            sb.append("Unknown argument: ");
            sb.append(input);
            sb.append(newline);
            log.setText("Usage <recordId> <recordId> [<path>]");
            return;
        }

        String[] tokens = input.trim().split(" ");
        if (tokens.length != 2 && tokens.length != 3) {
            sb.append("Unknown argument: ");
            sb.append(input);
            sb.append(newline);
            log.setText("Usage <recordId> <recordId> [<path>]");
            return;
        }
        RecordId id1 = null;
        RecordId id2 = null;
        try {
            id1 = RecordId.fromString(store.getTracker(), tokens[0]);
            id2 = RecordId.fromString(store.getTracker(), tokens[1]);
        } catch (IllegalArgumentException ex) {
            sb.append("Unknown argument: ");
            sb.append(input);
            sb.append(newline);
            sb.append("Error: ");
            sb.append(ex.getMessage());
            sb.append(newline);
            log.setText(sb.toString());
            return;
        }
        String path = "/";
        if (tokens.length == 3) {
            path = tokens[2];
        }

        NodeState node1 = new SegmentNodeState(id1);
        NodeState node2 = new SegmentNodeState(id2);
        for (String name : PathUtils.elements(path)) {
            node1 = node1.getChildNode(name);
            node2 = node2.getChildNode(name);
        }

        sb.append("SegmentNodeState diff ");
        sb.append(id1);
        sb.append(" vs ");
        sb.append(id2);
        sb.append(" at ");
        sb.append(path);
        sb.append(newline);
        sb.append("--------");
        sb.append(newline);
        sb.append(JsopBuilder.prettyPrint(JsopDiff.diffToJsop(node1, node2)));
        log.setText(sb.toString());
    }

    public void compact() {
        StringBuilder sb = new StringBuilder();

        long s = System.currentTimeMillis();
        store.gc();
        store.compact();
        try {
            store.flush();
        } catch (IOException e) {
            sb.append("IOException " + e.getMessage());
            e.printStackTrace();
        }
        s = System.currentTimeMillis() - s;

        sb.append("Compacted tar segments in " + s + " ms.");
        sb.append(newline);

        sb.append("File Index");
        sb.append(newline);

        List<String> files = new ArrayList<String>(store.getTarReaderIndex()
                .keySet());
        Collections.sort(files);

        for (String path : files) {
            sb.append(path);
            sb.append(newline);
        }
        sb.append("----------");
        log.setText(sb.toString());
    }

    private static class NamePathModel implements Comparable<NamePathModel> {

        private final String name;
        private final String path;
        private final NodeState state;

        private boolean loaded = false;

        private Long[] size = { -1l, -1l };

        public NamePathModel(String name, String path, NodeState state,
                Map<RecordId, Long[]> sizeCache) {
            this.name = name;
            this.path = path;
            this.state = state;
            if (state instanceof SegmentNodeState) {
                this.size = exploreSize((SegmentNodeState) state, sizeCache);
            }
        }

        public void loaded() {
            loaded = true;
        }

        public boolean isLoaded() {
            return loaded;
        }

        @Override
        public String toString() {
            if (size[1] > 0) {
                return name + " (" + FileUtils.byteCountToDisplaySize(size[0])
                        + ";" + FileUtils.byteCountToDisplaySize(size[1]) + ")";
            }
            if (size[0] > 0) {
                return name + " (" + FileUtils.byteCountToDisplaySize(size[0])
                        + ")";
            }
            return name;
        }

        public String getPath() {
            return path;
        }

        public NodeState getState() {
            return state;
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

    private static Long[] exploreSize(SegmentNodeState ns,
            Map<RecordId, Long[]> sizeCache) {
        if (sizeCache.containsKey(ns.getRecordId())) {
            return sizeCache.get(ns.getRecordId());
        }
        Long[] s = { 0l, 0l };

        List<String> names = Lists.newArrayList(ns.getChildNodeNames());

        if (names.contains("root")) {
            List<String> temp = Lists.newArrayList();
            int poz = 0;
            // push 'root' to the beginning
            Iterator<String> iterator = names.iterator();
            while (iterator.hasNext()) {
                String n = iterator.next();
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
            SegmentNodeState k = (SegmentNodeState) ns.getChildNode(n);
            if (sizeCache.containsKey(k.getRecordId())) {
                // already been here, record size under 'link'
                Long[] ks = sizeCache.get(k.getRecordId());
                s[1] = s[1] + ks[0] + ks[1];
            } else {
                Long[] ks = exploreSize(k, sizeCache);
                s[0] = s[0] + ks[0];
                s[1] = s[1] + ks[1];
            }
        }
        for (PropertyState ps : ns.getProperties()) {
            for (int j = 0; j < ps.count(); j++) {
                s[0] = s[0] + ps.size(j);
            }
        }
        sizeCache.put(ns.getRecordId(), s);
        return s;
    }
}
