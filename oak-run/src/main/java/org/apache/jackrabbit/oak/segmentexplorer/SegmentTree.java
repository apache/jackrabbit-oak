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
package org.apache.jackrabbit.oak.segmentexplorer;

import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

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
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Lists;

public class SegmentTree extends JPanel implements TreeSelectionListener {

    private final FileStore store;

    private final DefaultTreeModel treeModel;
    private final JTree tree;
    private final JTextArea log;

    private final Map<String, Set<UUID>> index;
    private final Map<RecordId, Long[]> sizeCache;

    public SegmentTree(FileStore store, JTextArea log) {
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
            sb.append("Size: ");
            sb.append("  direct: ");
            sb.append(FileUtils.byteCountToDisplaySize(model.getSize()[0]));
            sb.append(";  linked: ");
            sb.append(FileUtils.byteCountToDisplaySize(model.getSize()[1]));
            sb.append(newline);

            SegmentNodeState s = (SegmentNodeState) state;
            Segment segment = s.getRecordId().getSegment();
            sb.append("Segment ");
            sb.append(segment.getSegmentId());
            sb.append(newline);

            sb.append("File ");
            String file = getFile(segment.getSegmentId().toString());
            sb.append(file);
            sb.append(newline);

            sb.append("State");
            sb.append(newline);
            sb.append("  Child Node Count: " + s.getChildNodeCount(-1));
            sb.append(newline);
            for (ChildNodeEntry ce : s.getChildNodeEntries()) {
                SegmentNodeState c = (SegmentNodeState) ce.getNodeState();
                sb.append("    " + c.getRecordId().getSegmentId() + " -> "
                        + ce.getName());
                sb.append(newline);

                String f = getFile(c.getRecordId().getSegmentId().toString());
                if (!f.equals(file)) {
                    sb.append("      * " + f);
                    sb.append(newline);
                }
            }
            sb.append("  Property Count: " + s.getPropertyCount());
            sb.append(newline);
            for (PropertyState ps : s.getProperties()) {
                if (ps instanceof SegmentPropertyState) {
                    String val = "";
                    if (ps.getType() == Type.STRING) {
                        val = ps.getValue(Type.STRING);
                    } else if (ps.getType() == Type.NAME) {
                        val = ps.getValue(Type.NAME);
                    } else {
                        val = ps.toString();
                    }
                    // TODO handle the display for the rest of the property
                    // types
                    String sId = ((SegmentPropertyState) ps).getRecordId()
                            .getSegmentId().toString();
                    sb.append("    " + sId + " -> " + ps.getName() + " (" + val
                            + ")");
                    sb.append(newline);
                    String f = getFile(sId);
                    if (!f.equals(file)) {
                        sb.append("      * " + f);
                        sb.append(newline);
                    }

                } else {
                    String val = "";
                    if (ps.getType() == Type.STRING) {
                        val = ps.getValue(Type.STRING);
                    }
                    if (ps.getType() == Type.NAME) {
                        val = ps.getValue(Type.NAME);
                    }
                    sb.append("    " + ps.getName() + " ("
                            + ps.getClass().getSimpleName() + ")" + " (" + val
                            + ")");
                    sb.append(newline);
                }
            }
        }

        if ("/".equals(model.getPath())) {
            sb.append("File Index");
            sb.append(newline);
            for (Entry<String, Set<UUID>> path2Uuid : index.entrySet()) {
                sb.append(path2Uuid.getKey());
                sb.append(newline);
                // sb.append(path2Uuid.getValue());
                // sb.append(newline);
            }
            sb.append("----------");
        }

        log.setText(sb.toString());
    }

    private String getFile(String sId) {
        for (Entry<String, Set<UUID>> path2Uuid : index.entrySet()) {
            for (UUID uuid : path2Uuid.getValue()) {
                if (uuid.toString().equals(sId)) {
                    return path2Uuid.getKey();
                }
            }
        }
        return "";
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
