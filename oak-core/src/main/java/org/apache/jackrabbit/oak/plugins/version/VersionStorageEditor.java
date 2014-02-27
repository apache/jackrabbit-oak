/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.version;

import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONLABELS;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.REP_ADD_VERSION_LABELS;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.REP_REMOVE_VERSION;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.REP_REMOVE_VERSION_LABELS;

/**
 * Implements an editor watching for well known properties on the
 * /jcr:system/jcr:versionStorage node to trigger version operations (like
 * adding/removing labels or removing versions) on the protected version storage
 * tree.
 * <p>
 * This editor supports the following operations:
 * <ul>
 * <li>{@link VersionConstants#REP_ADD_VERSION_LABELS}: adds version labels to
 * existing version histories. The property is multi-valued and each value is a
 * PATH, which looks like this:
 * {@code &lt;version-history-path>/jcr:versionLabels/&lt;version-label>/&lt;version-name>}.
 * The {@code version-history-path} is a relative path to the version
 * history node starting at the /jcr:system/jcr:versionStorage node.
 * An attempt to add a version label that already exists will result in a
 * {@link CommitFailedException}. </li>
 * <li>{@link VersionConstants#REP_REMOVE_VERSION_LABELS}: removes version labels from
 * existing version histories. The property is multi-valued and each value is a
 * PATH, which looks like this:
 * {@code &lt;version-history-path>/jcr:versionLabels/&lt;version-label>/&lt;version-name>}.
 * The {@code version-history-path} is a relative path to the version
 * history node starting at the /jcr:system/jcr:versionStorage node. The
 * {@code &lt;version-name>} part is ignored when labels are removed and
 * can be anything, though it must be a valid JCR/Oak name.
 * An attempt to remove a version label, which does not exist, will result in a
 * {@link CommitFailedException}. </li>
 * <li>{@link VersionConstants#REP_REMOVE_VERSION}: : removes a version from
 * existing version histories, the associated labels and fixes the version tree.
 * The property is multi-valued and each value is a
 * PATH, which looks like this:
 * {@code &lt;version-history-path>/&lt;version-name>}.
 * The {@code version-history-path} is a relative path to the version
 * history node starting at the /jcr:system/jcr:versionStorage node.</li>
 * </ul>
 */
class VersionStorageEditor extends DefaultEditor {

    private final NodeBuilder versionStorageNode;
    private final NodeBuilder workspaceRoot;
    private ReadWriteVersionManager vMgr;
    private final List<String> pathRemainder;

    private final SortedMap<Integer, Operation> operations = Maps.newTreeMap();

    VersionStorageEditor(@Nonnull NodeBuilder versionStorageNode,
                         @Nonnull NodeBuilder workspaceRoot) {
        this(versionStorageNode, workspaceRoot,
                Arrays.asList(JCR_SYSTEM, JCR_VERSIONSTORAGE));
    }

    private VersionStorageEditor(@Nonnull NodeBuilder versionStorageNode,
                         @Nonnull NodeBuilder workspaceRoot,
                         @Nonnull List<String> pathRemainder) {
        this.versionStorageNode = versionStorageNode;
        this.workspaceRoot = workspaceRoot;
        this.pathRemainder = checkNotNull(pathRemainder);
    }

    @Override
    public Editor childNodeChanged(String name,
                                   NodeState before,
                                   NodeState after)
            throws CommitFailedException {
        if (pathRemainder.isEmpty()) {
            return null;
        }
        if (pathRemainder.get(0).equals(name)) {
            return new VersionStorageEditor(versionStorageNode, workspaceRoot,
                    pathRemainder.subList(1, pathRemainder.size()));
        } else {
            return null;
        }
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        String name = after.getName();
        if (REP_REMOVE_VERSION_LABELS.equals(name)) {
            operations.put(1, new RemoveVersionLabels(after.getValue(Type.PATHS)));
            versionStorageNode.removeProperty(name);
        } else if (REP_ADD_VERSION_LABELS.equals(name)) {
            operations.put(2, new AddVersionLabels(after.getValue(Type.PATHS)));
            versionStorageNode.removeProperty(name);
        } else if (REP_REMOVE_VERSION.equals(name)) {
            operations.put(3, new RemoveVersion(after.getValue(Type.PATHS)));
            versionStorageNode.removeProperty(name);
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        for (Operation op : operations.values()) {
            op.perform();
        }
    }

    //-------------------------< internal >-------------------------------------

    private ReadWriteVersionManager getVersionManager() {
        if (vMgr == null) {
            vMgr = new ReadWriteVersionManager(versionStorageNode, workspaceRoot);
        }
        return vMgr;
    }

    interface Operation {

        void perform() throws CommitFailedException;
    }

    private class AddVersionLabels implements Operation {

        private Iterable<String> labelPaths;

        public AddVersionLabels(Iterable<String> labelPaths) {
            this.labelPaths = labelPaths;
        }

        @Override
        public void perform() throws CommitFailedException {
            for (String s : labelPaths) {
                VersionLabel label = new VersionLabel(s);
                getVersionManager().addVersionLabel(label.versionHistoryPath, label.label, label.versionName);
            }
        }
    }

    private class RemoveVersionLabels implements Operation {

        private Iterable<String> labelPaths;

        public RemoveVersionLabels(Iterable<String> labelPaths) {
            this.labelPaths = labelPaths;
        }

        @Override
        public void perform() throws CommitFailedException {
            for (String s : labelPaths) {
                VersionLabel label = new VersionLabel(s);
                getVersionManager().removeVersionLabel(label.versionHistoryPath, label.label);
            }
        }
    }

    private class RemoveVersion implements Operation {

        private final Iterable<String> versionPaths;

        private RemoveVersion(@Nonnull Iterable<String> versionPaths) {
            this.versionPaths = versionPaths;
        }

        @Override
        public void perform() throws CommitFailedException {
            for (String path : versionPaths) {
                getVersionManager().removeVersion(path);
            }
        }
    }

    private static class VersionLabel {

        private final String versionHistoryPath;

        private final String label;

        private final String versionName;

        /**
         * @param path a label path as defined in the constructor of
         * {@link VersionStorageEditor}.
         * @throws IllegalArgumentException if the path is malformed
         */
        VersionLabel(@Nonnull String path) throws IllegalArgumentException {
            checkArgument(!PathUtils.isAbsolute(checkNotNull(path)),
                    "Version label path must be relative");
            List<String> elements = Lists.newArrayList(PathUtils.elements(path));
            // length of the path must be 7:
            // intermediate versionstorage nodes : 3
            // version history node : 1
            // jcr:versionLabels : 1
            // version label : 1
            // version name : 1
            if (elements.size() != 7) {
                throw new IllegalArgumentException(
                        "Invalid version label path: " + path);
            }
            StringBuilder builder = new StringBuilder();
            String slash = "";
            for (String element : elements.subList(0, 4)) {
                builder.append(slash);
                builder.append(element);
                slash = "/";
            }
            versionHistoryPath = builder.toString();
            checkArgument(elements.get(4).equals(JCR_VERSIONLABELS),
                    "Invalid version label path: " + path);
            label = elements.get(5);
            versionName = elements.get(6);
        }
    }
}
