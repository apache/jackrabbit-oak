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
package org.apache.jackrabbit.oak.upgrade.version;

import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Joiner;

public class VersionHistoryUtil {

    public static String getRelativeVersionHistoryPath(String versionableUuid) {
        return Joiner.on('/').join(concat(
                singleton(""),
                getRelativeVersionHistoryPathSegments(versionableUuid),
                singleton(versionableUuid)));
    }

    /**
     * Constructs the version history path based on the versionable's UUID.
     *
     * @param root The root NodeState below which to look for the version.
     * @param versionableUuid The String representation of the versionable's UUID.
     * @return The NodeState corresponding to the version history, or {@code null}
     *         if it does not exist.
     */
    static NodeState getVersionHistoryNodeState(NodeState versionStorage, String versionableUuid) {
        NodeState historyParent = versionStorage;
        for (String segment : getRelativeVersionHistoryPathSegments(versionableUuid)) {
            historyParent = historyParent.getChildNode(segment);
        }
        return historyParent.getChildNode(versionableUuid);
    }

    static NodeBuilder getVersionHistoryBuilder(NodeBuilder versionStorage, String versionableUuid) {
        NodeBuilder history = versionStorage;
        for (String segment : getRelativeVersionHistoryPathSegments(versionableUuid)) {
            history = history.getChildNode(segment);
        }
        return history.getChildNode(versionableUuid);
    }

    private static List<String> getRelativeVersionHistoryPathSegments(String versionableUuid) {
        final List<String> segments = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            segments.add(versionableUuid.substring(i * 2, i * 2 + 2));
        }
        return segments;
    }

    public static NodeState getVersionStorage(NodeState root) {
        return root.getChildNode(JCR_SYSTEM).getChildNode(JCR_VERSIONSTORAGE);
    }

    public static NodeBuilder getVersionStorage(NodeBuilder root) {
        return root.getChildNode(JCR_SYSTEM).getChildNode(JCR_VERSIONSTORAGE);
    }

    public static NodeBuilder createVersionStorage(NodeBuilder root) {
        NodeBuilder vs = root.child(JCR_SYSTEM).child(JCR_VERSIONSTORAGE);
        if (!vs.hasProperty(JCR_PRIMARYTYPE)) {
            vs.setProperty(JCR_PRIMARYTYPE, REP_VERSIONSTORAGE, Type.NAME);
        }
        return vs;
    }

}
