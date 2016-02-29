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
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Joiner;

public class VersionHistoryUtil {

    public static String getVersionHistoryPath(String versionableUuid) {
        return Joiner.on('/').join(concat(
                singleton(""),
                getVersionHistoryPathSegments(versionableUuid),
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
    static NodeState getVersionHistoryNodeState(NodeState root, String versionableUuid) {
        NodeState historyParent = root;
        for (String segment : getVersionHistoryPathSegments(versionableUuid)) {
            historyParent = historyParent.getChildNode(segment);
        }
        return historyParent.getChildNode(versionableUuid);
    }

    static NodeBuilder getVersionHistoryBuilder(NodeBuilder root, String versionableUuid) {
        NodeBuilder history = root;
        for (String segment : getVersionHistoryPathSegments(versionableUuid)) {
            history = history.getChildNode(segment);
        }
        return history.getChildNode(versionableUuid);
    }

    private static List<String> getVersionHistoryPathSegments(String versionableUuid) {
        final List<String> segments = new ArrayList<String>();
        segments.add(JCR_SYSTEM);
        segments.add(JCR_VERSIONSTORAGE);
        for (int i = 0; i < 3; i++) {
            segments.add(versionableUuid.substring(i * 2, i * 2 + 2));
        }
        return segments;
    }

}
