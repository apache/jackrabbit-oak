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
package org.apache.jackrabbit.oak.plugins.version;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.ISO8601;

/**
 * <i>Inspired by Jackrabbit 2.x</i>
 * <p>
 * This Class implements a version selector that selects a version by creation
 * date. The selected version is the latest that is older or equal than the
 * given date. If no version could be found {@code null} is returned
 * unless the {@code returnLatest} flag is set to {@code true}, where
 * the latest version is returned.
 * <pre>
 * V1.0 - 02-Sep-2006
 * V1.1 - 03-Sep-2006
 * V1.2 - 05-Sep-2006
 *
 * new DateVersionSelector("03-Sep-2006").select() -&gt; V1.1
 * new DateVersionSelector("04-Sep-2006").select() -&gt; V1.1
 * new DateVersionSelector("01-Sep-2006").select() -&gt; null
 * new DateVersionSelector("01-Sep-2006", true).select() -&gt; V1.2
 * new DateVersionSelector(null, true).select() -&gt; V1.2
 * </pre>
 */
class DateVersionSelector implements VersionSelector {

    /**
     * a version date hint
     */
    private final long timestamp;

    /**
     * Creates a {@code DateVersionSelector} that will select the latest
     * version of all those that are older than the given timestamp.
     *
     * @param timestamp reference timestamp
     */
    public DateVersionSelector(String timestamp) {
        this.timestamp = ISO8601.parse(timestamp).getTimeInMillis();
    }

    @Override
    public NodeBuilder select(@Nonnull NodeBuilder history)
            throws RepositoryException {
        long latestDate = Long.MIN_VALUE;
        NodeBuilder latestVersion = null;
        for (String name: history.getChildNodeNames()) {
            // OAK-1192 skip hidden child nodes
            if (name.charAt(0) == ':') {
                continue;
            }
            NodeBuilder v = history.getChildNode(name);
            if (name.equals(JcrConstants.JCR_ROOTVERSION)
                    || name.equals(JcrConstants.JCR_VERSIONLABELS)) {
                // ignore root version and labels node
                continue;
            }
            long c = ISO8601.parse(v.getProperty(JcrConstants.JCR_CREATED).getValue(Type.DATE)).getTimeInMillis();
            if (c > latestDate && c <= timestamp) {
                latestDate = c;
                latestVersion = v;
            } else if (c == latestDate) {
                throw new RepositoryException("two versions share the same jcr:created timestamp in history:" + history);
            }
        }
        return latestVersion;
    }
}
