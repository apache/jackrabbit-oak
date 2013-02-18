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
package org.apache.jackrabbit.oak.plugins.commit;

import java.util.Date;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.NodeTypeCommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This class provides a commit hook for nodes of type {@code mix:lastModified} that properly implements section
 * 3.7.11.8 of JSR283:
 * 
 * <pre>
 * [mix:lastModified] mixin
 * - jcr:lastModified (DATE) autocreated protected? OPV?
 * - jcr:lastModifiedBy (STRING) autocreated protected? OPV?
 * </pre>
 * 
 */
public class MixLastModifiedCommitHook extends NodeTypeCommitHook {

    public MixLastModifiedCommitHook() {
        super(NodeTypeConstants.MIX_LASTMODIFIED);
    }

    @Override
    public void processChangedNode(NodeState before, NodeState after, NodeBuilder nodeBuilder, @Nonnull String userID,
            @Nonnull Date lastModifiedDate) {
        if (!skipLastModifiedDate(before, after)) {
            nodeBuilder.setProperty(LongPropertyState.createDateProperty(NodeTypeConstants.JCR_LASTMODIFIED,
                    lastModifiedDate.getTime()));
        }
        if (!skipLastModifiedBy(before, after)) {
            nodeBuilder.setProperty(StringPropertyState.stringProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, userID));
        }
    }

    private boolean skipLastModifiedDate(NodeState before, NodeState after) {
        boolean result = false;
        PropertyState jcrLastModifiedDateBefore = before.getProperty(NodeTypeConstants.JCR_LASTMODIFIED);
        PropertyState jcrLastModifiedDateAfter = after.getProperty(NodeTypeConstants.JCR_LASTMODIFIED);
        if (jcrLastModifiedDateAfter != null && !jcrLastModifiedDateAfter.equals(jcrLastModifiedDateBefore)) {
            result = true;
        }
        return result;
    }

    private boolean skipLastModifiedBy(NodeState before, NodeState after) {
        boolean result = false;
        PropertyState jcrLastModifiedByBefore = before.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY);
        PropertyState jcrLastModifiedByAfter = after.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY);
        if (jcrLastModifiedByAfter != null && !jcrLastModifiedByAfter.equals(jcrLastModifiedByBefore)) {
            result = true;
        }
        return result;
    }
}
