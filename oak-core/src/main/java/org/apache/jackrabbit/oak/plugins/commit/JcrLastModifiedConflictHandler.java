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

package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.util.ISO8601.parse;

import java.util.Calendar;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class JcrLastModifiedConflictHandler implements PartialConflictHandler {

    @Override
    public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours,
            PropertyState theirs) {
        if (isLastModified(ours)) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        }
        return null;
    }

    @Override
    public Resolution changeDeletedProperty(NodeBuilder parent, PropertyState ours) {
        return null;
    }

    @Override
    public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours,
            PropertyState theirs) {
        if (isLastModified(ours)) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        }
        return null;
    }

    private static void merge(NodeBuilder parent, PropertyState ours,
            PropertyState theirs) {
        Calendar o = parse(ours.getValue(Type.DATE));
        Calendar t = parse(theirs.getValue(Type.DATE));
        // pick & set newer one
        if (o.before(t)) {
            parent.setProperty(JCR_LASTMODIFIED, t);
        } else {
            parent.setProperty(JCR_LASTMODIFIED, o);
        }
    }

    private static boolean isLastModified(PropertyState p) {
        return JCR_LASTMODIFIED.equals(p.getName());
    }

    @Override
    public Resolution deleteDeletedProperty(NodeBuilder parent, PropertyState ours) {
        return null;
    }

    @Override
    public Resolution deleteChangedProperty(NodeBuilder parent, PropertyState theirs) {
        return null;
    }

    @Override
    public Resolution addExistingNode(NodeBuilder parent, String name, NodeState ours, NodeState theirs) {
        return null;
    }

    @Override
    public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours) {
        return null;
    }

    @Override
    public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs) {
        return null;
    }

    @Override
    public Resolution deleteDeletedNode(NodeBuilder parent, String name) {
        return null;
    }
}
