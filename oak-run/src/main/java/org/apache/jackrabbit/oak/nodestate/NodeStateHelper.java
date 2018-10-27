/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.nodestate;

import static java.lang.Integer.getInteger;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public abstract class NodeStateHelper {

    private static final int CHILDREN_CAP = getInteger("oak.children.cap", 100);

    public static String nodeStateToString(NodeState state) {
        if (!state.exists()) {
            return "{N/A}";
        }
        StringBuilder builder = new StringBuilder("{");
        String separator = " ";
        for (PropertyState property : state.getProperties()) {
            builder.append(separator);
            separator = ", ";
            try {
                builder.append(property);
            } catch (Throwable t) {
                builder.append(property.getName());
                builder.append(" = { ERROR on property: ");
                builder.append(t.getMessage());
                builder.append(" }");
            }
        }
        int count = CHILDREN_CAP;
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (count-- == 0) {
                builder.append("...");
                break;
            }
            builder.append(separator);
            separator = ", ";
            try {
                builder.append(childNodeEntryToString(entry));
            } catch (Throwable t) {
                builder.append(entry.getName());
                builder.append(" = { ERROR on node: ");
                builder.append(t.getMessage());
                builder.append(" }");
            }
        }
        builder.append(" }");
        return builder.toString();
    }

    public static String childNodeEntryToString(ChildNodeEntry entry) {
        String name = entry.getName();
        NodeState state = entry.getNodeState();
        if (state.getChildNodeCount(1) == 0) {
            return name + " : " + nodeStateToString(state);
        } else {
            return name + " = { ... }";
        }
    }
}
