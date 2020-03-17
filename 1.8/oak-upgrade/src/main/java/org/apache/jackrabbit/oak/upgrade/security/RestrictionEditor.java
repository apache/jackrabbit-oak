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
package org.apache.jackrabbit.oak.upgrade.security;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.NT_REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_RESTRICTIONS;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class RestrictionEditor extends DefaultEditor {

    private final NodeBuilder builder;

    private final TypePredicate isACE;

    private PropertyState glob = null;

    public RestrictionEditor(NodeBuilder builder, TypePredicate isACE) {
        this.builder = builder;
        this.isACE = isACE;
    }

    private RestrictionEditor(RestrictionEditor parent, String name) {
        this.builder = parent.builder.getChildNode(name);
        this.isACE = parent.isACE;
    }

    @Override
    public void leave(NodeState before, NodeState after) {
        if (glob != null
                && isACE.apply(after)
                && !builder.hasChildNode(REP_RESTRICTIONS)) {
            NodeBuilder restrictions = builder.setChildNode(REP_RESTRICTIONS);
            restrictions.setProperty(JCR_PRIMARYTYPE, NT_REP_RESTRICTIONS, NAME);
            restrictions.setProperty(glob);
            builder.removeProperty(REP_GLOB);
        }
    }

    @Override
    public void propertyAdded(PropertyState after) {
        if (REP_GLOB.equals(after.getName())) {
            glob = after;
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        if (REP_GLOB.equals(after.getName())) {
            glob = after;
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        return new RestrictionEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        return new RestrictionEditor(this, name);
    }

}
