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
package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This {@link ConflictHandler} implementation wraps another conflict handler
 * and forwards all calls to the wrapped handler. Sub classes may override
 * methods of this class and intercept calls they are interested in.
 */
public class ConflictHandlerWrapper implements ConflictHandler {

    protected final ConflictHandler handler;

    public ConflictHandlerWrapper(ConflictHandler handler) {
        this.handler = handler;
    }

    @Override
    public Resolution addExistingProperty(NodeBuilder parent,
                                          PropertyState ours,
                                          PropertyState theirs) {
        return handler.addExistingProperty(parent, ours, theirs);
    }

    @Override
    public Resolution changeDeletedProperty(NodeBuilder parent,
                                            PropertyState ours) {
        return handler.changeDeletedProperty(parent, ours);
    }

    @Override
    public Resolution changeChangedProperty(NodeBuilder parent,
                                            PropertyState ours,
                                            PropertyState theirs) {
        return handler.changeChangedProperty(parent, ours, theirs);
    }

    @Override
    public Resolution deleteDeletedProperty(NodeBuilder parent,
                                            PropertyState ours) {
        return handler.deleteDeletedProperty(parent, ours);
    }

    @Override
    public Resolution deleteChangedProperty(NodeBuilder parent,
                                            PropertyState theirs) {
        return handler.deleteChangedProperty(parent, theirs);
    }

    @Override
    public Resolution addExistingNode(NodeBuilder parent,
                                      String name,
                                      NodeState ours,
                                      NodeState theirs) {
        return handler.addExistingNode(parent, name, ours, theirs);
    }

    @Override
    public Resolution changeDeletedNode(NodeBuilder parent,
                                        String name,
                                        NodeState ours) {
        return handler.changeDeletedNode(parent, name, ours);
    }

    @Override
    public Resolution deleteChangedNode(NodeBuilder parent,
                                        String name,
                                        NodeState theirs) {
        return handler.deleteChangedNode(parent, name, theirs);
    }

    @Override
    public Resolution deleteDeletedNode(NodeBuilder parent, String name) {
        return handler.deleteDeletedNode(parent, name);
    }
}
