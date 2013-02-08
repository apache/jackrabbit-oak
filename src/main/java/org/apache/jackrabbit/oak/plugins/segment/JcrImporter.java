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
package org.apache.jackrabbit.oak.plugins.segment;

import java.util.Arrays;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class JcrImporter {

    private final SegmentWriter writer;

    public JcrImporter(SegmentWriter writer) {
        this.writer = writer;
    }

    public RecordId writeNode(Node node) throws RepositoryException {
        NodeBuilder builder =
                new MemoryNodeBuilder(MemoryNodeState.EMPTY_NODE);
        buildNode(builder, node);
        return writer.writeNode(builder.getNodeState());
    }

    private void buildNode(NodeBuilder builder, Node node)
            throws RepositoryException {
        PropertyIterator properties = node.getProperties();
        while (properties.hasNext()) {
            Property property = properties.nextProperty();
            if (property.isMultiple()) {
                builder.setProperty(PropertyStates.createProperty(
                        property.getName(),
                        Arrays.asList(property.getValues())));
            } else {
                builder.setProperty(PropertyStates.createProperty(
                        property.getName(), property.getValue()));
            }
        }

        NodeIterator childNodes = node.getNodes();
        while (childNodes.hasNext()) {
            Node childNode = childNodes.nextNode();
            buildNode(builder.child(childNode.getName()), childNode);
        }
    }

}
