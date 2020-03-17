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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.NAMES;

final class NodeStateCopyUtils {
    private static final String OAK_CHILD_ORDER = ":childOrder";

    public static void copyToTree(NodeState state, Tree tree){
        tree.setOrderableChildren(state.hasProperty(OAK_CHILD_ORDER));
        copyProps(state, tree);

        Tree src = TreeFactory.createReadOnlyTree(state);
        for (Tree srcChild : src.getChildren()){
            String childName = srcChild.getName();
            Tree child = tree.addChild(childName);
            copyToTree(state.getChildNode(childName), child);
        }
    }

    public static void copyToNode(NodeState state, Node node) throws RepositoryException {
        copyProps(state, node);

        Tree src = TreeFactory.createReadOnlyTree(state);
        for (Tree srcChild : src.getChildren()){
            String childName = srcChild.getName();

            if (NodeStateUtils.isHidden(childName)){
                continue;
            }

            NodeState childState = state.getChildNode(childName);
            Node child = JcrUtils.getOrAddNode(node, childName, primaryType(childState));
            copyToNode(childState, child);
        }
    }

    private static void copyProps(NodeState state, Tree tree) {
        for (PropertyState ps : state.getProperties()){
            if (!ps.getName().equals(OAK_CHILD_ORDER)){
                tree.setProperty(ps);
            }
        }
    }

    private static void copyProps(NodeState state, Node node) throws RepositoryException {
        ValueFactory vf = node.getSession().getValueFactory();
        for (PropertyState ps : state.getProperties()){
            String name = ps.getName();
            if (name.equals(JcrConstants.JCR_PRIMARYTYPE)
                    || name.equals(OAK_CHILD_ORDER)){
                continue;
            }

            if (name.equals(JcrConstants.JCR_MIXINTYPES)){
                for (String n : ps.getValue(NAMES)) {
                    node.addMixin(n);
                }
                continue;
            }

            if (NodeStateUtils.isHidden(name)){
                continue;
            }

            if (ps.isArray()){
                Value[] values = new Value[ps.count()];
                for (int i = 0; i < ps.count(); i++) {
                    values[i] = createValue(vf, ps, i);
                }
                node.setProperty(name, values, ps.getType().tag());
            } else {
                node.setProperty(name, createValue(vf, ps, -1), ps.getType().tag());
            }
        }
    }

    private static Value createValue(ValueFactory vf, PropertyState ps, int index) throws RepositoryException {
        switch(ps.getType().tag()) {
            case PropertyType.STRING :
                return vf.createValue(getValue(ps, Type.STRING, index));
            case PropertyType.BINARY:
                Blob blob = getValue(ps, Type.BINARY, index);
                Binary bin = vf.createBinary(blob.getNewStream());
                return vf.createValue(bin);
            case PropertyType.LONG:
                return vf.createValue(getValue(ps, Type.LONG, index));
            case PropertyType.DOUBLE:
                return vf.createValue(getValue(ps, Type.DOUBLE, index));
            case PropertyType.DATE:
                return vf.createValue(getValue(ps, Type.DATE, index));
            case PropertyType.BOOLEAN:
                return vf.createValue(getValue(ps, Type.BOOLEAN, index));
            case PropertyType.NAME:
                return vf.createValue(getValue(ps, Type.NAME, index));
            case PropertyType.PATH:
                return vf.createValue(getValue(ps, Type.PATH, index));
            case PropertyType.REFERENCE:
                return vf.createValue(getValue(ps, Type.REFERENCE, index));
            case PropertyType.WEAKREFERENCE:
                return vf.createValue(getValue(ps, Type.WEAKREFERENCE, index));
            case PropertyType.URI:
                return vf.createValue(getValue(ps, Type.URI, index));
            case PropertyType.DECIMAL:
                return vf.createValue(getValue(ps, Type.DECIMAL, index));
            default:
                throw new IllegalStateException("Unsupported type " + ps.getType());
        }
    }

    private static <T> T getValue(PropertyState ps, Type<T> type, int index){
        return index < 0 ? ps.getValue(type) : ps.getValue(type, index);
    }

    private static String primaryType(NodeState state){
        return checkNotNull(state.getName(JcrConstants.JCR_PRIMARYTYPE), "jcr:primaryType not defined for %s", state);
    }
}
