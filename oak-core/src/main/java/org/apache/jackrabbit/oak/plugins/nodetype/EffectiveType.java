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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_MANDATORY;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;

import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class EffectiveType {

    private final List<NodeState> types;

    EffectiveType(List<NodeState> types) {
        this.types = checkNotNull(types);
    }

    void checkMandatoryItems(NodeState node)
            throws ConstraintViolationException {
        for (NodeState type : types) {
            NodeState properties =
                    type.getChildNode("oak:namedPropertyDefinitions");
            if (properties != null) {
                for (ChildNodeEntry entry : properties.getChildNodeEntries()) {
                    String name = entry.getName();
                    if ("oak:primaryType".equals(name)) {
                        name = JCR_PRIMARYTYPE;
                    } else if ("oak:mixinTypes".equals(name)) {
                        name = JCR_MIXINTYPES;
                    } else if ("oak:uuid".equals(name)) {
                        name = JCR_UUID;
                    }
                    if (node.getProperty(name) == null
                            && isMandatory(name, entry.getNodeState())) {
                        throw new ConstraintViolationException(
                                "Missing mandatory property " + name);
                    }
                }
            }

            NodeState childNodes =
                    type.getChildNode("oak:namedChildNodeDefinitions");
            if (childNodes != null) {
                for (ChildNodeEntry entry : childNodes.getChildNodeEntries()) {
                    String name = entry.getName();
                    if (!node.hasChildNode(name)
                            && isMandatory(name, entry.getNodeState())) {
                        throw new ConstraintViolationException(
                                "Missing mandatory child node " + name);
                    }
                }
            }
        }
    }

    /**
     * Finds a matching definition for a property with the given name and type.
     *
     * @param property modified property
     * @return matching property definition
     * @throws ConstraintViolationException if a matching definition was not found
     */
    @Nonnull
    NodeState getDefinition(PropertyState property)
            throws ConstraintViolationException {
        String propertyName = property.getName();
        Type<?> propertyType = property.getType();

        String escapedName;
        if (JCR_PRIMARYTYPE.equals(propertyName)) {
            escapedName = "oak:primaryType";
        } else if (JCR_MIXINTYPES.equals(propertyName)) {
            escapedName = "oak:mixinTypes";
        } else if (JCR_UUID.equals(propertyName)) {
            escapedName = "oak:uuid";
        } else {
            escapedName = propertyName;
        }

        String definedType = getTypeKey(propertyType);
        String undefinedType;
        if (propertyType.isArray()) {
            undefinedType = "UNDEFINEDS";
        } else {
            undefinedType = "UNDEFINED";
        }

        // Find matching named property definition
        for (NodeState type : types) {
            NodeState named = type.getChildNode("oak:namedPropertyDefinitions");
            if (named != null) {
                NodeState definitions = named.getChildNode(escapedName);
                if (definitions != null) {
                    NodeState definition = definitions.getChildNode(definedType);
                    if (definition == null) {
                        definition = definitions.getChildNode(undefinedType);
                    }
                    if (definition != null) {
                        return definition;
                    } else {
                        throw new ConstraintViolationException(
                                "No matching definition found for property "
                                        + propertyName);
                    }
                }
            }
        }

        // Find matching residual property definition
        for (NodeState type : types) {
            NodeState residual =
                    type.getChildNode("oak:residualPropertyDefinitions");
            if (residual != null) {
                NodeState definition = residual.getChildNode(definedType);
                if (definition == null) {
                    definition = residual.getChildNode(undefinedType);
                }
                if (definition != null) {
                    return definition;
                }
            }
        }

        throw new ConstraintViolationException(
                "No matching definition found for property " + propertyName);
    }

    /**
     * Finds a matching definition for a child node with the given name and
     * types.
     *
     * @param nodeName child node name
     * @param nodeType effective types of the child node
     * @return matching child node definition
     * @throws ConstraintViolationException if a matching definition was not found
     */
    @Nonnull
    NodeState getDefinition(String nodeName, Iterable<String> nodeType)
            throws ConstraintViolationException {
        boolean sns = false;
        int n = nodeName.length();
        if (n > 3 && nodeName.charAt(n - 1) == ']') {
            int i = n - 2;
            while (i > 1 && Character.isDigit(nodeName.charAt(i))) {
                i--;
            }
            if (nodeName.charAt(i) == '[') {
                nodeName = nodeName.substring(0, i);
                sns = true;
            }
        }

        // Find matching named child node definition
        for (NodeState type : types) {
            NodeState named = type.getChildNode("oak:namedChildNodeDefinitions");
            if (named != null) {
                NodeState definitions = named.getChildNode(nodeName);
                if (definitions != null) {
                    for (String typeName : nodeType) {
                        NodeState definition = definitions.getChildNode(typeName);
                        if (definition != null) {
                            return definition;
                        }
                    }

                    throw new ConstraintViolationException(
                            "Incorrect node type of child node " + nodeName);
                }
            }
        }

        // Find matching residual child node definition
        for (NodeState type : types) {
            NodeState residual =
                    type.getChildNode("oak:residualChildNodeDefinitions");
            if (residual != null) {
                for (String typeName : nodeType) {
                    NodeState definition = residual.getChildNode(typeName);
                    if (definition != null) {
                        return definition;
                    }
                }
            }
        }

        throw new ConstraintViolationException(
                "Incorrect node type of child node " + nodeName);
    }


    //-----------------------------------------------------------< private >--
    
    private boolean isMandatory(String name, NodeState definitions) {
        for (ChildNodeEntry entry : definitions.getChildNodeEntries()) {
            NodeState definition = entry.getNodeState();
            if (getBoolean(definition, JCR_MANDATORY)) {
                return true;
            }
        }
        return false;
    }

    private boolean getBoolean(NodeState node, String name) {
        PropertyState property = node.getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    private String getTypeKey(Type<?> type) {
        if (type == Type.BINARIES) {
            return "BINARIES";
        } else if (type == Type.BINARY) {
            return "BINARY";
        } else if (type == Type.BOOLEAN) {
            return "BOOLEAN";
        } else if (type == Type.BOOLEANS) {
            return "BOOLEANS";
        } else if (type == Type.DATE) {
            return "DATE";
        } else if (type == Type.DATES) {
            return "DATES";
        } else if (type == Type.DECIMAL) {
            return "DECIMAL";
        } else if (type == Type.DECIMALS) {
            return "DECIMALS";
        } else if (type == Type.DOUBLE) {
            return "DOUBLE";
        } else if (type == Type.DOUBLES) {
            return "DOUBLES";
        } else if (type == Type.LONG) {
            return "LONG";
        } else if (type == Type.LONGS) {
            return "LONGS";
        } else if (type == Type.NAME) {
            return "NAME";
        } else if (type == Type.NAMES) {
            return "NAMES";
        } else if (type == Type.PATH) {
            return "PATH";
        } else if (type == Type.PATHS) {
            return "PATHS";
        } else if (type == Type.REFERENCE) {
            return "REFERENCE";
        } else if (type == Type.REFERENCES) {
            return "REFERENCES";
        } else if (type == Type.STRING) {
            return "STRING";
        } else if (type == Type.STRINGS) {
            return "STRINGS";
        } else if (type == Type.URI) {
            return "URI";
        } else if (type == Type.URIS) {
            return "URIS";
        } else if (type == Type.WEAKREFERENCE) {
            return "WEAKREFERENCE";
        } else if (type == Type.WEAKREFERENCES) {
            return "WEAKREFERENCES";
        } else {
            return "unknown";
        }
    }

}
