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
package org.apache.jackrabbit.oak.jcr.xml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import javax.annotation.Nonnull;
import javax.jcr.InvalidSerializedDataException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.xml.Importer;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * {@code SysViewImportHandler}  ...
 */
class SysViewImportHandler extends TargetImportHandler {

    /**
     * stack of ImportState instances; an instance is pushed onto the stack
     * in the startElement method every time a sv:node element is encountered;
     * the same instance is popped from the stack in the endElement method
     * when the corresponding sv:node element is encountered.
     */
    private final Stack<ImportState> stack = new Stack<ImportState>();
    private final ArrayList<BufferedStringValue> currentPropValues = new ArrayList<BufferedStringValue>();

    /**
     * fields used temporarily while processing sv:property and sv:value elements
     */
    private NameInfo currentPropName;
    private int currentPropType = PropertyType.UNDEFINED;
    private PropInfo.MultipleStatus currentPropMultipleStatus = PropInfo.MultipleStatus.UNKNOWN;
    // list of appendable value objects
    private BufferedStringValue currentPropValue;

    /**
     * Constructs a new {@code SysViewImportHandler}.
     *
     * @param importer     the underlying importer
     * @param sessionContext the session context
     */
    SysViewImportHandler(Importer importer, SessionContext sessionContext) {
        super(importer, sessionContext);
    }

    private void processNode(ImportState state, boolean start, boolean end)
            throws SAXException {
        if (!start && !end) {
            return;
        }
        String id = state.uuid;
        NodeInfo node = new NodeInfo(state.nodeName, state.nodeTypeName, state.mixinNames, id);
        // call Importer
        try {
            if (start) {
                importer.startNode(node, state.props);
                // dispose temporary property values
                for (PropInfo pi : state.props) {
                    pi.dispose();
                }

            }
            if (end) {
                importer.endNode(node);
            }
        } catch (RepositoryException re) {
            throw new SAXException(re);
        }
    }

    //-------------------------------------------------------< ContentHandler >

    @Override
    public void startElement(String namespaceURI, String localName,
                             String qName, Attributes atts)
            throws SAXException {
        // check element name
        if (namespaceURI.equals(NamespaceConstants.NAMESPACE_SV) && "node".equals(localName)) {
            // sv:node element

            // node name (value of sv:name attribute)
            String svName = getAttribute(atts, NamespaceConstants.NAMESPACE_SV, "name");
            if (svName == null) {
                throw new SAXException(new InvalidSerializedDataException(
                        "missing mandatory sv:name attribute of element sv:node"));
            }

            if (!stack.isEmpty()) {
                // process current node first
                ImportState current = stack.peek();
                // need to start current node
                if (!current.started) {
                    processNode(current, true, false);
                    current.started = true;
                }
            }

            // push new ImportState instance onto the stack
            ImportState state = new ImportState();
            try {
                state.nodeName = new NameInfo(svName).getRepoQualifiedName();
            } catch (RepositoryException e) {
                throw new SAXException(new InvalidSerializedDataException("illegal node name: " + svName, e));
            }
            stack.push(state);
        } else if (namespaceURI.equals(NamespaceConstants.NAMESPACE_SV) && "property".equals(localName)) {
            // sv:property element

            // reset temp fields
            currentPropValues.clear();

            // property name (value of sv:name attribute)
            String svName = getAttribute(atts, NamespaceConstants.NAMESPACE_SV, "name");
            if (svName == null) {
                throw new SAXException(new InvalidSerializedDataException(
                        "missing mandatory sv:name attribute of element sv:property"));
            }
            try {
                currentPropName = new NameInfo(svName);
            } catch (RepositoryException e) {
                throw new SAXException(new InvalidSerializedDataException("illegal property name: " + svName, e));
            }
            // property type (sv:type attribute)
            String type = getAttribute(atts, NamespaceConstants.NAMESPACE_SV, "type");
            if (type == null) {
                throw new SAXException(new InvalidSerializedDataException(
                        "missing mandatory sv:type attribute of element sv:property"));
            }
            try {
                currentPropType = PropertyType.valueFromName(type);
            } catch (IllegalArgumentException e) {
                throw new SAXException(new InvalidSerializedDataException(
                        "Unknown property type: " + type, e));
            }
            // 'multi-value' hint (sv:multiple attribute)
            String multiple = getAttribute(atts, NamespaceConstants.NAMESPACE_SV, "multiple");
            if (multiple != null) {
                currentPropMultipleStatus = PropInfo.MultipleStatus.MULTIPLE;
            } else {
                currentPropMultipleStatus = PropInfo.MultipleStatus.UNKNOWN;
            }
        } else if (namespaceURI.equals(NamespaceConstants.NAMESPACE_SV) && "value".equals(localName)) {
            // sv:value element
            boolean base64 =
                    currentPropType == PropertyType.BINARY
                    || "xs:base64Binary".equals(atts.getValue("xsi:type"));
            currentPropValue = new BufferedStringValue(
                    sessionContext.getValueFactory(), currentNamePathMapper(),
                    base64);
        } else {
            throw new SAXException(new InvalidSerializedDataException(
                    "Unexpected element in system view xml document: {" + namespaceURI + '}' + localName));
        }
    }

    @Override
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        if (currentPropValue != null) {
            // property value (character data of sv:value element)
            try {
                currentPropValue.append(ch, start, length);
            } catch (IOException ioe) {
                throw new SAXException("error while processing property value",
                        ioe);
            }
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length)
            throws SAXException {
        if (currentPropValue != null) {
            // property value

            // data reported by the ignorableWhitespace event within
            // sv:value tags is considered part of the value
            try {
                currentPropValue.append(ch, start, length);
            } catch (IOException ioe) {
                throw new SAXException("error while processing property value",
                        ioe);
            }
        }
    }

    @Override
    public void endElement(String namespaceURI, String localName, String qName)
            throws SAXException {
        // check element name
        ImportState state = stack.peek();
        if (namespaceURI.equals(NamespaceConstants.NAMESPACE_SV) && "node".equals(localName)) {
            // sv:node element
            if (!state.started) {
                // need to start & end current node
                processNode(state, true, true);
                state.started = true;
            } else {
                // need to end current node
                processNode(state, false, true);
            }
            // pop current state from stack
            stack.pop();
        } else if (namespaceURI.equals(NamespaceConstants.NAMESPACE_SV) && "property".equals(localName)) {
            // sv:property element

            // check if all system properties (jcr:primaryType, jcr:uuid etc.)
            // have been collected and create node as necessary primaryType
            if (isSystemProperty("primaryType")) {
                BufferedStringValue val = currentPropValues.get(0);
                String s = null;
                try {
                    s = val.retrieve();
                    state.nodeTypeName = new NameInfo(s).getRepoQualifiedName();
                } catch (IOException e) {
                    throw new SAXException(new InvalidSerializedDataException("illegal node type name: " + s, e));
                } catch (RepositoryException e) {
                    throw new SAXException(new InvalidSerializedDataException("illegal node type name: " + s, e));
                }
            } else if (isSystemProperty("mixinTypes")) {
                if (state.mixinNames == null) {
                    state.mixinNames = new ArrayList<String>(currentPropValues.size());
                }
                for (BufferedStringValue val : currentPropValues) {
                    String s = null;
                    try {
                        s = val.retrieve();
                        state.mixinNames.add(new NameInfo(s).getRepoQualifiedName());
                    } catch (IOException ioe) {
                        throw new SAXException("error while retrieving value", ioe);
                    } catch (RepositoryException e) {
                        throw new SAXException(new InvalidSerializedDataException("illegal mixin type name: " + s, e));
                    }
                }
            } else if (isSystemProperty("uuid")) {
                BufferedStringValue val = currentPropValues.get(0);
                try {
                    state.uuid = val.retrieve();
                } catch (IOException ioe) {
                    throw new SAXException("error while retrieving value", ioe);
                }
            } else {
                if (currentPropMultipleStatus == PropInfo.MultipleStatus.UNKNOWN
                        && currentPropValues.size() != 1) {
                    currentPropMultipleStatus = PropInfo.MultipleStatus.MULTIPLE;
                }
                PropInfo prop = new PropInfo(
                        currentPropName == null ? null : currentPropName.getRepoQualifiedName(),
                        currentPropType,
                        currentPropValues,
                        currentPropMultipleStatus);
                state.props.add(prop);
            }
            // reset temp fields
            currentPropValues.clear();
        } else if (namespaceURI.equals(NamespaceConstants.NAMESPACE_SV) && "value".equals(localName)) {
            // sv:value element
            currentPropValues.add(currentPropValue);
            // reset temp fields
            currentPropValue = null;
        } else {
            throw new SAXException(new InvalidSerializedDataException("invalid element in system view xml document: " + localName));
        }
    }

    private boolean isSystemProperty(@Nonnull String localName) {
        return currentPropName != null
                && currentPropName.getNamespaceUri().equals(NamespaceRegistry.NAMESPACE_JCR)
                && currentPropName.getLocalName().equals(localName);
    }

    //--------------------------------------------------------< inner classes >

    /**
     * The state of parsing the XML stream.
     */
    static class ImportState {
        /**
         * name of current node
         */
        String nodeName;
        /**
         * primary type of current node
         */
        String nodeTypeName;
        /**
         * list of mixin types of current node
         */
        List<String> mixinNames;
        /**
         * uuid of current node
         */
        String uuid;

        /**
         * list of PropInfo instances representing properties of current node
         */
        final List<PropInfo> props = new ArrayList<PropInfo>();

        /**
         * flag indicating whether startNode() has been called for current node
         */
        boolean started;
    }

    //-------------------------------------------------------------< private >

    /**
     * Returns the value of the named XML attribute.
     *
     * @param attributes set of XML attributes
     * @param namespaceUri attribute namespace
     * @param localName attribute local name
     * @return attribute value,
     *         or {@code null} if the named attribute is not found
     */
    private static String getAttribute(Attributes attributes, String namespaceUri, String localName) {
        return attributes.getValue(namespaceUri, localName);
    }

}
