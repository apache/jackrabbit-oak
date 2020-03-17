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
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.xml.Importer;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.util.ISO9075;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.google.common.collect.Lists;

/**
 * {@code DocViewImportHandler} processes Document View XML SAX events
 * and 'translates' them into {@code {@link Importer}} method calls.
 */
class DocViewImportHandler extends TargetImportHandler {

    private static final Logger log = LoggerFactory.getLogger(DocViewImportHandler.class);

    /**
     * stack of NodeInfo instances; an instance is pushed onto the stack
     * in the startElement method and is popped from the stack in the
     * endElement method.
     */
    private final Stack<NodeInfo> stack = new Stack<NodeInfo>();
    // buffer used to merge adjacent character data
    private BufferedStringValue textHandler;

    /**
     * Constructs a new {@code DocViewImportHandler}.
     *
     * @param importer     the importer
     * @param sessionContext the session context
     */
    DocViewImportHandler(Importer importer, SessionContext sessionContext) {
        super(importer, sessionContext);
    }

    /**
     * Parses the given string as a list of JCR names. Any whitespace sequence
     * is supported as a names separator instead of just a single space to
     * be more liberal in what we accept. The current namespace context is
     * used to convert the prefixed name strings to QNames.
     *
     * @param value string value
     * @return the parsed names
     * @throws SAXException if an invalid name was encountered
     */
    private Iterable<String> parseNames(String value) throws SAXException {
        String[] names = value.split("\\p{Space}+");
        List<String> qnames = Lists.newArrayListWithCapacity(names.length);
        for (String name : names) {
            try {
                qnames.add(new NameInfo(name).getRepoQualifiedName());
            } catch (RepositoryException e) {
                throw new SAXException("Invalid name: " + name, e);
            }
        }
        return qnames;
    }

    /**
     * Appends the given character data to the internal buffer.
     *
     * @param ch     the characters to be appended
     * @param start  the index of the first character to append
     * @param length the number of characters to append
     * @throws SAXException if an error occurs
     * @see #characters(char[], int, int)
     * @see #ignorableWhitespace(char[], int, int)
     * @see #processCharacters()
     */
    private void appendCharacters(char[] ch, int start, int length)
            throws SAXException {
        if (textHandler == null) {
            textHandler = new BufferedStringValue(
                    sessionContext.getValueFactory(), currentNamePathMapper(),
                    false);
        }
        try {
            textHandler.append(ch, start, length);
        } catch (IOException ioe) {
            String msg = "internal error while processing internal buffer data";
            log.error(msg, ioe);
            throw new SAXException(msg, ioe);
        }
    }

    /**
     * Translates character data reported by the
     * {@code {@link #characters(char[], int, int)}} &
     * {@code {@link #ignorableWhitespace(char[], int, int)}} SAX events
     * into a  {@code jcr:xmltext} child node with one
     * {@code jcr:xmlcharacters} property.
     *
     * @throws SAXException if an error occurs
     * @see #appendCharacters(char[], int, int)
     */
    private void processCharacters()
            throws SAXException {
        try {
            if (textHandler != null && textHandler.length() > 0) {
                // there is character data that needs to be added to
                // the current node

                // check for pure whitespace character data
                Reader reader = textHandler.reader();
                try {
                    int ch;
                    while ((ch = reader.read()) != -1) {
                        if (ch > 0x20) {
                            break;
                        }
                    }
                    if (ch == -1) {
                        // the character data consists of pure whitespace, ignore
                        log.debug("ignoring pure whitespace character data...");
                        // reset handler
                        textHandler.dispose();
                        textHandler = null;
                        return;
                    }
                } finally {
                    reader.close();
                }

                NodeInfo node = new NodeInfo(getJcrName(NamespaceRegistry.NAMESPACE_JCR, "xmltext"), null, null, null);
                ArrayList<PropInfo> props = new ArrayList<PropInfo>();
                props.add(new PropInfo(getJcrName(NamespaceRegistry.NAMESPACE_JCR, "xmlcharacters"),
                        PropertyType.STRING, textHandler));
                // call Importer
                importer.startNode(node, props);
                importer.endNode(node);

                // reset handler
                textHandler.dispose();
                textHandler = null;
            }
        } catch (IOException ioe) {
            String msg = "internal error while processing internal buffer data";
            log.error(msg, ioe);
            throw new SAXException(msg, ioe);
        } catch (RepositoryException re) {
            throw new SAXException(re);
        }
    }

    private String getJcrName(String uri, String name)
            throws RepositoryException {
        return sessionContext.getSession().getNamespacePrefix(uri) + ':' + name;
    }

    /**
     * Processes the given {@code name}, i.e. decodes it and checks
     * the format of the decoded name.
     *
     * @param nameInfo name to process
     * @return the decoded and valid jcr name or the original name if it is
     *         not encoded or if the resulting decoded name would be illegal.
     * @throws javax.jcr.RepositoryException
     */
    private NameInfo processName(NameInfo nameInfo) throws RepositoryException {
        String decodedLocalName = ISO9075.decode(nameInfo.getLocalName());
        NameInfo decodedNameInfo = new NameInfo(nameInfo.getDocPrefix(), decodedLocalName);
        if (!decodedLocalName.equals(nameInfo.getLocalName())) {
            try {
                JcrNameParser.checkName(decodedNameInfo.getRepoQualifiedName(), true);
            } catch (ConstraintViolationException e) {
                // decoded name would be illegal according to jsr 170,
                // use encoded name as a fallback
                log.warn("encountered illegal decoded name '" + decodedLocalName + '\'', e);
                return nameInfo;
            }
        }
        return decodedNameInfo;
    }

    //-------------------------------------------------------< ContentHandler >

    /**
     * {@inheritDoc}
     * <p>
     * See also {@link org.apache.jackrabbit.commons.xml.Exporter#exportProperties(Node)}
     * regarding special handling of multi-valued properties on export.
     */
    @Override
    public void startElement(String namespaceURI, String localName,
                             String qName, Attributes atts)
            throws SAXException {
        // process buffered character data
        processCharacters();

        try {
            NameInfo nameInfo = new NameInfo(qName);
            nameInfo = processName(nameInfo);

            // properties
            String id = null;
            String nodeTypeName = null;
            Iterable<String> mixinTypes = null;

            List<PropInfo> props = new ArrayList<PropInfo>(atts.getLength());
            for (int i = 0; i < atts.getLength(); i++) {
                if (atts.getURI(i).equals(NamespaceConstants.NAMESPACE_XMLNS)) {
                    // skip namespace declarations reported as attributes
                    // see http://issues.apache.org/jira/browse/JCR-620#action_12448164
                    continue;
                }

                NameInfo propNameInfo = processName(new NameInfo(atts.getQName(i)));
                String attrValue = atts.getValue(i);
                if (NamespaceRegistry.NAMESPACE_JCR.equals(propNameInfo.getNamespaceUri())
                        && "primaryType".equals(propNameInfo.getLocalName())) {
                    // jcr:primaryType
                    if (!attrValue.isEmpty()) {
                        //TODO
                        nodeTypeName = attrValue;
                    }
                } else if (NamespaceRegistry.NAMESPACE_JCR.equals(propNameInfo.getNamespaceUri())
                        && "mixinTypes".equals(propNameInfo.getLocalName())) {
                    // jcr:mixinTypes
                    mixinTypes = parseNames(attrValue);
                } else if (NamespaceRegistry.NAMESPACE_JCR.equals(propNameInfo.getNamespaceUri())
                        && "uuid".equals(propNameInfo.getLocalName())) {
                    // jcr:uuid
                    if (!attrValue.isEmpty()) {
                        id = attrValue;
                    }
                } else {
                    // always assume single-valued property for the time being
                    // until a way of properly serializing/detecting multi-valued
                    // properties on re-import is found (see JCR-325);
                    // see also DocViewSAXEventGenerator#leavingProperties(Node, int)
                    // TODO proper multi-value serialization support
                    TextValue tv = new StringValue(attrValue, sessionContext.getValueFactory(), currentNamePathMapper());
                    props.add(new PropInfo(propNameInfo.getRepoQualifiedName(), PropertyType.UNDEFINED, tv));
                }
            }

            NodeInfo node = new NodeInfo(nameInfo.getRepoQualifiedName(), nodeTypeName, mixinTypes, id);
            // all information has been collected, now delegate to importer
            importer.startNode(node, props);
            // push current node data onto stack
            stack.push(node);
        } catch (RepositoryException re) {
            throw new SAXException(re);
        }
    }

    @Override
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        /**
         * buffer data reported by the characters event;
         * will be processed on the next endElement or startElement event.
         */
        appendCharacters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length)
            throws SAXException {
        /**
         * buffer data reported by the ignorableWhitespace event;
         * will be processed on the next endElement or startElement event.
         */
        appendCharacters(ch, start, length);
    }

    @Override
    public void endElement(String namespaceURI, String localName, String qName)
            throws SAXException {
        // process buffered character data
        processCharacters();

        NodeInfo node = stack.peek();
        try {
            // call Importer
            importer.endNode(node);
        } catch (RepositoryException re) {
            throw new SAXException(re);
        }
        // we're done with this node, pop it from stack
        stack.pop();
    }
}
