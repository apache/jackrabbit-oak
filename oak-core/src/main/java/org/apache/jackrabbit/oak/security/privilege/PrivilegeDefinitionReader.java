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
package org.apache.jackrabbit.oak.security.privilege;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.PRIVILEGES_PATH;
import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.REP_AGGREGATES;
import static org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants.REP_IS_ABSTRACT;


/**
 * Reads privilege definitions without applying any validation.
 */
class PrivilegeDefinitionReader {

    private final Tree privilegesTree;

    PrivilegeDefinitionReader(Tree privilegesTree) {
        this.privilegesTree = privilegesTree;
    }

    PrivilegeDefinitionReader(ContentSession contentSession) {
        this(contentSession.getLatestRoot().getTree(PRIVILEGES_PATH));
    }

    Map<String, PrivilegeDefinition> readDefinitions() {
        Map<String, PrivilegeDefinition> definitions = new HashMap<String, PrivilegeDefinition>();
        if (privilegesTree != null) {
            for (Tree child : privilegesTree.getChildren()) {
                PrivilegeDefinition def = readDefinition(child);
                definitions.put(def.getName(), def);
            }
        }
        return definitions;
    }

    PrivilegeDefinition readDefinition(Tree definitionTree) {
        NodeUtil n = new NodeUtil(definitionTree);
        String name = n.getName();
        boolean isAbstract = n.getBoolean(REP_IS_ABSTRACT);
        String[] declAggrNames = n.getStrings(REP_AGGREGATES);

        return new PrivilegeDefinitionImpl(name, isAbstract, declAggrNames);
    }

    /**
     * Reads privilege definitions for the specified {@code InputStream}. The
     * aim of this method is to provide backwards compatibility with
     * custom privilege definitions of Jackrabbit 2.x repositories. The caller
     * is in charge of migrating the definitions.
     *
     * @param customPrivileges
     * @param nsRegistry
     * @return
     * @throws RepositoryException
     * @throws IOException
     */
    static PrivilegeDefinition[] readCustomDefinitons(InputStream customPrivileges,
                                                                 NamespaceRegistry nsRegistry) throws RepositoryException, IOException {
        Map<String, PrivilegeDefinition> definitions = new LinkedHashMap<String, PrivilegeDefinition>();
        InputSource src = new InputSource(customPrivileges);
        for (PrivilegeDefinition def : PrivilegeXmlHandler.readDefinitions(src, nsRegistry)) {
            String privName = def.getName();
            if (definitions.containsKey(privName)) {
                throw new RepositoryException("Duplicate entry for custom privilege with name " + privName.toString());
            }
            definitions.put(privName, def);
        }
        return definitions.values().toArray(new PrivilegeDefinition[definitions.size()]);
    }



    //--------------------------------------------------------------------------
    /**
     * The {@code PrivilegeXmlHandler} loads privilege definitions from a XML
     * document using the following format:
     * <pre>
     *  &lt;!DOCTYPE privileges [
     *  &lt;!ELEMENT privileges (privilege)+&gt;
     *  &lt;!ELEMENT privilege (contains)+&gt;
     *  &lt;!ATTLIST privilege abstract (true|false) false&gt;
     *  &lt;!ATTLIST privilege name NMTOKEN #REQUIRED&gt;
     *  &lt;!ELEMENT contains EMPTY&gt;
     *  &lt;!ATTLIST contains name NMTOKEN #REQUIRED&gt;
     * ]>
     * </pre>
     */
    private static class PrivilegeXmlHandler {

        private static final String TEXT_XML = "text/xml";
        private static final String APPLICATION_XML = "application/xml";

        private static final String XML_PRIVILEGES = "privileges";
        private static final String XML_PRIVILEGE = "privilege";
        private static final String XML_CONTAINS = "contains";

        private static final String ATTR_NAME = "name";
        private static final String ATTR_ABSTRACT = "abstract";

        private static final String ATTR_XMLNS = "xmlns:";

        private static DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = createFactory();

        private static DocumentBuilderFactory createFactory() {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setIgnoringComments(false);
            factory.setIgnoringElementContentWhitespace(true);
            return factory;
        }

        private static PrivilegeDefinition[] readDefinitions(InputSource input,
                                                             NamespaceRegistry nsRegistry) throws RepositoryException, IOException {
            try {
                List<PrivilegeDefinition> defs = new ArrayList<PrivilegeDefinition>();

                DocumentBuilder builder = createDocumentBuilder();
                Document doc = builder.parse(input);
                Element root = doc.getDocumentElement();
                if (!XML_PRIVILEGES.equals(root.getNodeName())) {
                    throw new IllegalArgumentException("root element must be named 'privileges'");
                }

                updateNamespaceMapping(root, nsRegistry);

                NodeList nl = root.getElementsByTagName(XML_PRIVILEGE);
                for (int i = 0; i < nl.getLength(); i++) {
                    Node n = nl.item(i);
                    PrivilegeDefinition def = parseDefinition(n, nsRegistry);
                    if (def != null) {
                        defs.add(def);
                    }
                }
                return defs.toArray(new PrivilegeDefinition[defs.size()]);

            } catch (SAXException e) {
                throw new RepositoryException(e);
            } catch (ParserConfigurationException e) {
                throw new RepositoryException(e);
            }
        }

        /**
         * Build a new {@code PrivilegeDefinition} from the given XML node.
         * @param n the xml node storing the privilege definition.
         * @param nsRegistry
         * @return a new PrivilegeDefinition.
         * @throws javax.jcr.RepositoryException
         */
        private static PrivilegeDefinition parseDefinition(Node n, NamespaceRegistry nsRegistry) throws RepositoryException {
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element elem = (Element) n;

                updateNamespaceMapping(elem, nsRegistry);

                String name = elem.getAttribute(ATTR_NAME);
                boolean isAbstract = Boolean.parseBoolean(elem.getAttribute(ATTR_ABSTRACT));

                Set<String> aggrNames = new HashSet<String>();
                NodeList nodeList = elem.getChildNodes();
                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node contains = nodeList.item(i);
                    if (isElement(n) && XML_CONTAINS.equals(contains.getNodeName())) {
                        String aggrName = ((Element) contains).getAttribute(ATTR_NAME);
                        if (aggrName != null) {
                            aggrNames.add(aggrName);
                        }
                    }
                }
                return new PrivilegeDefinitionImpl(name, isAbstract, aggrNames);
            }

            // could not parse into privilege definition
            return null;
        }

        /**
         * Create a new {@code DocumentBuilder}
         *
         * @return a new {@code DocumentBuilder}
         * @throws ParserConfigurationException
         */
        private static DocumentBuilder createDocumentBuilder() throws ParserConfigurationException {
            DocumentBuilder builder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
            builder.setErrorHandler(new DefaultHandler());
            return builder;
        }

        /**
         * Update the specified nsRegistry mappings with the nsRegistry declarations
         * defined by the given XML element.
         *
         * @param elem
         * @param nsRegistry
         * @throws javax.jcr.RepositoryException
         */
        private static void updateNamespaceMapping(Element elem, NamespaceRegistry nsRegistry) throws RepositoryException {
            NamedNodeMap attributes = elem.getAttributes();
            for (int i = 0; i < attributes.getLength(); i++) {
                Attr attr = (Attr) attributes.item(i);
                if (attr.getName().startsWith(ATTR_XMLNS)) {
                    String prefix = attr.getName().substring(ATTR_XMLNS.length());
                    String uri = attr.getValue();
                    nsRegistry.registerNamespace(prefix, uri);
                }
            }
        }

        /**
         * Returns {@code true} if the given XML node is an element.
         *
         * @param n
         * @return {@code true} if the given XML node is an element; {@code false} otherwise.
         */
        private static boolean isElement(Node n) {
            return n.getNodeType() == Node.ELEMENT_NODE;
        }
    }
}