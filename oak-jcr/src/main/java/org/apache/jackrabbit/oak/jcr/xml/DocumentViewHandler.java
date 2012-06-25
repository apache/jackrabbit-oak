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

import java.util.HashMap;
import java.util.Map;

import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.commons.JcrUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class DocumentViewHandler extends DefaultHandler {

    private static class Context {

        private final Context parent;

        private final Node node;

        private final Map<String, String> namespaces =
                new HashMap<String, String>();

        private final StringBuilder text = new StringBuilder();

        public Context(Context parent, Node node) {
            this.parent = parent;
            this.node = node;
        }

        public String getNamespaceURI(String prefix)
                throws RepositoryException {
            String uri = namespaces.get(prefix);
            if (uri != null) {
                return uri;
            } else if (parent != null) {
                return parent.getNamespaceURI(prefix);
            } else {
                return node.getSession().getNamespacePrefix(uri);
            }
        }

        public String toJcrName(String qname)
                throws RepositoryException {
            if (qname != null) {
                int colon = qname.indexOf(':');
                if (colon != -1) {
                    String prefix = qname.substring(0, colon);
                    String local = qname.substring(colon + 1);
                    String uri = getNamespaceURI(prefix);
                    return toJcrName(uri, local, qname);
                } else {
                    return qname;
                }
            } else {
                return null;
            }
        }

        public String toJcrName(String uri, String local, String qname)
                throws RepositoryException {
            if (uri != null) {
                String prefix = node.getSession().getNamespacePrefix(uri);
                if (prefix.isEmpty()) {
                    return local;
                } else {
                    return prefix + ":" + local;
                }
            } else if (local != null) {
                return local;
            } else {
                return toJcrName(qname);
            }
        }

    }

    private Context context;

    public DocumentViewHandler(Node parent, int uuidBehavior) {
        this.context = new Context(null, parent);
    }

    @Override
    public void startElement(
            String uri, String localName, String qName,
            Attributes atts) throws SAXException {
        try {
            String name = context.toJcrName(uri, localName, qName);
            String type = context.toJcrName(atts.getValue(
                    NamespaceRegistry.NAMESPACE_JCR,
                    Property.JCR_PRIMARY_TYPE));
            Node node = JcrUtils.getOrAddNode(context.node, name, type);
            for (int i = 0; i < atts.getLength(); i++) {
                name = context.toJcrName(
                        atts.getURI(i), atts.getLocalName(i), atts.getQName(i));
                if (name.equals("jcr:primaryType")) {
                    type = context.toJcrName(atts.getValue(i));
                    if (!node.isNodeType(type)) {
                        node.setPrimaryType(type);
                    }
                } else if (name.equals("jcr:mixinTypes")) {
                    String[] types = atts.getValue(i).split("\\s+");
                    for (int j = 0; j < types.length; j++) {
                        type = context.toJcrName(types[i]);
                        if (!node.isNodeType(type)) {
                            node.addMixin(type);
                        }
                    }
                } else {
                    node.setProperty(name, atts.getValue(i));
                }
            }

            context = new Context(context, node);
        } catch (RepositoryException e) {
            throw new SAXException(e);
        }
    }

    @Override
    public void endElement(
            String uri, String localName, String qName)
            throws SAXException {
        try {
            if (context.text.length() > 0) {
                Node xmltext = JcrUtils.getOrAddNode(context.node, "jcr:xmltext");
                xmltext.setProperty("jcr:xmlcharacters", context.text.toString());
            }

            context = context.parent;
        } catch (RepositoryException e) {
            throw new SAXException(e);
        }
    }

    @Override
    public void characters(char[] ch, int offset, int length) {
        context.text.append(ch, offset, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int offset, int length) {
        context.text.append(ch, offset, length);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri)
            throws SAXException {
        context.namespaces.put(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        context.namespaces.remove(prefix);
    }

}