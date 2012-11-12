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

import java.util.ArrayList;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XmlImportHandler extends DefaultHandler {

    private static final Logger log = LoggerFactory.getLogger(XmlImportHandler.class);

    private Node node;

    private String nodeName;

    private String propertyName;

    private final List<String> mixinNames = new ArrayList<String>();

    private final List<String> values = new ArrayList<String>();

    private final StringBuilder builder = new StringBuilder();

    public XmlImportHandler(Node parent, int uuidBehavior) {
        this.node = parent;
    }

    @Override
    public void startElement(
            String uri, String localName, String qName,
            Attributes atts) throws SAXException {
        if ("http://www.jcp.org/jcr/sv/1.0".equals(uri)) {
            String value = atts.getValue("sv:name");
            if ("node".equals(localName)) {
                // create node on jcr:primaryType sv:property
                nodeName = value;
            } else if (value != null) {
                propertyName = value;
            }
            builder.setLength(0);
        } else {
            for (int i = 0; i < atts.getLength(); i++) {
                try {
                    node.setProperty(atts.getQName(i), atts.getValue(i));
                } catch (RepositoryException e) {
                    throw new SAXException(e);
                }
            }
        }
    }

    @Override
    public void endElement(
            String uri, String localName, String qName)
            throws SAXException {
        if (!"http://www.jcp.org/jcr/sv/1.0".equals(uri)) {
            return;
        } else if ("value".equals(localName)) {
            values.add(builder.toString());
        } else if ("property".equals(localName)) {
            try {
                if (values.size() == 1) {
                    if (propertyName.equals("jcr:primaryType")) {
                        try {
                            log.debug(node.getPath() + " adding child " + nodeName + " with jcr:primaryType " + values.get(0));
                            node = node.addNode(nodeName, values.get(0));
                            for (String mixin : mixinNames) {
                                node.addMixin(mixin);
                            }
                            mixinNames.clear();
                        } catch (RepositoryException e) {
                            throw new SAXException(e);
                        }
                    } else if (propertyName.equals("jcr:mixinTypes")) {
                        if (node.getName().equals(nodeName)) {
                            log.debug(node.getPath() + " adding mixinType " + values.get(0));
                            node.addMixin(values.get(0));
                        } else {
                            // remember mixin and add when jcr:primaryType is known
                            mixinNames.addAll(values);
                        }
                    } else {
                        node.setProperty(propertyName, values.get(0));
                    }
                } else if (propertyName.equals("jcr:mixinTypes")) {
                    if (node.getName().equals(nodeName)) {
                        for (String value : values) {
                            log.debug(node.getPath() + " adding mixinType " + value);
                            node.addMixin(value);
                        }
                    } else {
                        // remember mixin and add when jcr:primaryType is known
                        mixinNames.addAll(values);
                    }
                } else {
                    node.setProperty(propertyName, values.toArray(new String[values.size()]));
                }
            } catch (RepositoryException e) {
                throw new SAXException(e);
            }
            values.clear();
        } else if ("node".equals(localName)) {
            try {
                // any mixins we missed?
                for (String mixin : mixinNames) {
                    node.addMixin(mixin);
                }
                mixinNames.clear();
                node = node.getParent();
            } catch (RepositoryException e) {
                throw new SAXException(e);
            }
        }
    }

    @Override
    public void characters(char[] ch, int offset, int length) {
        builder.append(ch, offset, length);
    }

}