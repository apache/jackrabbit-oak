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

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XmlImportHandler extends DefaultHandler {

    private Node node;

    private String nodeName;

    private String name;

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
                name = value;
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
                    if (name.equals("jcr:primaryType")) {
                        try {
                            node = node.addNode(nodeName, values.get(0));
                        } catch (RepositoryException e) {
                            throw new SAXException(e);
                        }
                    } else if (name.equals("jcr:mixinTypes")) {
                        node.addMixin(values.get(0));
                    } else {
                        node.setProperty(name, values.get(0));
                    }
                } else if (name.equals("jcr:mixinTypes")) {
                    for (String value : values) {
                        node.addMixin(value);
                    }
                } else {
                    node.setProperty(name, values.toArray(new String[values.size()]));
                }
            } catch (RepositoryException e) {
                throw new SAXException(e);
            }
            values.clear();
        } else if ("node".equals(localName)) {
            try {
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