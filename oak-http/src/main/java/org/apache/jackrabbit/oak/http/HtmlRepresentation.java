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
package org.apache.jackrabbit.oak.http;

import java.io.IOException;
import java.net.URLEncoder;

import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.SAXException;

import com.google.common.base.Charsets;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

class HtmlRepresentation implements Representation {

    @Override
    public MediaType getType() {
        return MediaType.text("html");
    }

    public void render(Tree tree, HttpServletResponse response)
            throws IOException {
        try {
            XHTMLContentHandler xhtml = startResponse(response, tree.getPath());
            xhtml.startDocument();
            xhtml.startElement("dl");

            for (PropertyState property : tree.getProperties()) {
                xhtml.element("dt", property.getName());
                if (property.isArray()) {
                    xhtml.startElement("dd");
                    xhtml.startElement("ol");
                    for (String value : property.getValue(STRINGS)) {
                        xhtml.element("li", value);
                    }
                    xhtml.endElement("ol");
                    xhtml.endElement("dd");
                } else {
                    xhtml.element("dd", property.getValue(STRING));
                }
            }

            for (Tree child : tree.getChildren()) {
                String name = child.getName();
                xhtml.element("dt", name);
                xhtml.startElement("dd");
                xhtml.startElement("a", "href", response.encodeRedirectURL(
                        URLEncoder.encode(name, Charsets.UTF_8.name()) + "/"));
                xhtml.characters(child.getPath());
                xhtml.endElement("a");
                xhtml.endElement("dd");
            }

            xhtml.endElement("dl");
            xhtml.endDocument();
        } catch (SAXException e) {
            throw new IOException(e);
        }
    }

    public void render(PropertyState property, HttpServletResponse response)
            throws IOException {
        try {
            XHTMLContentHandler xhtml =
                    startResponse(response, property.getName());
            xhtml.startDocument();

            if (property.isArray()) {
                xhtml.startElement("ol");
                for (String value : property.getValue(STRINGS)) {
                    xhtml.element("li", value);
                }
                xhtml.endElement("ol");
            } else {
                xhtml.element("p", property.getValue(STRING));
            }

            xhtml.endDocument();
        } catch (SAXException e) {
            throw new IOException(e);
        }
    }

    private XHTMLContentHandler startResponse(
            HttpServletResponse response, String title)
            throws IOException {
        try {
            response.setContentType("text/html");
            response.setCharacterEncoding("UTF-8");

            SAXTransformerFactory factory =
                    (SAXTransformerFactory) SAXTransformerFactory.newInstance();
            TransformerHandler handler = factory.newTransformerHandler();
            Transformer transformer = handler.getTransformer();
            transformer.setOutputProperty(OutputKeys.METHOD, "html");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            handler.setResult(new StreamResult(response.getOutputStream()));

            Metadata metadata = new Metadata();
            metadata.set(Metadata.TITLE, title);
            return new XHTMLContentHandler(handler, metadata);
        } catch (TransformerConfigurationException e) {
            throw new IOException(e);
        }
    }

}
