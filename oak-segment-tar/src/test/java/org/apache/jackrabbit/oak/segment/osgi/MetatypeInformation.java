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

package org.apache.jackrabbit.oak.segment.osgi;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class MetatypeInformation {

    static MetatypeInformation open(InputStream stream) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(stream);
        return new MetatypeInformation(document.getDocumentElement());
    }

    private static boolean hasAttribute(Element element, String name, String value) {
        return element.hasAttribute(name) && element.getAttribute(name).equals(value);
    }

    private final Element root;

    private MetatypeInformation(Element root) {
        this.root = root;
    }

    ObjectClassDefinition getObjectClassDefinition(String id) {
        return new ObjectClassDefinition(id);
    }

    class ObjectClassDefinition {

        private final String id;

        private ObjectClassDefinition(String id) {
            this.id = id;
        }

        HasAttributeDefinition hasAttributeDefinition(String id) {
            return new HasAttributeDefinition(this.id, id);
        }

    }

    class HasAttributeDefinition {

        private final String ocd;

        private final String id;

        private String type;

        private String defaultValue;

        private String cardinality;

        private String[] options;

        private HasAttributeDefinition(String ocd, String id) {
            this.ocd = ocd;
            this.id = id;
        }

        HasAttributeDefinition withStringType() {
            this.type = "String";
            return this;
        }

        HasAttributeDefinition withLongType() {
            this.type = "Long";
            return this;
        }

        HasAttributeDefinition withDoubleType() {
            this.type = "Double";
            return this;
        }

        HasAttributeDefinition withFloatType() {
            this.type = "Float";
            return this;
        }

        HasAttributeDefinition withIntegerType() {
            this.type = "Integer";
            return this;
        }

        HasAttributeDefinition withByteType() {
            this.type = "Byte";
            return this;
        }

        HasAttributeDefinition withCharType() {
            this.type = "Char";
            return this;
        }

        HasAttributeDefinition withBooleanType() {
            this.type = "Boolean";
            return this;
        }

        HasAttributeDefinition withShortType() {
            this.type = "Short";
            return this;
        }

        HasAttributeDefinition withDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        HasAttributeDefinition withCardinality(String cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        HasAttributeDefinition withOptions(String... options) {
            this.options = options;
            return this;
        }

        boolean check() {
            NodeList ocds = root.getElementsByTagName("OCD");
            for (int i = 0; i < ocds.getLength(); i++) {
                Element ocd = (Element) ocds.item(i);
                if (hasAttribute(ocd, "id", this.ocd)) {
                    return checkOCD(ocd);
                }
            }
            return false;
        }

        private boolean checkOCD(Element ocd) {
            NodeList ads = ocd.getElementsByTagName("AD");
            for (int i = 0; i < ads.getLength(); i++) {
                Element ad = (Element) ads.item(i);
                if (hasAttribute(ad, "id", id)) {
                    return checkAD(ad);
                }
            }
            return false;
        }

        private boolean checkAD(Element ad) {
            if (type != null && !hasAttribute(ad, "type", type)) {
                return false;
            }
            if (defaultValue != null && !hasAttribute(ad, "default", defaultValue)) {
                return false;
            }
            if (cardinality != null && !hasAttribute(ad, "cardinality", cardinality)) {
                return false;
            }
            if (options != null) {
                Set<String> optionValues = new HashSet<>();

                NodeList optionElements = ad.getElementsByTagName("Option");
                for (int i = 0; i < optionElements.getLength(); i++) {
                    Element optionElement = (Element) optionElements.item(i);
                    optionValues.add(optionElement.getAttribute("value"));
                }

                if (options.length != optionValues.size()) {
                    return false;
                }

                for (String option : options) {
                    if (!optionValues.contains(option)) {
                        return false;
                    }
                }
            }
            return true;
        }

    }

    HasDesignate hasDesignate() {
        return new HasDesignate();
    }

    class HasDesignate {

        private String pid;

        private String factoryPid;

        private String reference;

        private HasDesignate() {
            // Prevent instantiation outside of this class.
        }

        HasDesignate withPid(String pid) {
            this.pid = pid;
            return this;
        }

        HasDesignate withFactoryPid(String factoryPid) {
            this.factoryPid = factoryPid;
            return this;
        }

        HasDesignate withReference(String reference) {
            this.reference = reference;
            return this;
        }

        boolean check() {
            NodeList designates = root.getElementsByTagName("Designate");
            for (int i = 0; i < designates.getLength(); i++) {
                if (checkDesignate((Element) designates.item(i))) {
                    return true;
                }
            }
            return false;
        }

        private boolean checkDesignate(Element element) {
            if (pid != null && !hasAttribute(element, "pid", pid)) {
                return false;
            }
            if (factoryPid != null && !hasAttribute(element, "factoryPid", factoryPid)) {
                return false;
            }
            if (reference != null) {
                NodeList objects = element.getElementsByTagName("Object");
                for (int i = 0; i < objects.getLength(); i++) {
                    if (hasAttribute((Element) objects.item(i), "ocdref", reference)) {
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

    }

}
