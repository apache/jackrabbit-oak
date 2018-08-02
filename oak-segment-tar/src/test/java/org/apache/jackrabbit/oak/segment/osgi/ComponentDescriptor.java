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

import javax.print.attribute.HashPrintServiceAttributeSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class ComponentDescriptor {

    public static ComponentDescriptor open(InputStream stream) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(stream);
        return new ComponentDescriptor(document.getDocumentElement());
    }

    private final Element root;

    private ComponentDescriptor(Element root) {
        this.root = root;
    }

    private static boolean hasAttribute(Element element, String name, String value) {
        return element.hasAttribute(name) && element.getAttribute(name).equals(value);
    }

    private static boolean hasNoAttribute(Element element, String name) {
        return !element.hasAttribute(name);
    }

    boolean hasName(String name) {
        return hasAttribute(root, "name", name);
    }

    boolean hasRequireConfigurationPolicy() {
        return hasAttribute(root, "configuration-policy", "require");
    }

    boolean hasActivateMethod(String name) {
        return hasAttribute(root, "activate", name);
    }

    boolean hasDeactivateMethod(String name) {
        return hasAttribute(root, "deactivate", name);
    }

    boolean hasConfigurationPid(String name) {
        return hasAttribute(root, "configuration-pid", name);
    }

    boolean hasImplementationClass(String value) {
        NodeList implementations = root.getElementsByTagName("implementation");
        if (implementations.getLength() == 0) {
            return false;
        }
        return hasImplementationClass((Element) implementations.item(0), value);
    }

    private static boolean hasImplementationClass(Element implementation, String value) {
        return hasAttribute(implementation, "class", value);
    }

    class HasProperty {

        private String name;

        private String type;

        private String value;

        private HasProperty(String name) {
            this.name = name;
        }

        HasProperty withStringType() {
            this.type = "String";
            return this;
        }

        HasProperty withLongType() {
            this.type = "Long";
            return this;
        }

        HasProperty withDoubleType() {
            this.type = "Double";
            return this;
        }

        HasProperty withFloatType() {
            this.type = "Float";
            return this;
        }

        HasProperty withIntegerType() {
            this.type = "Integer";
            return this;
        }

        HasProperty withByteType() {
            this.type = "Byte";
            return this;
        }

        HasProperty withCharacterType() {
            this.type = "Character";
            return this;
        }

        HasProperty withBooleanType() {
            this.type = "Boolean";
            return this;
        }

        HasProperty withShortType() {
            this.type = "Short";
            return this;
        }

        HasProperty withValue(String value) {
            this.value = value;
            return this;
        }

        boolean check() {
            NodeList properties = root.getElementsByTagName("property");
            for (int i = 0; i < properties.getLength(); i++) {
                Element property = (Element) properties.item(i);
                if (hasAttribute(property, "name", name)) {
                    if (type != null && !hasAttribute(property, "type", type)) {
                        return false;
                    }
                    if (value != null && !hasAttribute(property, "value", value)) {
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

    }

    HasProperty hasProperty(String name) {
        return new HasProperty(name);
    }

    class HasReference {

        private String name;

        private String iface;

        private String cardinality;

        private String policy;

        private String policyOption;

        private String target;

        private String bind;

        private String unbind;

        private String field;

        private HasReference(String name) {
            this.name = name;
        }

        HasReference withInterface(String iface) {
            this.iface = iface;
            return this;
        }

        HasReference withOptionalUnaryCardinality() {
            this.cardinality = "0..1";
            return this;
        }

        HasReference withMandatoryUnaryCardinality() {
            this.cardinality = "1..1";
            return this;
        }

        HasReference withOptionalMultipleCardinality() {
            this.cardinality = "0..n";
            return this;
        }

        HasReference withMandatoryMultipleCardinality() {
            this.cardinality = "1..n";
            return this;
        }

        HasReference withStaticPolicy() {
            this.policy = "static";
            return this;
        }

        HasReference withDynamicPolicy() {
            this.policy = "dynamic";
            return this;
        }

        HasReference withReluctantPolicyOption() {
            this.policyOption = "reluctant";
            return this;
        }

        HasReference withGreedyPolicyOption() {
            this.policyOption = "greedy";
            return this;
        }

        HasReference withTarget(String target) {
            this.target = target;
            return this;
        }

        HasReference withBind(String bind) {
            this.bind = bind;
            return this;
        }

        HasReference withUnbind(String unbind) {
            this.unbind = unbind;
            return this;
        }

        HasReference withField(String field) {
            this.field = field;
            return this;
        }

        boolean check() {
            NodeList references = root.getElementsByTagName("reference");
            for (int i = 0; i < references.getLength(); i++) {
                Element reference = (Element) references.item(i);
                if (hasAttribute(reference, "name", name)) {
                    if (iface != null && !hasAttribute(reference, "interface", iface)) {
                        return false;
                    }
                    if (cardinality != null && !hasValidCardinality(reference)) {
                        return false;
                    }
                    if (policy != null && !hasValidPolicy(reference)) {
                        return false;
                    }
                    if (policyOption != null && !hasAttribute(reference, "policy-option", policyOption)) {
                        return false;
                    }
                    if (target != null && !hasAttribute(reference, "target", target)) {
                        return false;
                    }
                    if (bind != null && !hasAttribute(reference, "bind", bind)) {
                        return false;
                    }
                    if (unbind != null && !hasAttribute(reference, "unbind", unbind)) {
                        return false;
                    }
                    if (field != null && !hasAttribute(reference, "field", field)) {
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

        private boolean hasValidCardinality(Element reference) {
            if (cardinality.equals("1..1") && hasNoAttribute(reference, "cardinality")) {
                return true;
            }
            return hasAttribute(reference, "cardinality", cardinality);
        }

        private boolean hasValidPolicy(Element reference) {
            if (policy.equals("static") && hasNoAttribute(reference, "policy")) {
                return true;
            }
            return hasAttribute(reference, "policy", policy);
        }

    }

    HasReference hasReference(String name) {
        return new HasReference(name);
    }

}
