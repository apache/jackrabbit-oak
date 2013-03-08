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

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.commons.NamespaceHelper;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * An <code>ImportHandler</code> instance can be used to import serialized
 * data in System View XML or Document View XML. Processing of the XML is
 * handled by specialized <code>ContentHandler</code>s
 * (i.e. <code>SysViewImportHandler</code> and <code>DocViewImportHandler</code>).
 * <p/>
 * The actual task of importing though is delegated to the implementation of
 * the <code>{@link Importer}</code> interface.
 * <p/>
 * <b>Important Note:</b>
 * <p/>
 * These SAX Event Handlers expect that Namespace URI's and local names are
 * reported in the <code>start/endElement</code> events and that
 * <code>start/endPrefixMapping</code> events are reported
 * (i.e. default SAX2 Namespace processing).
 */
public class ImportHandler extends DefaultHandler {

    private static Logger log = LoggerFactory.getLogger(ImportHandler.class);

    private final Importer importer;
    private final NamespaceHelper helper;
    private final ValueFactory valueFactory;
    protected Locator locator;
    private TargetImportHandler targetHandler = null;
    private final Map<String, String> tempPrefixMap = new HashMap<String, String>();

    public ImportHandler(Node importTargetNode, Root root, Session session,
                         SessionDelegate dlg, UserConfiguration userConfig,
                         AccessControlConfiguration accessControlConfig,
                         int uuidBehavior)
            throws RepositoryException {
        this.helper = new NamespaceHelper(session);
        this.importer = new SessionImporter(importTargetNode, root, session, dlg, helper, userConfig, accessControlConfig, uuidBehavior);
        this.valueFactory = session.getValueFactory();
    }

    //---------------------------------------------------------< ErrorHandler >

    /**
     * {@inheritDoc}
     */
    @Override
    public void warning(SAXParseException e) throws SAXException {
        // log exception and carry on...
        log.warn("warning encountered at line: " + e.getLineNumber()
                + ", column: " + e.getColumnNumber()
                + " while parsing XML stream", e);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void error(SAXParseException e) throws SAXException {
        // log exception and carry on...
        log.error("error encountered at line: " + e.getLineNumber()
                + ", column: " + e.getColumnNumber()
                + " while parsing XML stream: " + e.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fatalError(SAXParseException e) throws SAXException {
        // log and re-throw exception
        log.error("fatal error encountered at line: " + e.getLineNumber()
                + ", column: " + e.getColumnNumber()
                + " while parsing XML stream: " + e.toString());
        throw e;
    }

    //-------------------------------------------------------< ContentHandler >

    /**
     * {@inheritDoc}
     */
    @Override
    public void endDocument() throws SAXException {
        // delegate to target handler
        if (targetHandler != null) {
            targetHandler.endDocument();
        }
    }

    /**
     * Records the given namespace mapping to be included in the local
     * namespace context. The local namespace context is instantiated
     * in {@link #startElement(String, String, String, Attributes)} using
     * all the the namespace mappings recorded for the current XML element.
     * <p/>
     * The namespace is also recorded in the persistent namespace registry
     * unless it is already known.
     *
     * @param prefix namespace prefix
     * @param uri    namespace URI
     */
    @Override
    public void startPrefixMapping(String prefix, String uri)
            throws SAXException {
        try {
            helper.registerNamespace(prefix, uri);
            if (targetHandler != null) {
                targetHandler.startPrefixMapping(prefix, uri);
            } else {
                tempPrefixMap.put(prefix, uri);
            }
        } catch (RepositoryException re) {
            throw new SAXException(re);
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (targetHandler != null) {
            targetHandler.endPrefixMapping(prefix);
        } else {
            tempPrefixMap.remove(prefix);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startElement(String namespaceURI, String localName, String qName,
                             Attributes atts) throws SAXException {
        if (targetHandler == null) {
            // the namespace of the first element determines the type of XML
            // (system view/document view)
            if (NamespaceConstants.NAMESPACE_SV.equals(namespaceURI)) {
                targetHandler = new SysViewImportHandler(importer, valueFactory, helper);
            } else {
                targetHandler = new DocViewImportHandler(importer, valueFactory, helper);
            }

            targetHandler.startDocument();

            for (Map.Entry<String, String> prefixMapping : tempPrefixMap.entrySet()) {
                targetHandler.startPrefixMapping(prefixMapping.getKey(), prefixMapping.getValue());
            }
        }

        // delegate to target handler
        targetHandler.startElement(namespaceURI, localName, qName, atts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        // delegate to target handler
        targetHandler.characters(ch, start, length);
    }

    /**
     * Delegates the call to the underlying target handler and asks the
     * handler to end the current namespace context.
     * {@inheritDoc}
     */
    @Override
    public void endElement(String namespaceURI, String localName, String qName)
            throws SAXException {
        targetHandler.endElement(namespaceURI, localName, qName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDocumentLocator(Locator locator) {
        this.locator = locator;
    }

}
