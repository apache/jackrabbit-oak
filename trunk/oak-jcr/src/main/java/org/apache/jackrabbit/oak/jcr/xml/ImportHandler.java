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

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.commons.NamespaceHelper;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.xml.Importer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * An {@code ImportHandler} instance can be used to import serialized
 * data in System View XML or Document View XML. Processing of the XML is
 * handled by specialized {@code ContentHandler}s
 * (i.e. {@code SysViewImportHandler} and {@code DocViewImportHandler}).
 * <p>
 * The actual task of importing though is delegated to the implementation of
 * the {@code {@link Importer}} interface.
 * <p>
 * <b>Important Note:</b>
 * <p>
 * These SAX Event Handlers expect that Namespace URI's and local names are
 * reported in the {@code start/endElement} events and that
 * {@code start/endPrefixMapping} events are reported
 * (i.e. default SAX2 Namespace processing).
 */
public class ImportHandler extends DefaultHandler {

    private static final Logger log = LoggerFactory.getLogger(ImportHandler.class);

    private final Root root;

    private final SessionContext sessionContext;
    private final Importer importer;
    private final boolean isWorkspaceImport;

    protected Locator locator;
    private TargetImportHandler targetHandler;
    private final Map<String, String> tempPrefixMap = new HashMap<String, String>();

    public ImportHandler(String absPath, SessionContext sessionContext,
                         int uuidBehavior, boolean isWorkspaceImport) throws RepositoryException {
        this.sessionContext = sessionContext;
        this.isWorkspaceImport = isWorkspaceImport;

        SessionDelegate sd = sessionContext.getSessionDelegate();
        root = (isWorkspaceImport) ? sd.getContentSession().getLatestRoot() : sd.getRoot();
        importer = new ImporterImpl(absPath, sessionContext, root, uuidBehavior, isWorkspaceImport);
    }

    //---------------------------------------------------------< ErrorHandler >

    @Override
    public void warning(SAXParseException e) throws SAXException {
        // log exception and carry on...
        log.warn("warning encountered at line: {}, column: {} while parsing XML stream", 
				e.getLineNumber(), e.getColumnNumber(), e);
    }

    @Override
    public void error(SAXParseException e) throws SAXException {
        // log exception and carry on...
        log.error("error encountered at line: {}, column: {} while parsing XML stream", 
				e.getLineNumber(), e.getColumnNumber(), e);
    }

    @Override
    public void fatalError(SAXParseException e) throws SAXException {
        // log and re-throw exception
        log.error("fatal error encountered at line: {}, column: {} while parsing XML stream",
				e.getLineNumber(), e.getColumnNumber(), e);
        throw e;
    }

    //-------------------------------------------------------< ContentHandler >

    @Override
    public void endDocument() throws SAXException {
        // delegate to target handler
        if (targetHandler != null) {
            targetHandler.endDocument();
        }
        if (isWorkspaceImport) {
            try {
                root.commit();
                sessionContext.getSession().refresh(false);
            } catch (CommitFailedException e) {
                throw new SAXException(e);
            } catch (RepositoryException e) {
                throw new SAXException(e);
            }
        }
    }

    /**
     * Records the given namespace mapping to be included in the local
     * namespace context. The local namespace context is instantiated
     * in {@link #startElement(String, String, String, Attributes)} using
     * all the the namespace mappings recorded for the current XML element.
     * <p>
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
            new NamespaceHelper(sessionContext.getSession()).registerNamespace(
                    prefix, uri);
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

    @Override
    public void startElement(String namespaceURI, String localName, String qName,
                             Attributes atts) throws SAXException {
        if (targetHandler == null) {
            // the namespace of the first element determines the type of XML
            // (system view/document view)
            if (NamespaceConstants.NAMESPACE_SV.equals(namespaceURI)) {
                targetHandler = new SysViewImportHandler(importer, sessionContext);
            } else {
                targetHandler = new DocViewImportHandler(importer, sessionContext);
            }

            targetHandler.startDocument();

            for (Map.Entry<String, String> prefixMapping : tempPrefixMap.entrySet()) {
                targetHandler.startPrefixMapping(prefixMapping.getKey(), prefixMapping.getValue());
            }
        }

        // delegate to target handler
        targetHandler.startElement(namespaceURI, localName, qName, atts);
    }

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

    @Override
    public void setDocumentLocator(Locator locator) {
        this.locator = locator;
    }

}
