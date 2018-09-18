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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.spi.xml.Importer;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * {@code TargetImportHandler} serves as the base class for the concrete
 * classes {@code {@link DocViewImportHandler}} and
 * {@code {@link SysViewImportHandler}}.
 */
public abstract class TargetImportHandler extends DefaultHandler {

    protected final Importer importer;
    protected final SessionContext sessionContext;
    private final ListMultimap<String, String> documentContext = ArrayListMultimap.create();
    private Map<String, String> documentPrefixMap = Collections.emptyMap();

    protected TargetImportHandler(Importer importer, SessionContext sessionContext) {
        this.importer = importer;
        this.sessionContext = sessionContext;
    }

    //--------------------------------------------------------< ImportHandler >

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        documentContext.put(prefix, uri);
        documentPrefixMap = createCurrentPrefixMap();
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        List<String> uris = documentContext.get(prefix);
        if (!uris.isEmpty()) {
            uris.remove(uris.size() - 1);
        }
        documentPrefixMap = createCurrentPrefixMap();
    }

    /**
     * Initializes the underlying {@link org.apache.jackrabbit.oak.spi.xml.Importer} instance. This method
     * is called by the XML parser when the XML document starts.
     *
     * @throws SAXException if the importer can not be initialized
     * @see DefaultHandler#startDocument()
     */
    @Override
    public void startDocument() throws SAXException {
        try {
            importer.start();
        } catch (RepositoryException re) {
            throw new SAXException(re);
        }
    }

    /**
     * Closes the underlying {@link org.apache.jackrabbit.oak.spi.xml.Importer} instance. This method
     * is called by the XML parser when the XML document ends.
     *
     * @throws SAXException if the importer can not be closed
     * @see DefaultHandler#endDocument()
     */
    @Override
    public void endDocument() throws SAXException {
        try {
            importer.end();
        } catch (RepositoryException re) {
            throw new SAXException(re);
        }
    }


    //--------------------------------------------------------

    public NamePathMapper currentNamePathMapper() {
        return new NamePathMapperImpl(new LocalNameMapper(
                sessionContext.getSessionDelegate().getRoot(),
                documentPrefixMap));
    }

    private Map<String, String> createCurrentPrefixMap() {
        Map<String, String> result = new HashMap<String, String>();
        Set<Map.Entry<String, Collection<String>>> entries = documentContext.asMap().entrySet();
        for (Map.Entry<String, Collection<String>> entry : entries) {
            String key = entry.getKey();
            List<String> value = (List<String>) entry.getValue();
            if (value != null && !value.isEmpty()) {
                result.put(key, value.get(value.size() - 1));
            }
        }
        return result;
    }


    protected class NameInfo {

        private final String localName;
        private final String docPrefix;
        private String namespaceUri;
        private String repoPrefix;

        NameInfo(String docQualifiedName) throws RepositoryException {
            int idx = docQualifiedName.indexOf(':');
            if (idx == -1) {
                docPrefix = null;
                localName = docQualifiedName;
            } else {
                String[] splits = docQualifiedName.split(":", 2);
                docPrefix = splits[0];
                localName = splits[1];
            }
            init();
        }

        NameInfo(String docPrefix, String localName) throws RepositoryException {
            this.localName = localName;
            this.docPrefix = docPrefix;
            init();
        }

        private void init() throws RepositoryException {
            if (docPrefix == null) {
                namespaceUri = "";
                repoPrefix = null;
            } else {
                List<String> uris = documentContext.get(docPrefix);
                Tree tree = sessionContext.getSessionDelegate().getRoot().getTree("/");
                if (uris.isEmpty()) {
                    namespaceUri = Namespaces.getNamespaceURI(tree, docPrefix);
                    repoPrefix = docPrefix;
                } else {
                    namespaceUri = uris.get(uris.size() - 1);
                    repoPrefix = Namespaces.getNamespacePrefix(tree, namespaceUri);
                }
            }
        }

        String getLocalName() {
            return localName;
        }

        String getNamespaceUri() {
            return namespaceUri;
        }

        String getDocPrefix() {
            return docPrefix;
        }

        String getRepoQualifiedName() {
            if (repoPrefix == null || repoPrefix.isEmpty()) {
                return localName;
            } else {
                return repoPrefix + ':' + localName;
            }
        }
    }
}
