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

package org.apache.jackrabbit.j2ee;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.security.auth.Subject;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singleton;

/**
 * IndexInitializer configures the repository with required fulltext index
 *
 */
public class IndexInitializer {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Repository repository;

    public IndexInitializer(Repository repository) {
        this.repository = repository;
    }

    public void initialize() throws RepositoryException{
        Session s = createAdministrativeSession();
        try{
            if (!s.nodeExists("/oak:index/lucene")){
                createFullTextIndex(s);
            }
            s.save();
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    private void createFullTextIndex(Session s) throws RepositoryException {
        String indexPath = "/oak:index/lucene";
        Node lucene = JcrUtils.getOrCreateByPath(indexPath, JcrConstants.NT_UNSTRUCTURED,
                "oak:QueryIndexDefinition", s, false);
        lucene.setProperty("async", "async");
        lucene.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "lucene");
        lucene.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        lucene.setProperty(LuceneIndexConstants.INDEX_PATH, indexPath);
        lucene.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());

        Node indexRules = lucene.addNode(LuceneIndexConstants.INDEX_RULES, JcrConstants.NT_UNSTRUCTURED);
        Node ntBaseRule = indexRules.addNode(JcrConstants.NT_BASE);

        //Fulltext index only includes property of type String and Binary
        ntBaseRule.setProperty(LuceneIndexConstants.INCLUDE_PROPERTY_TYPES,
                new String[] {PropertyType.TYPENAME_BINARY, PropertyType.TYPENAME_STRING});

        Node propNode = ntBaseRule.addNode(LuceneIndexConstants.PROP_NODE);

        Node allPropNode = propNode.addNode("allProps");
        allPropNode.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        allPropNode.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        allPropNode.setProperty(LuceneIndexConstants.PROP_NAME, LuceneIndexConstants.REGEX_ALL_PROPS);
        allPropNode.setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);
        allPropNode.setProperty(LuceneIndexConstants.PROP_USE_IN_SPELLCHECK, true);

        //Create aggregates for nt:file
        Node aggNode = lucene.addNode(LuceneIndexConstants.AGGREGATES);

        Node aggFile = aggNode.addNode(JcrConstants.NT_FILE);
        aggFile.addNode("include0").setProperty(LuceneIndexConstants.AGG_PATH, JcrConstants.JCR_CONTENT);

        log.info("Created fulltext index definition at {}", indexPath);
    }

    private Session createAdministrativeSession() throws RepositoryException {
        //Admin ID here can be any string and need not match the actual admin userId
        final String adminId = "admin";
        Principal admin = new AdminPrincipal() {
            @Override
            public String getName() {
                return adminId;
            }
        };
        AuthInfo authInfo = new AuthInfoImpl(adminId, null, singleton(admin));
        Subject subject = new Subject(true, singleton(admin), singleton(authInfo), Collections.emptySet());
        Session adminSession;
        try {
            adminSession = Subject.doAsPrivileged(subject, new PrivilegedExceptionAction<Session>() {
                @Override
                public Session run() throws Exception {
                    return repository.login();
                }
            }, null);
        } catch (PrivilegedActionException e) {
            throw new RepositoryException("failed to retrieve admin session.", e);
        }

        return adminSession;
    }
}
