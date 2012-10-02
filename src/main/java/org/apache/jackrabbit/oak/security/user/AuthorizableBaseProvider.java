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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.security.user.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthorizableBaseProvider... TODO
 */
abstract class AuthorizableBaseProvider implements UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AuthorizableBaseProvider.class);

    final CoreValueFactory valueFactory;
    final Root root;
    final IdentifierManager identifierManager;

    AuthorizableBaseProvider(ContentSession contentSession, Root root, UserConfig config) {
        this.valueFactory = contentSession.getCoreValueFactory();
        this.root = root;
        this.identifierManager = new IdentifierManager(root);
    }

    Tree getByID(String authorizableId, Type authorizableType) {
        Tree tree = identifierManager.getTree(getContentID(authorizableId));
        if (isAuthorizableTree(tree, authorizableType)) {
            return tree;
        } else {
            return null;
        }
    }

    Tree getByPath(String authorizableOakPath) {
        Tree tree = root.getTree(authorizableOakPath);
        if (isAuthorizableTree(tree, Type.AUTHORIZABLE)) {
            return tree;
        } else {
            return null;
        }
    }

    String getContentID(String authorizableId) {
        return IdentifierManager.generateUUID(authorizableId.toLowerCase());
    }

    String getContentID(Tree authorizableTree) {
        return identifierManager.getIdentifier(authorizableTree);
    }

    boolean isAuthorizableTree(Tree tree, Type authorizableType) {
        // FIXME: check for node type according to the specified type constraint
        if (tree != null && tree.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            String ntName = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue().getString();
            switch (authorizableType) {
                case GROUP:
                    return NT_REP_GROUP.equals(ntName);
                case USER:
                    return NT_REP_USER.equals(ntName);
                default:
                    return NT_REP_USER.equals(ntName) || NT_REP_GROUP.equals(ntName);
            }
        }
        return false;
    }
}