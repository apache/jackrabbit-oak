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

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;

/**
 * Base class for {@link UserProvider} and {@link MembershipProvider}.
 */
abstract class AuthorizableBaseProvider implements UserConstants {

    final ConfigurationParameters config;
    final Root root;
    final IdentifierManager identifierManager;

    AuthorizableBaseProvider(Root root, ConfigurationParameters config) {
        this.root = root;
        this.config = config;
        this.identifierManager = new IdentifierManager(root);
    }

    Tree getByID(String authorizableId, AuthorizableType authorizableType) {
        Tree tree = identifierManager.getTree(getContentID(authorizableId));
        if (UserUtility.isType(tree, authorizableType)) {
            return tree;
        } else {
            return null;
        }
    }

    Tree getByPath(String authorizableOakPath) {
        Tree tree = root.getTree(authorizableOakPath);
        if (UserUtility.isType(tree, AuthorizableType.AUTHORIZABLE)) {
            return tree;
        } else {
            return null;
        }
    }

    String getContentID(Tree authorizableTree) {
        return identifierManager.getIdentifier(authorizableTree);
    }

    static String getContentID(String authorizableId) {
        return IdentifierManager.generateUUID(authorizableId.toLowerCase());
    }
}