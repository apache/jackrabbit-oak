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
package org.apache.jackrabbit.oak.security.user.query;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.apache.jackrabbit.util.Text;

/**
 * Common utilities used for user/group queries.
 */
public final class QueryUtil {

    private QueryUtil() {}

    /**
     * Determine the search root for the given authorizable type based on the
     * configured root path.
     *
     * @param type The authorizable type.
     * @param config The configuration parameters.
     * @return The path of search root for the specified authorizable type.
     */
    @Nonnull
    public static String getSearchRoot(AuthorizableType type, ConfigurationParameters config) {
        String path;
        if (type == AuthorizableType.USER) {
            path = UserUtility.getAuthorizableRootPath(config, AuthorizableType.USER);
        } else if (type == AuthorizableType.GROUP) {
            path = UserUtility.getAuthorizableRootPath(config, AuthorizableType.GROUP);
        } else {
            path = UserUtility.getAuthorizableRootPath(config, AuthorizableType.AUTHORIZABLE);
        }
        StringBuilder searchRoot = new StringBuilder();
        searchRoot.append("/jcr:root").append(path);
        return searchRoot.toString();
    }

    /**
     * Retrieve the node type name for the specified authorizable type.
     *
     * @param type The authorizable type.
     * @return The corresponding node type name.
     */
    @Nonnull
    public static String getNodeTypeName(AuthorizableType type) {
        if (type == AuthorizableType.USER) {
            return UserConstants.NT_REP_USER;
        } else if (type == AuthorizableType.GROUP) {
            return UserConstants.NT_REP_GROUP;
        } else {
            return UserConstants.NT_REP_AUTHORIZABLE;
        }
    }

    /**
     * Escape {@code string} for matching in jcr escaped node names
     *
     * @param string string to escape
     * @return escaped string
     */
    @Nonnull
    public static String escapeNodeName(String string) {
        StringBuilder result = new StringBuilder();

        int k = 0;
        int j;
        do {
            j = string.indexOf('%', k);
            if (j < 0) {
                // jcr escape trail
                result.append(Text.escapeIllegalJcrChars(string.substring(k)));
            } else if (j > 0 && string.charAt(j - 1) == '\\') {
                // literal occurrence of % -> jcr escape
                result.append(Text.escapeIllegalJcrChars(string.substring(k, j) + '%'));
            } else {
                // wildcard occurrence of % -> jcr escape all but %
                result.append(Text.escapeIllegalJcrChars(string.substring(k, j))).append('%');
            }

            k = j + 1;
        } while (j >= 0);

        return result.toString();
    }

    @Nonnull
    public static String format(Value value) throws RepositoryException {
        switch (value.getType()) {
            case PropertyType.STRING:
            case PropertyType.BOOLEAN:
                return '\'' + value.getString() + '\'';

            case PropertyType.LONG:
            case PropertyType.DOUBLE:
                return value.getString();

            case PropertyType.DATE:
                return "xs:dateTime('" + value.getString() + "')";

            default:
                throw new RepositoryException("Property of type " + PropertyType.nameFromValue(value.getType()) + " not supported");
        }
    }

    @Nonnull
    public static String escapeForQuery(String value) {
        StringBuilder ret = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                ret.append("\\\\");
            } else if (c == '\'') {
                ret.append("''");
            } else {
                ret.append(c);
            }
        }
        return ret.toString();
    }

    @Nonnull
    public static RelationOp getCollation(QueryBuilder.Direction direction) throws RepositoryException {
        switch (direction) {
            case ASCENDING:
                return RelationOp.GT;
            case DESCENDING:
                return RelationOp.LT;
            default:
                throw new RepositoryException("Unknown sort order " + direction);
        }
    }
}
