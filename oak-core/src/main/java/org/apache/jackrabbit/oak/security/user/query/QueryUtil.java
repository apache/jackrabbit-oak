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
import org.apache.jackrabbit.oak.commons.QueryUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;

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
        String path = UserUtil.getAuthorizableRootPath(config, type);
        return QueryConstants.SEARCH_ROOT_PATH + path;
    }

    /**
     * Retrieve the node type name for the specified authorizable type.
     *
     * @param type The authorizable type.
     * @return The corresponding node type name.
     */
    @Nonnull
    public static String getNodeTypeName(@Nonnull AuthorizableType type) {
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
    public static String escapeNodeName(@Nonnull String string) {
        return QueryUtils.escapeNodeName(string);
    }

    @Nonnull
    public static String format(@Nonnull Value value) throws RepositoryException {
        switch (value.getType()) {
            case PropertyType.STRING:
            case PropertyType.BOOLEAN:
                return '\'' + QueryUtil.escapeForQuery(value.getString()) + '\'';

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
    public static String escapeForQuery(@Nonnull String oakName, @Nonnull NamePathMapper namePathMapper) {
        return escapeForQuery(namePathMapper.getJcrName(oakName));
    }

    @Nonnull
    public static String escapeForQuery(@Nonnull String value) {
        return QueryUtils.escapeForQuery(value);
    }

    @Nonnull
    public static RelationOp getCollation(@Nonnull QueryBuilder.Direction direction) throws RepositoryException {
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
