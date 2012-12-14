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

import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResultRowToAuthorizable... TODO
 */
public class ResultRowToAuthorizable implements Function<ResultRow, Authorizable> {

    private static final Logger log = LoggerFactory.getLogger(ResultRowToAuthorizable.class);

    private final UserManager userManager;

    public ResultRowToAuthorizable(UserManager userManager) {
        this.userManager = userManager;
    }

    @Override
    public Authorizable apply(ResultRow row) {
        if (row != null) {
            try {
                return userManager.getAuthorizableByPath(row.getPath());
            } catch (RepositoryException e) {
                log.debug("Failed to access authorizable " + row.getPath());
            }
        }
        return null;
    }
}