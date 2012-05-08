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
package org.apache.jackrabbit.oak.jcr.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.predicate.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthorizableTypePredicate...
 */
class AuthorizableTypePredicate implements Predicate {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AuthorizableTypePredicate.class);

    private final int authorizableType;

    AuthorizableTypePredicate(int authorizableType) {
        this.authorizableType = authorizableType;
    }

    @Override
    public boolean evaluate(Object object) {
        if (object instanceof Authorizable) {
            Authorizable a = (Authorizable) object;
            switch (authorizableType) {
            case UserManager.SEARCH_TYPE_AUTHORIZABLE:
                return true;
            case UserManager.SEARCH_TYPE_GROUP:
                return a.isGroup();
            case UserManager.SEARCH_TYPE_USER:
                return !a.isGroup();
            default:
                log.warn("Illegal authorizable type " + authorizableType);
                return false;
        }
        }
        return false;
    }
}