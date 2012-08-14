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
import org.apache.jackrabbit.commons.flat.PropertySequence;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * AuthorizableIterator...
 */
class AuthorizableIterator implements Iterator {

    private static final Logger log = LoggerFactory.getLogger(AuthorizableIterator.class);

    private final Iterator<?> nodeIds;
    private final AuthorizableTypePredicate predicate;
    private final UserManagerImpl userManager;
    private final long size;

    private Authorizable next;

    AuthorizableIterator(List<CoreValue> authorizableNodeIds, int authorizableType, UserManagerImpl userManager) {
        this(Arrays.asList(authorizableNodeIds).iterator(), authorizableType, userManager, authorizableNodeIds.size());
    }

    AuthorizableIterator(PropertySequence authorizableNodeIds, int authorizableType, UserManagerImpl userManager) {
        this(authorizableNodeIds.iterator(), authorizableType, userManager, -1);  // TODO calculate size here
    }

    AuthorizableIterator(PropertyIterator authorizableNodeIds, int authorizableType, UserManagerImpl userManager) {
        this(authorizableNodeIds, authorizableType, userManager, authorizableNodeIds.getSize());
    }

    private AuthorizableIterator(Iterator<?> nodeIds, int authorizableType,
                                 UserManagerImpl userManager, long size) {
        this.nodeIds = nodeIds;
        this.predicate = new AuthorizableTypePredicate(authorizableType);
        this.userManager = userManager;
        this.size = size;

        next = fetchNext();
    }

    //-----------------------------------------------------------< Iterator >---
    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Authorizable next() {
        if (next == null) {
            throw new NoSuchElementException();
        }

        Authorizable a = next;
        next = fetchNext();
        return a;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    //--------------------------------------------------------------------------
    long getSize() {
        return size;
    }

    private Authorizable fetchNext() {
        while (nodeIds.hasNext()) {
            try {
                String nid = getNodeId(nodeIds.next());
                Node n = userManager.getSession().getNodeByIdentifier(nid);
                Authorizable a = userManager.getAuthorizable(n);
                if (a != null && predicate.evaluate(a)) {
                    return a;
                }
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
            }
        }

        return null;
    }

    private static String getNodeId(Object o) throws RepositoryException {
        if (o instanceof CoreValue) {
            return ((CoreValue) o).getString();
        } else if (o instanceof Value) {
            return ((Value) o).getString();
        } else if (o instanceof Property) {
            return ((Property) o).getString();
        } else {
            return o.toString();
        }
    }
}