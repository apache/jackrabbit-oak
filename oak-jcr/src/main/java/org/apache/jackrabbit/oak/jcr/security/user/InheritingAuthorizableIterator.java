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
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.commons.iterator.FilterIterator;
import org.apache.jackrabbit.commons.iterator.LazyIteratorChain;
import org.apache.jackrabbit.commons.predicate.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * InheritingAuthorizableIterator...
 */
public class InheritingAuthorizableIterator<T extends Authorizable> extends FilterIterator {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(InheritingAuthorizableIterator.class);

    InheritingAuthorizableIterator(Iterator<Iterator<T>> inheritingIterator) {
        super(new LazyIteratorChain<T>(inheritingIterator), new ProcessedIdPredicate());
    }

    /**
     * Predicate to keep track of the IDs of those groups that have already
     * been processed.
     */
    private static final class ProcessedIdPredicate implements Predicate {

        private final HashSet<String> processedIds = new HashSet<String>();

        @Override
        public boolean evaluate(Object object) {
            if (object instanceof Group) {
                try {
                    return processedIds.add(((Group) object).getID());
                } catch (RepositoryException e) {
                    log.warn(e.getMessage());
                }
            }
            return false;
        }
    }
}