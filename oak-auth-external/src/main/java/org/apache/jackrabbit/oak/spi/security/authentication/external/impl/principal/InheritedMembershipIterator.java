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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


class InheritedMembershipIterator extends AbstractLazyIterator<Group> {

    private static final Logger log = LoggerFactory.getLogger(InheritedMembershipIterator.class);
    
    private final Iterator<Group> groupIterator;
    private final List<Iterator<Group>> inherited = new ArrayList<>();
    private final Set<String> processed = new HashSet<>();
    private Iterator<Group> inheritedIterator = null;

    InheritedMembershipIterator(@NotNull Iterator<Group> groupIterator) {
        this.groupIterator = groupIterator;
    }

    @Nullable
    @Override
    protected Group getNext() {
        if (groupIterator.hasNext()) {
            Group gr = groupIterator.next();
            try {
                // call 'memberof' to cover nested inheritance
                Iterator<Group> it = gr.memberOf();
                if (it.hasNext()) {
                    inherited.add(it);
                }
            } catch (RepositoryException e) {
                log.error("Failed to retrieve membership of group {}", gr, e);
            }
            return gr;
        }

        if (inheritedIterator == null) {
            inheritedIterator = getNextInheritedIterator();
        }

        while (inheritedIterator.hasNext()) {
            Group gr = inheritedIterator.next();
            if (notProcessedBefore(gr)) {
                return gr;
            }
            if (!inheritedIterator.hasNext()) {
                inheritedIterator = getNextInheritedIterator();
            }
        } 
            
        // all inherited groups have been processed
        return null;
    }
    
    private boolean notProcessedBefore(@NotNull Group group) {
        try {
            return processed.add(group.getID()) && !EveryonePrincipal.NAME.equals(group.getPrincipal().getName());
        } catch (RepositoryException repositoryException) {
            return true;
        }
    }

    @NotNull
    private Iterator<Group> getNextInheritedIterator() {
        if (inherited.isEmpty()) {
            // no more inherited groups to retrieve
            return Collections.emptyIterator();
        } else {
            // no need to verify if the inherited iterator has any elements as this has been asserted before
            // adding it to the list.
            return inherited.remove(0);
        }
    }
}
