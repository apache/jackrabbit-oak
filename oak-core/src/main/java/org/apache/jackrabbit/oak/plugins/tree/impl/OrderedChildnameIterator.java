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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *  Return the childrenNames in the order defined by the orderedChildren iterator, and merges it
 *  with the existing children defined by allChildren.
 *
 *  This implementation focuses on being as lazy as possible; especially consuming the
 *  allChildren iterator can be slow.
 */

public class OrderedChildnameIterator implements Iterator<String>{

    final Iterator<String> orderedChildren;
    final Iterator<String> allChildren;

    private String nextResult;

    private final List<String> nonOrderedChildren = new ArrayList<>();
    private Iterator<String> nonOrderedChildrenIterator = null;

    public OrderedChildnameIterator (Iterable<String> orderedChildren, Iterable<String> allChildren) {
        this.orderedChildren = orderedChildren == null ? Collections.emptyIterator() : orderedChildren.iterator();
        this.allChildren = allChildren.iterator();
        nextResult = getNextElement();
    }

    private String getNextElement() {
        if (orderedChildren.hasNext()) {
            String elem = getNextOrderedChild();
            if (elem != null) {
                return elem;
            }
        }
        // if the flow comes here, all orderedChildren have already been consumed, and the
        // nonOrderedChildren list is no longer changed, so it's safe to create the iterator here
        if (nonOrderedChildrenIterator == null) {
            nonOrderedChildrenIterator = nonOrderedChildren.iterator();
        }
        // return all children which have already been read into the nonOrderedChildren list
        if (nonOrderedChildrenIterator.hasNext()) {
            return nonOrderedChildrenIterator.next();
        }
        // return all children which have not been consumed from the allChildren iterator
        if (allChildren.hasNext()) {
            return allChildren.next();
        }
        // all iterators consumed, no children anymore
        return null;
    }

    /**
     * Consume the next element from the orderedChild list and validates that it's actually present
     * @return the next ordered child or {code null} if all ordered children have already been returned
     */
    private String getNextOrderedChild() {
        // check that this element is actually present in the allChildren iterable
        while (orderedChildren.hasNext()) {
            String current = orderedChildren.next();
            if (isOrderedChildPresent(current)) {
                return current;
            }
        }
        return null;
    }

    /**
     * Check if the provided childname is also provided by the allChildren iterator.
     * 
     * Note, that in the pth
     * 
     * 
     * @param orderedChildName
     * @return true if childname is a valid child, false otherwise
     */
    private boolean isOrderedChildPresent(String orderedChildName) {
        // read from the allChildren iterator until it's a hit or exhausted
        while (!nonOrderedChildren.contains(orderedChildName) && allChildren.hasNext()) {
            nonOrderedChildren.add(allChildren.next());
        }
        if (nonOrderedChildren.contains(orderedChildName)) {
            // remove it from the list, as it is returned early
            nonOrderedChildren.remove(orderedChildName);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean hasNext() {
        return nextResult != null;
    }

    @Override
    public String next() {
        String n = nextResult;
        nextResult = getNextElement();
        return n;
    }


}
