package org.apache.jackrabbit.oak.plugins.tree.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *  Return the childrenNames in the order defined by the orderedChildren iterator, and merges it
 *  with the existing children defined by allChildren.
 *  
 *  This implementation focuses on being as lazy as possible; especially consuming the
 *  allChildren iterator can be slow.
 */

public class OrderedChildnameIterable implements Iterable<String> {

    final OrderedChildnameIterator iter;

    public OrderedChildnameIterable (Iterable<String> orderedChildren, Iterable<String> allChildren) {
        iter = new OrderedChildnameIterator(orderedChildren,allChildren);
    }

    @Override
    public Iterator<String> iterator() {
        return iter;
    }

    public class OrderedChildnameIterator implements Iterator<String> {

        final Iterator<String> orderedChildren;
        final Iterator<String> allChildren;

        private String nextResult;

        // lazily populated by elements from the allChildren iterable
        private final Set<String> allChildrenSet = new HashSet<>();

        private final List<String> nonOrderedChildren = new ArrayList<>();
        private Iterator<String> nonOrderedChildrenIterator = null;

        public OrderedChildnameIterator (Iterable<String> orderedChildren, Iterable<String> allChildren) {
            this.orderedChildren = orderedChildren.iterator();
            this.allChildren = allChildren.iterator();
            nextResult = getNextElement();
        }

        String getNextElement() {
            String elem = null;

            if (orderedChildren.hasNext()) {
                elem = getNextOrderedChild();
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
            // return all children which have not been consumed from the allChildren iterator yet
            if (allChildren.hasNext()) {
                return allChildren.next();
            }
            // all iterators consumed, no children anymore
            return null;
        }

        /**
         * Consume the next element from the orderedChild list
         * @return null if no ordered child can be retrieved, otherwise the next ordered child name
         */
        String getNextOrderedChild() {
            String current = null;
            // check that this element is actually present in the allChildren iterable
            while (current == null && orderedChildren.hasNext()) {
                current = orderedChildren.next();
                if (isOrderedChildPresent(current)) {
                    return current;
                }
            }
            return null;
        }

        boolean isOrderedChildPresent(String orderedChildName) {
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
}
