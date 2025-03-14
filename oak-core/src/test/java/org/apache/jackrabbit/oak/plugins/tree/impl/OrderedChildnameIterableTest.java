package org.apache.jackrabbit.oak.plugins.tree.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class OrderedChildnameIterableTest {

    static final List<String> ALL_CHILDREN = List.of("1","2","3","4","5");

    List<String> iterableToList(Iterable<String> iter) {
        List<String> result = new ArrayList<>();
        iter.iterator().forEachRemaining(result::add);
        return result;
    }

    @Test
    public void noOrderedChildren() {
        // all children are returned in their order
        OrderedChildnameIterable iterable = new OrderedChildnameIterable(List.of(),ALL_CHILDREN);
        Assert.assertEquals(ALL_CHILDREN, iterableToList(iterable));
    }

    @Test
    public void orderedChildren() {
        // only 2 child nodes ordered, return them up front
        OrderedChildnameIterable iterable = new OrderedChildnameIterable(List.of("4","5"),ALL_CHILDREN);
        Assert.assertEquals(List.of("4","5","1","2","3"), iterableToList(iterable));
    }

    @Test
    public void orderedChildrenWithNonExistingOrderedChild() {
        // the ordered list contains a non-existing childname, which is not part of children list
        OrderedChildnameIterable iterable = new OrderedChildnameIterable(List.of("4","5","nonexisting"),ALL_CHILDREN);
        Assert.assertEquals(List.of("4","5","1","2","3"), iterableToList(iterable));
    }

    @Test
    public void onlyOrderedChildrenAvailable() {
        // the orderedChildren property is populated, but no children are available
        OrderedChildnameIterable iterable = new OrderedChildnameIterable(List.of("1","2"),List.of());
        Assert.assertEquals(List.of(), iterableToList(iterable));
    }

}
