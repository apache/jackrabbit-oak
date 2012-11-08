package org.apache.jackrabbit.mongomk.impl.model.tree;

import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.NodeDiffHandler;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.NodeStateDiff;
import org.apache.jackrabbit.mk.model.tree.NodeStore;
import org.apache.jackrabbit.mongomk.api.model.Node;

/**
 * This dummy NodeStore implementation is needed in order to be able to reuse
 * Oak's DiffBuilder in MongoMK.
 */
public class SimpleMongoNodeStore implements NodeStore {

    @Override
    public NodeState getRoot() {
        return null;
    }

    @Override
    public void compare(final NodeState before, final NodeState after,
            final NodeStateDiff diff) {

        Node beforeNode = ((MongoNodeState)before).unwrap();
        Node afterNode = ((MongoNodeState)after).unwrap();

        beforeNode.diff(afterNode, new NodeDiffHandler() {
            @Override
            public void propAdded(String propName, String value) {
                diff.propertyAdded(after.getProperty(propName));
            }

            @Override
            public void propChanged(String propName, String oldValue,
                    String newValue) {
                diff.propertyChanged(before.getProperty(propName),
                        after.getProperty(propName));
            }

            @Override
            public void propDeleted(String propName, String value) {
                diff.propertyDeleted(before.getProperty(propName));
            }

            @Override
            public void childNodeAdded(ChildNodeEntry added) {
                String name = added.getName();
                diff.childNodeAdded(name, after.getChildNode(name));
            }

            @Override
            public void childNodeDeleted(ChildNodeEntry deleted) {
                String name = deleted.getName();
                diff.childNodeDeleted(name, before.getChildNode(name));
            }

            @Override
            public void childNodeChanged(ChildNodeEntry changed, Id newId) {
                String name = changed.getName();
                diff.childNodeChanged(name, before.getChildNode(name),
                        after.getChildNode(name));
            }
        });
    }
}