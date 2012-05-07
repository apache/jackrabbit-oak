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
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.commons.PathUtils;

import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import java.util.List;

/**
 * {@code PropertyDelegate} serve as internal representations of {@code Property}s.
 * The methods of this class do not throw checked exceptions. Instead clients
 * are expected to inspect the return value and ensure that all preconditions
 * hold before a method is invoked. Specifically the behaviour of all methods
 * of this class but {@link #isStale()} is undefined if the instance is stale.
 * An item is stale if the underlying items does not exist anymore.
 */
public class PropertyDelegate extends ItemDelegate {

    /**
     * The underlying {@link Tree} of the parent node. In order to ensure the
     * instance is up to date, this field <em>should not be accessed directly</em>
     * but rather the {@link #getParentTree()} Tree()} method should be used.
     */
    private Tree parent;

    /**
     * The underlying {@link PropertyState}. In order to ensure the instance is up
     * to date, this field <em>should not be accessed directly</em> but rather the
     * {@link #getPropertyState()} method should be used.
     */
    private PropertyState propertyState;

    PropertyDelegate(SessionDelegate sessionDelegate, Tree parent, PropertyState propertyState) {
        super(sessionDelegate);
        this.parent = parent;
        this.propertyState = propertyState;
    }

    @Override
    public String getName() {
        return getPropertyState().getName();
    }

    @Override
    public String getPath() {
        return getParent().getPath() + '/' + getName();
    }

    @Override
    public NodeDelegate getParent() {
        return new NodeDelegate(sessionDelegate, getParentTree());
    }

    @Override
    public boolean isStale() {
        return getPropertyState() == null;
    }

    @Override
    public Status getStatus() {
        return getParentTree().getPropertyStatus(getName());
    }

    @Override
    public SessionDelegate getSessionDelegate() {
        return sessionDelegate;
    }

    /**
     * Get the value of the property
     * @return  value or {@code null} if multi values
     */
    public CoreValue getValue() {
        return getPropertyState().getValue();
    }

    /**
     * Get the value of the property
     * @return  value or {@code null} if single valued
     */
    public Iterable<CoreValue> getValues() {
        return getPropertyState().getValues();
    }

    /**
     * Determine whether the property is multi valued
     * @return  {@code true} if multi valued
     */
    public boolean isMultivalue() {
        return getPropertyState().isArray();
    }

    /**
     * Get the property definition of the property
     * @return
     */
    public PropertyDefinition getDefinition() {
        // TODO
        return new PropertyDefinition() {

            @Override
            public int getRequiredType() {
                return 0;
            }

            @Override
            public String[] getValueConstraints() {
                // TODO
                return new String[0];
            }

            @Override
            public Value[] getDefaultValues() {
                // TODO
                return new Value[0];
            }

            @Override
            public boolean isMultiple() {
                // TODO
                return getPropertyState().isArray();
            }

            @Override
            public String[] getAvailableQueryOperators() {
                // TODO
                return new String[0];
            }

            @Override
            public boolean isFullTextSearchable() {
                // TODO
                return false;
            }

            @Override
            public boolean isQueryOrderable() {
                // TODO
                return false;
            }

            @Override
            public NodeType getDeclaringNodeType() {
                // TODO
                return null;
            }

            @Override
            public String getName() {
                // TODO
                return getPropertyState().getName();
            }

            @Override
            public boolean isAutoCreated() {
                // TODO
                return false;
            }

            @Override
            public boolean isMandatory() {
                // TODO
                return false;
            }

            @Override
            public int getOnParentVersion() {
                // TODO
                return 0;
            }

            @Override
            public boolean isProtected() {
                // TODO
                return false;
            }
        };
    }

    /**
     * Set the value of the property
     * @param value
     */
    public void setValue(CoreValue value) {
        getParentTree().setProperty(getName(), value);
    }

    /**
     * Set the values of the property
     * @param values
     */
    public void setValues(List<CoreValue> values) {
        getParentTree().setProperty(getName(), values);
    }

    /**
     * Remove the property
     */
    public void remove() {
        getParentTree().removeProperty(getName());
    }

    //------------------------------------------------------------< private >---

    private PropertyState getPropertyState() {
        resolve();
        return propertyState;
    }

    private Tree getParentTree() {
        resolve();
        return parent;
    }

    private synchronized void resolve() {
        parent = sessionDelegate.getTree(parent.getPath());
        String path = PathUtils.concat(parent.getPath(), propertyState.getName());

        if (parent == null) {
            propertyState = null;
        } else {
            propertyState = parent.getProperty(PathUtils.getName(path));
        }
    }

}
