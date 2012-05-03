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
import org.apache.jackrabbit.oak.commons.PathUtils;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import java.util.List;

public class PropertyDelegate extends ItemDelegate {

    private final SessionDelegate sessionDelegate;
    private Tree parent;
    private PropertyState propertyState;

    PropertyDelegate(SessionDelegate sessionDelegate, Tree parent,
            PropertyState propertyState) {
        this.sessionDelegate = sessionDelegate;
        this.parent = parent;
        this.propertyState = propertyState;
    }

    PropertyDefinition getDefinition() throws RepositoryException {
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
    
    void remove() throws RepositoryException {
        getParentTree().removeProperty(getName());
    }
    
    void setValue(CoreValue value) throws RepositoryException {
        getParentTree().setProperty(getName(), value);
    }

    void setValues(List<CoreValue> values) throws RepositoryException {
        getParentTree().setProperty(getName(), values);
    }

    Tree getParentTree() {
        resolve();
        return parent;
    }

    PropertyState getPropertyState() {
        resolve();
        return propertyState;
    }

    Tree.Status getPropertyStatus() {
        return getParentTree().getPropertyStatus(getName());
    }
    
    @Override
    String getName() {
        return getPropertyState().getName();
    }

    @Override
    String getPath() {
        String parentPath = getParentTree().getPath();
        return parentPath.isEmpty() ? '/' + getName() : '/' + parentPath + '/' + getName();
    }

    SessionDelegate getSessionDelegate() {
        return sessionDelegate;
    }

    //------------------------------------------------------------< private >---

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
