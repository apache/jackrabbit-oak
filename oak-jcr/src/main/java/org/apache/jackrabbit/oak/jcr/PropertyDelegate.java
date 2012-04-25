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

import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.Paths;

public class PropertyDelegate {

    private final SessionContext<SessionImpl> sessionContext;
    private Tree parent;
    private PropertyState propertyState;

    PropertyDelegate(SessionContext<SessionImpl> sessionContext, Tree parent,
            PropertyState propertyState) {
        this.sessionContext = sessionContext;
        this.parent = parent;
        this.propertyState = propertyState;
    }

    PropertyDefinition getDefinition() throws RepositoryException {
        // TODO
        return null;
    }
    
    void remove() throws RepositoryException {
        getParentContentTree().removeProperty(getName());
    }
    
    void setValue(CoreValue value) throws RepositoryException {
        getParentContentTree().setProperty(getName(), value);
    }

    void setValues(List<CoreValue> values) throws RepositoryException {
        getParentContentTree().setProperty(getName(), values);
    }

    Tree getParentContentTree() {
        resolve();
        return parent;
    }

    PropertyState getPropertyState() {
        resolve();
        return propertyState;
    }

    Tree.Status getPropertyStatus() {
        return getParentContentTree().getPropertyStatus(getName());
    }
    
    String getName() {
        return getPropertyState().getName();
    }

    String getPath() {
        return '/' + getParentContentTree().getPath() + '/' + getName();
    }

    //------------------------------------------------------------< private >---

    private Root getBranch() {
        return sessionContext.getBranch();
    }

    private synchronized void resolve() {
        parent = getBranch().getTree(parent.getPath());
        String path = Paths.concat(parent.getPath(), propertyState.getName());

        if (parent == null) {
            propertyState = null;
        }
        else {
            propertyState = parent.getProperty(Paths.getName(path));
        }
    }
}
