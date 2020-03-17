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
package org.apache.jackrabbit.oak.security.user.autosave;

import java.security.Principal;
import java.util.Iterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;

class AuthorizableImpl implements Authorizable {

    private final Authorizable dlg;
    private final AutoSaveEnabledManager mgr;

    AuthorizableImpl(Authorizable dlg, AutoSaveEnabledManager mgr) {
        this.dlg = dlg;
        this.mgr = mgr;
    }

    Authorizable getDlg() {
        return dlg;
    }

    AutoSaveEnabledManager getMgr() {
        return mgr;
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public String getID() throws RepositoryException {
        return dlg.getID();
    }

    @Override
    public boolean isGroup() {
        return dlg.isGroup();
    }

    @Override
    public Principal getPrincipal() throws RepositoryException {
        return dlg.getPrincipal();
    }

    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return AuthorizableWrapper.createGroupIterator(dlg.declaredMemberOf(), mgr);
    }

    @Override
    public Iterator<Group> memberOf() throws RepositoryException {
        return AuthorizableWrapper.createGroupIterator(dlg.memberOf(), mgr);
    }

    @Override
    public void remove() throws RepositoryException {
        try {
            dlg.remove();
        } finally {
            mgr.autosave();
        }
    }

    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return dlg.getPropertyNames();
    }

    @Override
    public Iterator<String> getPropertyNames(String s) throws RepositoryException {
        return dlg.getPropertyNames(s);

    }

    @Override
    public boolean hasProperty(String s) throws RepositoryException {
        return dlg.hasProperty(s);
    }

    @Override
    public void setProperty(String s, Value value) throws RepositoryException {
        try {
            dlg.setProperty(s, value);
        } finally {
            mgr.autosave();
        }
    }

    @Override
    public void setProperty(String s, Value[] values) throws RepositoryException {
        try {
            dlg.setProperty(s, values);
        } finally {
            mgr.autosave();
        }
    }

    @Override
    public Value[] getProperty(String s) throws RepositoryException {
        return dlg.getProperty(s);
    }

    @Override
    public boolean removeProperty(String s) throws RepositoryException {
        try {
            return dlg.removeProperty(s);
        } finally {
            mgr.autosave();
        }
    }

    @Override
    public String getPath() throws RepositoryException {
        return dlg.getPath();
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return dlg.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof AuthorizableImpl) {
            return dlg.equals(((AuthorizableImpl) o).dlg);
        }
        return false;
    }

    @Override
    public String toString() {
        return dlg.toString();
    }
}