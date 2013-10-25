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

import java.util.Iterator;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;

class GroupImpl extends AuthorizableImpl implements Group {

    GroupImpl(Group dlg, AutoSaveEnabledManager mgr) {
        super(dlg, mgr);
    }

    private Group getDelegate() {
        return (Group) dlg;
    }

    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        return AuthorizableWrapper.createIterator(getDelegate().getDeclaredMembers(), mgr);
    }

    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        return AuthorizableWrapper.createIterator(getDelegate().getMembers(), mgr);
    }

    @Override
    public boolean isDeclaredMember(Authorizable authorizable) throws RepositoryException {
        if (isValid(authorizable)) {
            return getDelegate().isDeclaredMember(((AuthorizableImpl) authorizable).dlg);
        } else {
            return false;
        }
    }

    @Override
    public boolean isMember(Authorizable authorizable) throws RepositoryException {
        if (isValid(authorizable)) {
            return getDelegate().isMember(((AuthorizableImpl) authorizable).dlg);
        } else {
            return false;
        }
    }

    @Override
    public boolean addMember(Authorizable authorizable) throws RepositoryException {
        try {
            if (isValid(authorizable)) {
                return getDelegate().addMember(((AuthorizableImpl) authorizable).dlg);
            } else {
                return false;
            }
        } finally {
            mgr.autosave();
        }
    }

    @Override
    public boolean removeMember(Authorizable authorizable) throws RepositoryException {
        try {
            if (isValid(authorizable)) {
                return getDelegate().removeMember(((AuthorizableImpl) authorizable).dlg);
            } else {
                return false;
            }
        } finally {
            mgr.autosave();
        }
    }

    private boolean isValid(Authorizable a) {
        return a instanceof AuthorizableImpl;
    }
}