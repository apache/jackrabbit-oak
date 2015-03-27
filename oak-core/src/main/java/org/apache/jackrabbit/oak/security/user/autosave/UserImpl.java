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
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;

class UserImpl extends AuthorizableImpl implements User {

    UserImpl(User dlg, AutoSaveEnabledManager mgr) {
        super(dlg, mgr);
    }

    private User getDelegate() {
        return (User) getDlg();
    }

    @Override
    public boolean isAdmin() {
        return getDelegate().isAdmin();
    }

    @Override
    public boolean isSystemUser() {
        return getDelegate().isSystemUser();
    }

    @Override
    public Credentials getCredentials() throws RepositoryException {
        return getDelegate().getCredentials();
    }

    @Override
    public Impersonation getImpersonation() throws RepositoryException {
        return new ImpersonationImpl(getDelegate().getImpersonation());
    }

    @Override
    public void changePassword(String pw) throws RepositoryException {
        try {
            getDelegate().changePassword(pw);
        } finally {
            getMgr().autosave();
        }

    }

    @Override
    public void changePassword(String pw, String oldPw) throws RepositoryException {
        try {
            getDelegate().changePassword(pw, oldPw);
        } finally {
            getMgr().autosave();
        }
    }

    @Override
    public void disable(String msg) throws RepositoryException {
        try {
            getDelegate().disable(msg);
        } finally {
            getMgr().autosave();
        }
    }

    @Override
    public boolean isDisabled() throws RepositoryException {
        return getDelegate().isDisabled();
    }

    @Override
    public String getDisabledReason() throws RepositoryException {
        return getDelegate().getDisabledReason();
    }

    private final class ImpersonationImpl implements Impersonation {

        private final Impersonation dlg;

        private ImpersonationImpl(Impersonation dlg) {
            this.dlg = dlg;
        }
        @Override
        public PrincipalIterator getImpersonators() throws RepositoryException {
            return dlg.getImpersonators();
        }

        @Override
        public boolean grantImpersonation(Principal principal) throws RepositoryException {
            try {
                return dlg.grantImpersonation(principal);
            } finally {
                getMgr().autosave();
            }
        }

        @Override
        public boolean revokeImpersonation(Principal principal) throws RepositoryException {
            try {
                return dlg.revokeImpersonation(principal);
            } finally {
                getMgr().autosave();
            }
        }

        @Override
        public boolean allows(Subject subject) throws RepositoryException {
            return dlg.allows(subject);
        }
    }
}