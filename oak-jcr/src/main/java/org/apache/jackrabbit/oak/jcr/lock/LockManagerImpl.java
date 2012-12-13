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
package org.apache.jackrabbit.oak.jcr.lock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.lock.Lock;
import javax.jcr.lock.LockException;
import javax.jcr.lock.LockManager;

import org.apache.jackrabbit.oak.jcr.SessionDelegate;

/**
 * Simple lock manager implementation that just keeps track of a set of lock
 * tokens and delegates all locking operations back to the {@link Session}
 * and {@link Node} implementations.
 */
public class LockManagerImpl implements LockManager {

    private final SessionDelegate sessionDelegate;

    private final Set<String> tokens = new HashSet<String>();

    public LockManagerImpl(SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
    }

    @Override
    public String[] getLockTokens() throws RepositoryException {
        String[] array = tokens.toArray(new String[tokens.size()]);
        Arrays.sort(array);
        return array;
    }

    @Override
    public void addLockToken(String lockToken) throws RepositoryException {
        tokens.add(lockToken);
    }

    @Override
    public void removeLockToken(String lockToken) throws RepositoryException {
        if (!tokens.remove(lockToken)) {
            throw new LockException(
                    "Lock token " + lockToken + " is not held by this session");
        }
    }

    @Override
    public boolean isLocked(String absPath) throws RepositoryException {
        return getSession().getNode(absPath).isLocked();
    }

    @Override
    @SuppressWarnings("deprecation")
    public boolean holdsLock(String absPath) throws RepositoryException {
        return getSession().getNode(absPath).holdsLock();
    }

    @Override
    @SuppressWarnings("deprecation")
    public Lock getLock(String absPath) throws RepositoryException {
        return getSession().getNode(absPath).getLock();
    }

    @Override
    @SuppressWarnings("deprecation")
    public Lock lock(
            String absPath, boolean isDeep, boolean isSessionScoped,
            long timeoutHint, String ownerInfo) throws RepositoryException {
        return getSession().getNode(absPath).lock(isDeep, isSessionScoped);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void unlock(String absPath) throws RepositoryException {
        getSession().getNode(absPath).unlock();
    }

    private Session getSession() {
        return sessionDelegate.getSession();
    }
}