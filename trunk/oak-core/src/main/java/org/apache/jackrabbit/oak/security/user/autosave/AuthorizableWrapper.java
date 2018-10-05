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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;

final class AuthorizableWrapper<T extends Authorizable> implements Function<T, T> {

    private final AutoSaveEnabledManager mgr;

    private AuthorizableWrapper(AutoSaveEnabledManager mgr) {
        this.mgr = mgr;
    }

    @Override
    public T apply(T authorizable) {
        if (authorizable == null) {
            return null;
        } else {
            return (T) mgr.wrap(authorizable);
        }
    }

    static Iterator<Authorizable> createIterator(Iterator<Authorizable> dlgs, AutoSaveEnabledManager mgr) {
        return Iterators.transform(dlgs, new AuthorizableWrapper(mgr));
    }

    static Iterator<Group> createGroupIterator(Iterator<Group> dlgs, AutoSaveEnabledManager mgr) {
        return Iterators.transform(dlgs, new AuthorizableWrapper(mgr));
    }
}