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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.util.Collection;
import java.util.Iterator;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.RangeIteratorDecorator;

/**
 * Principal specific {@code RangeIteratorAdapter} implementing the
 * {@code PrincipalIterator} interface.
 */
public class PrincipalIteratorAdapter extends RangeIteratorDecorator implements PrincipalIterator {

    /**
     * Static instance of an empty {@link PrincipalIterator}.
     */
    @SuppressWarnings("unchecked")
    public static final PrincipalIteratorAdapter EMPTY =
            new PrincipalIteratorAdapter((Iterator<? extends Principal>) RangeIteratorAdapter.EMPTY);

    /**
     * Creates an adapter for the given {@link java.util.Iterator} of principals.
     *
     * @param iterator iterator of {@link java.security.Principal}s
     */
    public PrincipalIteratorAdapter(Iterator<? extends Principal> iterator) {
        super(new RangeIteratorAdapter(iterator));
    }

    /**
     * Creates an iterator for the given collection of {@code Principal}s.
     *
     * @param collection collection of {@link Principal} objects.
     */
    public PrincipalIteratorAdapter(Collection<? extends Principal> collection) {
        super(new RangeIteratorAdapter(collection));
    }

    //----------------------------------------< AccessControlPolicyIterator >---
    /**
     * Returns the next policy.
     *
     * @return next policy.
     */
    @Override
    public Principal nextPrincipal() {
        return (Principal) next();
    }
}