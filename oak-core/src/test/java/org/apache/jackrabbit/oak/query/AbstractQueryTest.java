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
package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.index.IndexWrapper;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;

import javax.jcr.GuestCredentials;

/**
 * AbstractQueryTest...
 */
public abstract class AbstractQueryTest {

    protected final IndexWrapper mk;
    protected final ContentRepositoryImpl rep;
    protected final CoreValueFactory vf;
    protected final SessionQueryEngine qe;
    protected final ContentSession session;

    {
        mk = new IndexWrapper(new MicroKernelImpl());
        rep = new ContentRepositoryImpl(mk, null, (ValidatorProvider) null);
        try {
            session = rep.login(new GuestCredentials(), "default");
            vf = session.getCoreValueFactory();
            qe = session.getQueryEngine();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}