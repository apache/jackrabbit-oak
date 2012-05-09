/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.query;

import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.RowIteratorAdapter;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.value.ValueFactoryImpl;

import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryResultImpl implements QueryResult {

    final SessionDelegate sessionDelegate;
    final ValueFactoryImpl valueFactory;
    final Result result;
    final String pathFilter;

    public QueryResultImpl(SessionDelegate sessionDelegate, Result result) {
        this.sessionDelegate = sessionDelegate;
        this.result = result;
        this.valueFactory = sessionDelegate.getValueFactory();

        // TODO the path currently contains the workspace name
        // TODO filter in oak-core once we support workspaces there
        pathFilter = "/" + sessionDelegate.getWorkspaceName();
    }

    @Override
    public String[] getColumnNames() throws RepositoryException {
        return result.getColumnNames();
    }

    @Override
    public String[] getSelectorNames() throws RepositoryException {
        return result.getSelectorNames();
    }

    @Override
    public RowIterator getRows() throws RepositoryException {
        Iterator<RowImpl> it = new Iterator<RowImpl>() {

            private final Iterator<? extends ResultRow> it = result.getRows().iterator();
            private RowImpl current;

            {
                fetch();
            }

            private void fetch() {
                current = null;
                while(it.hasNext()) {
                    ResultRow r = it.next();
                    String path = r.getPath();
                    if (PathUtils.isAncestor(pathFilter, path)) {
                        current = new RowImpl(r, valueFactory);
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public RowImpl next() {
                if (current == null) {
                    throw new NoSuchElementException();
                }
                RowImpl r = current;
                fetch();
                return r;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
        return new RowIteratorAdapter(it);
    }

    @Override
    public NodeIterator getNodes() throws RepositoryException {
        Iterator<NodeImpl> it = new Iterator<NodeImpl>() {

            private final Iterator<? extends ResultRow> it = result.getRows().iterator();
            private NodeImpl current;

            {
                fetch();
            }

            private void fetch() {
                current = null;
                while(it.hasNext()) {
                    ResultRow r = it.next();
                    String path = r.getPath();
                    if (PathUtils.isAncestor(pathFilter, path)) {
                        path = PathUtils.relativize(pathFilter, path);
                        NodeDelegate d = sessionDelegate.getNode(path);
                        current = new NodeImpl(d);
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public NodeImpl next() {
                if (current == null) {
                    throw new NoSuchElementException();
                }
                NodeImpl n = current;
                fetch();
                return n;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
        return new NodeIteratorAdapter(it);
    }

}
