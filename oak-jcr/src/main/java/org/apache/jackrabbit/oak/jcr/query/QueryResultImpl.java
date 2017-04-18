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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.CheckForNull;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.RowIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.jcr.query.PrefetchIterator.PrefetchOptions;
import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryResultImpl implements QueryResult {
    private static final Logger queryOpsLogger = LoggerFactory.getLogger("org.apache.jackrabbit.oak.jcr.operations.query");
    static final Logger LOG = LoggerFactory.getLogger(QueryResultImpl.class);

    protected final SessionContext sessionContext;
    
    final Result result;

    private final SessionDelegate sessionDelegate;

    public QueryResultImpl(SessionContext sessionContext, Result result) {
        this.sessionContext = sessionContext;
        this.sessionDelegate = sessionContext.getSessionDelegate();
        this.result = result;
    }

    @Override
    public String[] getColumnNames() throws RepositoryException {
        return result.getColumnNames();
    }

    @Override
    public String[] getSelectorNames() {
        return result.getSelectorNames();
    }

    @Override
    public RowIterator getRows() throws RepositoryException {
        Iterator<RowImpl> rowIterator = new Iterator<RowImpl>() {

            private final Iterator<? extends ResultRow> it = result.getRows().iterator();
            private final String pathSelector;
            private RowImpl current;
            private int rowCount;
            //Avoid log check for every row access
            private final boolean debugEnabled = queryOpsLogger.isDebugEnabled();

            {
                String[] columnSelectorNames = result.getColumnSelectorNames();
                if (columnSelectorNames.length == 1) {
                    pathSelector = columnSelectorNames[0];
                } else {
                    pathSelector = null;
                }
                fetch();
            }

            private void fetch() {
                if (it.hasNext()) {
                    current = new RowImpl(
                            QueryResultImpl.this, it.next(), pathSelector);
                    if (debugEnabled) {
                        rowCount++;
                        if (rowCount % 100 == 0) {
                            queryOpsLogger.debug("Iterated over [{}] results so far", rowCount);
                        }
                    }
                } else {
                    current = null;
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
        final PrefetchIterator<RowImpl> prefIt = new  PrefetchIterator<RowImpl>(
                sessionDelegate.sync(rowIterator),
                new PrefetchOptions() { {
                    size = result.getSize();
                    fastSize = sessionContext.getFastQueryResultSize();
                    fastSizeCallback = result;
                } });
        return new RowIteratorAdapter(prefIt) {
            @Override
            public long getSize() {
                return prefIt.size();
            }
        };
    }

    @CheckForNull
    NodeImpl<? extends NodeDelegate> getNode(Tree tree) throws RepositoryException {
        if (tree != null && tree.exists()) {
            NodeDelegate node = new NodeDelegate(sessionDelegate, tree);
            return NodeImpl.createNode(node, sessionContext);
        }
        return null;
    }

    @Override
    public NodeIterator getNodes() throws RepositoryException {
        String[] columnSelectorNames = result.getColumnSelectorNames();
        if (columnSelectorNames.length != 1) {
            throw new RepositoryException("Query contains more than one selector: " +
                    Arrays.toString(columnSelectorNames));
        }
        final String selectorName = columnSelectorNames[0];
        if (selectorName == null) {
            throw new RepositoryException("Query does not contain a selector: " +
                    Arrays.toString(columnSelectorNames));
        }
        Iterator<NodeImpl<? extends NodeDelegate>> nodeIterator = 
                new Iterator<NodeImpl<? extends NodeDelegate>>() {

            private final Iterator<? extends ResultRow> it = result.getRows().iterator();
            private NodeImpl<? extends NodeDelegate> current;

            {
                fetch();
            }

            private void fetch() {
                current = null;
                while (it.hasNext()) {
                    ResultRow r = it.next();
                    Tree tree = r.getTree(selectorName);
                    if (tree != null && tree.exists()) {
                        try {
                            current = getNode(tree);
                            break;
                        } catch (RepositoryException e) {
                            LOG.warn("Unable to fetch result node for path "
                                     + tree.getPath(), e);
                        }
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public NodeImpl<? extends NodeDelegate> next() {
                if (current == null) {
                    throw new NoSuchElementException();
                }
                NodeImpl<? extends NodeDelegate> n = current;
                fetch();
                return n;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
        final PrefetchIterator<NodeImpl<? extends NodeDelegate>> prefIt = 
                new  PrefetchIterator<NodeImpl<? extends NodeDelegate>>(
                    sessionDelegate.sync(nodeIterator),
                    new PrefetchOptions() { {
                        size = result.getSize();
                        fastSize = sessionContext.getFastQueryResultSize();
                        fastSizeCallback = result;
                    } });
        return new NodeIteratorAdapter(prefIt) {
            @Override
            public long getSize() {
                return prefIt.size();
            }
        };
    }

    Value createValue(PropertyValue value) {
        if (value == null) {
            return null;
        }
        return ValueFactoryImpl.createValue(value, sessionContext);
    }

}
