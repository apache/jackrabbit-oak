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

import java.util.Iterator;
import java.util.List;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A query result.
 */
public class ResultImpl implements Result {

    protected final Query query;
    protected final NodeState root;

    ResultImpl(Query query, NodeState root) {
        this.query = query;
        this.root = root;
    }

    @Override
    public String[] getColumnNames() {
        ColumnImpl[] cols = query.getColumns();
        String[] names = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            names[i] = cols[i].getColumnName();
        }
        return names;
    }

    @Override
    public String[] getSelectorNames() {
        List<SelectorImpl> selectors = query.getSelectors();
        String[] names = new String[selectors.size()];
        for (int i = 0; i < selectors.size(); i++) {
            names[i] = selectors.get(i).getSelectorName();
        }
        return names;
    }

    @Override
    public Iterable<? extends ResultRow> getRows() {
        return new Iterable<ResultRowImpl>() {

            @Override
            public Iterator<ResultRowImpl> iterator() {
                return query.getRows(root);
            }

        };
    }

    @Override
    public long getSize() {
        return query.getSize();
    }

}
