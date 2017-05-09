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

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;

/**
 * A query result.
 */
public class ResultImpl implements Result {

    protected final Query query;

    ResultImpl(Query query) {
        this.query = query;
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
    public String[] getColumnSelectorNames() {
        ArrayList<String> list = new ArrayList<String>();
        for (ColumnImpl c : query.getColumns()) {
            SelectorImpl selector = c.getSelector();
            String name = selector == null ? null : selector.getSelectorName();
            if (!list.contains(name)) {
                list.add(name);
            }
        }
        return list.toArray(new String[list.size()]);
    }

    @Override
    public String[] getSelectorNames() {
        return query.getSelectorNames();
    }

    @Override
    public Iterable<? extends ResultRow> getRows() {
        return new Iterable<ResultRowImpl>() {

            @Override
            public Iterator<ResultRowImpl> iterator() {
                return query.getRows();
            }

        };
    }

    @Override
    public long getSize() {
        return query.getSize();
    }

    @Override
    public long getSize(SizePrecision precision, long max) {
        return query.getSize(precision, max);
    }

}
