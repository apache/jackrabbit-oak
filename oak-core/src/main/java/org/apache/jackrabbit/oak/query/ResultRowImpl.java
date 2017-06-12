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

import java.util.Arrays;
import java.util.Comparator;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;

/**
 * A query result row that keeps all data (for this row only) in memory.
 */
public class ResultRowImpl implements ResultRow {

    private final Query query;
    private final Tree[] trees;

    /**
     * The column values.
     */
    private final PropertyValue[] values;

    /**
     * Whether the value at the given index is used for comparing rows (used
     * within hashCode and equals). If null, all columns are distinct.
     */
    private final boolean[] distinctValues;

    /**
     * The values used for ordering.
     */
    private final PropertyValue[] orderValues;

    ResultRowImpl(Query query, Tree[] trees, PropertyValue[] values, boolean[] distinctValues, PropertyValue[] orderValues) {
        this.query = query;
        this.trees = trees;
        this.values = values;
        this.distinctValues = distinctValues;
        this.orderValues = orderValues;
    }

    PropertyValue[] getOrderValues() {
        return orderValues;
    }

    @Override
    public String getPath() {
        return getPath(null);
    }

    @Override
    public String getPath(String selectorName) {
        Tree tree = getTree(selectorName);
        if (tree != null) {
            return tree.getPath();
        } else {
            return null;
        }
    }

    @Override
    public Tree getTree(String selectorName) {
        if (selectorName == null) {
            if (trees.length > 1) {
                throw new IllegalArgumentException("More than one selector");
            } else if (trees.length == 0) {
                throw new IllegalArgumentException("This query does not have a selector");
            }
            return trees[0];
        }
        int index = query.getSelectorIndex(selectorName);
        if (trees == null || index >= trees.length) {
            return null;
        }
        return trees[index];
    }

    @Override
    public PropertyValue getValue(String columnName) {
        int index = query.getColumnIndex(columnName);
        if (index >= 0) {
            return values[index];
        }
        if (JcrConstants.JCR_PATH.equals(columnName)) {
            return PropertyValues.newString(getPath());
        }
        // OAK-318:
        // somebody might call rep:excerpt(text)
        // even though the query doesn't contain that column
        if (columnName.startsWith(QueryConstants.REP_EXCERPT)) {
            int columnIndex = query.getColumnIndex(QueryConstants.REP_EXCERPT);
            PropertyValue indexExcerptValue = null;
            if (columnIndex >= 0) {
                indexExcerptValue = values[columnIndex];
                if (indexExcerptValue != null) {
                    if (QueryConstants.REP_EXCERPT.equals(columnName) || SimpleExcerptProvider.REP_EXCERPT_FN.equals(columnName)) {
                        return SimpleExcerptProvider.getExcerpt(indexExcerptValue);
                    }
                }
            }
            return getFallbackExcerpt(columnName, indexExcerptValue);
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    private PropertyValue getFallbackExcerpt(String columnName, PropertyValue indexValue) {
        String ex = SimpleExcerptProvider.getExcerpt(getPath(), columnName,
                query, true);
        if (ex != null && ex.length() > 24) {
            return PropertyValues.newString(ex);
        } else if (indexValue != null) {
            return SimpleExcerptProvider.getExcerpt(indexValue);
        }
        return PropertyValues.newString(getPath());
    }

    @Override
    public PropertyValue[] getValues() {
        PropertyValue[] v2 = new PropertyValue[values.length];
        System.arraycopy(values, 0, v2, 0, values.length);
        return v2;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        for (String s : query.getSelectorNames()) {
            String p = getPath(s);
            if (p != null) {
                buff.append(s).append(": ").append(p).append(" ");
            }
        }
        ColumnImpl[] cols = query.getColumns();
        for (int i = 0; i < values.length; i++) {
            ColumnImpl c = cols[i];
            String n = c.getColumnName();
            if (n != null) {
                buff.append(n).append(": ").append(values[i]).append(" ");
            }
        }
        return buff.toString();
    }


    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + Arrays.hashCode(getPaths());
        result = 31 * result + hashCodeOfValues();
        return result;
    }

    private int hashCodeOfValues() {
        int result = 1;
        for (int i = 0; i < values.length; i++) {
            if (distinctValues == null || distinctValues[i]) {
                PropertyValue v = values[i];
                result = 31 * result + (v == null ? 0 : v.hashCode());
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj.getClass() != obj.getClass()) {
            return false;
        }
        ResultRowImpl other = (ResultRowImpl) obj;
        if (!Arrays.equals(getPaths(), other.getPaths())) {
            return false;
        } else if (!Arrays.equals(distinctValues, other.distinctValues)) {
            return false;
        }
        // if distinctValues are equals, then the number of values
        // is also equal
        for (int i = 0; i < values.length; i++) {
            if (distinctValues == null || distinctValues[i]) {
                Object o1 = values[i];
                Object o2 = other.values[i];
                if (!(o1 == null ? o2 == null : o1.equals(o2))) {
                    return false;
                }
            }
        }
        return true;
    }

    private String[] getPaths() {
        String[] paths = new String[trees.length];
        for (int i = 0; i < trees.length; i++) {
            if (trees[i] != null) {
                paths[i] = trees[i].getPath();
            } else {
                paths[i] = null;
            }
        }
        return paths;
    }

    public static Comparator<ResultRowImpl> getComparator(
            final OrderingImpl[] orderings) {
        if (orderings == null) {
            return null;
        }
        return new Comparator<ResultRowImpl>() {

            @Override
            public int compare(ResultRowImpl o1, ResultRowImpl o2) {
                PropertyValue[] orderValues = o1.getOrderValues();
                PropertyValue[] orderValues2 = o2.getOrderValues();
                int comp = 0;
                for (int i = 0, size = orderings.length; i < size; i++) {
                    PropertyValue a = orderValues[i];
                    PropertyValue b = orderValues2[i];
                    if (a == null || b == null) {
                        if (a == b) {
                            comp = 0;
                        } else if (a == null) {
                            // TODO order by: nulls first (it looks like), or
                            // low?
                            comp = -1;
                        } else {
                            comp = 1;
                        }
                    } else {
                        comp = a.compareTo(b);
                    }
                    if (comp != 0) {
                        if (orderings[i].isDescending()) {
                            comp = -comp;
                        }
                        break;
                    }
                }
                return comp;
            }
        };

    }

}