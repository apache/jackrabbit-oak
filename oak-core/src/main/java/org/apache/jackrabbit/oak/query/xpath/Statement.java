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
package org.apache.jackrabbit.oak.query.xpath;

import java.util.ArrayList;

import org.apache.jackrabbit.oak.query.QueryOptions;
import org.apache.jackrabbit.oak.query.QueryOptions.Traversal;
import org.apache.jackrabbit.oak.query.xpath.Expression.AndCondition;
import org.apache.jackrabbit.oak.query.xpath.Expression.OrCondition;
import org.apache.jackrabbit.oak.query.xpath.Expression.Property;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;

/**
 * An xpath statement.
 */
public class Statement {
    
    private static final UnsupportedOperationException TOO_MANY_UNION = 
            new UnsupportedOperationException("Too many union queries");
    private final static int MAX_UNION = Integer.getInteger("oak.xpathMaxUnion", 1000);

    boolean explain;
    boolean measure;
    
    /**
     * The selector to get the columns from (the selector used in the select
     * column list).
     */
    private Selector columnSelector;
    
    private ArrayList<Expression> columnList = new ArrayList<Expression>();
    
    /**
     * All selectors.
     */
    private ArrayList<Selector> selectors;
    
    private Expression where;

    ArrayList<Order> orderList = new ArrayList<Order>();
    
    String xpathQuery;
    
    QueryOptions queryOptions;
    
    public Statement optimize() {
        ignoreOrderByScoreDesc();
        if (where == null) {
            return this;
        }
        where = where.optimize();
        optimizeSelectorNodeTypes();
        ArrayList<Expression> unionList = new ArrayList<Expression>();
        try {
            addToUnionList(where, unionList);
        } catch (UnsupportedOperationException e) {
            // too many union
            return this;
        }
        if (unionList.size() == 1) {
            return this;
        }
        Statement union = null;
        for (int i = 0; i < unionList.size(); i++) {
            Expression e = unionList.get(i);
            Statement s = new Statement();
            s.columnSelector = new Selector(columnSelector);
            s.selectors = cloneSelectors();
            s.columnList = columnList;
            s.where = e;
            if (union == null) {
                union = s;
            } else {
                union = new UnionStatement(union.optimize(), s.optimize());
            }
        }
        union.orderList = orderList;
        union.xpathQuery = xpathQuery;
        union.measure = measure;
        union.explain = explain;
        union.queryOptions = queryOptions;

        return union;
    }
    
    private ArrayList<Selector> cloneSelectors() {
        ArrayList<Selector> list = new ArrayList<Selector>();
        for (Selector s : selectors) {
            list.add(new Selector(s));
        }
        return list;
    }
    
    private void optimizeSelectorNodeTypes() {
        if (!XPathToSQL2Converter.NODETYPE_OPTIMIZATION) {
            return;
        }
        for (int i = 0; i < selectors.size(); i++) {
            Selector s = selectors.get(i);
            if (s.nodeType != null && !"nt:base".equals(s.nodeType)) {
                // explicit node type: ignore
                continue;
            }
            // only filter by selectorName if there are multiple selectors
            String selectorName = selectors.size() == 1 ? null : s.name;
            String nodeType = where.getMostSpecificNodeType(selectorName);
            if (nodeType != null) {
                s.nodeType = nodeType;
            }
        }
    }
    
    private static void addToUnionList(Expression condition,  ArrayList<Expression> unionList) {
        if (condition instanceof OrCondition) {
            OrCondition or = (OrCondition) condition;
            // conditions of type                
            // @x = 1 or @y = 2
            // or similar are converted to
            // (@x = 1) union (@y = 2)
            addToUnionList(or.left, unionList);
            addToUnionList(or.right, unionList);
            return;
        } else if (condition instanceof AndCondition) {
            // conditions of type
            // @a = 1 and (@x = 1 or @y = 2)
            // are automatically converted to
            // (@a = 1 and @x = 1) union (@a = 1 and @y = 2)
            AndCondition and = (AndCondition) condition;
            and = and.pullOrRight();
            if (and.right instanceof OrCondition) {
                OrCondition or = (OrCondition) and.right;
                // same as above, but with the added "and"
                addToUnionList(new AndCondition(and.left, or.left), unionList);
                addToUnionList(new AndCondition(and.left, or.right), unionList);
                return;
            }
        }
        if (unionList.size() > MAX_UNION) {
            throw TOO_MANY_UNION;
        }
        unionList.add(condition);
    }
    
    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        
        // explain | measure ...
        if (explain) {
            buff.append("explain ");
        } 
        if (measure) {
            buff.append("measure ");
        }
        
        // select ...
        buff.append("select ");
        buff.append(new Expression.Property(columnSelector, QueryConstants.JCR_PATH, false).toString());
        if (selectors.size() > 1) {
            buff.append(" as ").append('[').append(QueryConstants.JCR_PATH).append(']');
        }
        buff.append(", ");
        buff.append(new Expression.Property(columnSelector, QueryConstants.JCR_SCORE, false).toString());
        if (selectors.size() > 1) {
            buff.append(" as ").append('[').append(QueryConstants.JCR_SCORE).append(']');
        }
        if (columnList.isEmpty()) {
            buff.append(", ");
            buff.append(new Expression.Property(columnSelector, "*", false).toString());
        } else {
            for (int i = 0; i < columnList.size(); i++) {
                buff.append(", ");
                Expression e = columnList.get(i);
                String columnName = e.toString();
                buff.append(columnName);
                if (selectors.size() > 1) {
                    buff.append(" as [").append(e.getColumnAliasName()).append("]");
                }
            }
        }
        
        // from ...
        buff.append(" from ");
        for (int i = 0; i < selectors.size(); i++) {
            Selector s = selectors.get(i);
            if (i > 0) {
                buff.append(" inner join ");
            }
            String nodeType = s.nodeType;
            if (nodeType == null) {
                nodeType = "nt:base";
            }
            buff.append('[' + nodeType + ']').append(" as ").append(s.name);
            if (s.joinCondition != null) {
                buff.append(" on ").append(s.joinCondition);
            }
        }
        
        // where ...
        if (where != null) {
            buff.append(" where ").append(where.toString());
        }
        
        // order by ...
        if (!orderList.isEmpty()) {
            buff.append(" order by ");
            for (int i = 0; i < orderList.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(orderList.get(i));
            }
        }
        appendQueryOptions(buff, queryOptions);
        // leave original xpath string as a comment
        appendXPathAsComment(buff, xpathQuery);
        return buff.toString();        
    }
    
    private void ignoreOrderByScoreDesc() {
        if (orderList.size() != 1) {
            return;
        }
        Order order = orderList.get(0);
        if (!order.descending) {
            return;
        }
        if (!order.expr.toString().equals("[jcr:score]")) {
            return;
        }
        // so we have just one expression, 
        // and it is "order by @jcr:score desc"
        // this we can remove
        orderList.remove(0);
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public void setMeasure(boolean measure) {
        this.measure = measure;
    }

    public void addSelectColumn(Property p) {
        columnList.add(p);
    }

    public void setSelectors(ArrayList<Selector> selectors) {
        this.selectors = selectors;
    }
    
    public void setWhere(Expression where) {
        this.where = where;
    }

    public void addOrderBy(Order order) {
        this.orderList.add(order);
    }

    public void setColumnSelector(Selector columnSelector) {
        this.columnSelector = columnSelector;
    }
    
    public void setOriginalQuery(String xpathQuery) {
        this.xpathQuery = xpathQuery;
    }
    
    /**
     * A union statement.
     */
    static class UnionStatement extends Statement {
        
        private final Statement s1, s2;
        
        UnionStatement(Statement s1, Statement s2) {
            this.s1 = s1;
            this.s2 = s2;
        }
        
        @Override
        public Statement optimize() {
            Statement s1b = s1.optimize();
            Statement s2b = s2.optimize();
            if (s1 == s1b && s2 == s2b) {
                // no change
                return this;
            }
            return new UnionStatement(s1b, s2b);
        }
        
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            // explain | measure ...
            if (explain) {
                buff.append("explain ");
            } 
            if (measure) {
                buff.append("measure ");
            }
            buff.append(s1).append(" union ").append(s2);
            // order by ...
            if (orderList != null && !orderList.isEmpty()) {
                buff.append(" order by ");
                for (int i = 0; i < orderList.size(); i++) {
                    if (i > 0) {
                        buff.append(", ");
                    }
                    buff.append(orderList.get(i));
                }
            }
            appendQueryOptions(buff, queryOptions);
            // leave original xpath string as a comment
            appendXPathAsComment(buff, xpathQuery);
            return buff.toString();
        }
        
    }
    
    private static void appendQueryOptions(StringBuilder buff, QueryOptions queryOptions) {
        if (queryOptions == null) {
            return;
        }
        buff.append(" option(");
        int optionCount = 0;
        if (queryOptions.traversal != Traversal.DEFAULT) {
            buff.append("traversal " + queryOptions.traversal);
            optionCount++;
        }
        if (queryOptions.indexName != null) {
            if (optionCount > 0) {
                buff.append(", ");
            }
            buff.append("index name [");
            buff.append(queryOptions.indexName);
            buff.append("]");
            optionCount++;
        }
        if (queryOptions.indexTag != null) {
            if (optionCount > 0) {
                buff.append(", ");
            }
            buff.append("index tag [");
            buff.append(queryOptions.indexTag);
            buff.append("]");
            optionCount++;
        }
        buff.append(")");
    }
    
    private static void appendXPathAsComment(StringBuilder buff, String xpath) {
        if (xpath == null) {
            return;
        }
        buff.append(" /* xpath: ");
        // the xpath query may contain the "end comment" marker
        String xpathEscaped = xpath.replaceAll("\\*\\/", "* /");
        buff.append(xpathEscaped);
        buff.append(" */");        
    }

    public  void setQueryOptions(QueryOptions options) {
        this.queryOptions = options;
    }

}
