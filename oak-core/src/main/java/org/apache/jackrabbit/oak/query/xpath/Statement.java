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

import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.xpath.Expression.AndCondition;
import org.apache.jackrabbit.oak.query.xpath.Expression.OrCondition;
import org.apache.jackrabbit.oak.query.xpath.Expression.Property;

/**
 * An xpath statement.
 */
public class Statement {

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
    
    public Statement optimize() {
        ignoreOrderByScoreDesc();
        if (where == null) {
            return this;
        }
        where = where.optimize();
        ArrayList<Expression> unionList = new ArrayList<Expression>();
        addToUnionList(where, unionList);
        if (unionList.size() == 1) {
            return this;
        }
        Statement union = null;
        for (int i = 0; i < unionList.size(); i++) {
            Expression e = unionList.get(i);
            Statement s = new Statement();
            s.columnSelector = columnSelector;
            s.selectors = selectors;
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

        return union;
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
        buff.append(new Expression.Property(columnSelector, QueryImpl.JCR_PATH, false).toString());
        if (selectors.size() > 1) {
            buff.append(" as ").append('[').append(QueryImpl.JCR_PATH).append(']');
        }
        buff.append(", ");
        buff.append(new Expression.Property(columnSelector, QueryImpl.JCR_SCORE, false).toString());
        if (selectors.size() > 1) {
            buff.append(" as ").append('[').append(QueryImpl.JCR_SCORE).append(']');
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
            // leave original xpath string as a comment
            appendXPathAsComment(buff, xpathQuery);
            return buff.toString();
        }
        
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

}
