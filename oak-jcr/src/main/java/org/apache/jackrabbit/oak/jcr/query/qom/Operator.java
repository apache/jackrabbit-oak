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
package org.apache.jackrabbit.oak.jcr.query.qom;

import javax.jcr.RepositoryException;
import javax.jcr.query.qom.Comparison;
import javax.jcr.query.qom.DynamicOperand;
import javax.jcr.query.qom.QueryObjectModelConstants;
import javax.jcr.query.qom.QueryObjectModelFactory;
import javax.jcr.query.qom.StaticOperand;

/**
 * Enumeration of the JCR 2.0 query operators.
 *
 * @since Apache Jackrabbit 2.0
 */
public enum Operator {

    EQ(QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO, "="),

    NE(QueryObjectModelConstants.JCR_OPERATOR_NOT_EQUAL_TO, "!=", "<>"),

    GT(QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN, ">"),

    GE(QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO, ">="),

    LT(QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN, "<"),

    LE(QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO, "<="),

    LIKE(QueryObjectModelConstants.JCR_OPERATOR_LIKE, null, "like");

    /**
     * JCR name of this operator.
     */
    private final String name;

    /**
     * This operator in XPath syntax.
     */
    private final String xpath;

    /**
     * This operator in SQL syntax.
     */
    private final String sql;

    Operator(String name, String op) {
        this(name, op, op);
    }

    Operator(String name, String xpath, String sql) {
        this.name = name;
        this.xpath = xpath;
        this.sql = sql;
    }

    /**
     * Returns a comparison between the given operands using this operator.
     *
     * @param factory factory for creating the comparison
     * @param left operand on the left hand side
     * @param right operand on the right hand side
     * @return comparison
     * @throws RepositoryException if the comparison can not be created
     */
    public Comparison comparison(
            QueryObjectModelFactory factory,
            DynamicOperand left, StaticOperand right)
            throws RepositoryException {
        return factory.comparison(left, name, right);
    }

    /**
     * Formats an XPath constraint with this operator and the given operands.
     * The operands are simply used as-is, without any quoting or escaping.
     *
     * @param a first operand
     * @param b second operand
     * @return XPath constraint, {@code a op b} or
     *         {@code jcr:like(a, b)} for {@link #LIKE}
     */
    public String formatXpath(String a, String b) {
        if (this == LIKE) {
            return "jcr:like(" + a + ", " + b + ')';
        }
        return a + ' ' + xpath + ' ' + b;
    }

    /**
     * Formats an SQL constraint with this operator and the given operands.
     * The operands are simply used as-is, without any quoting or escaping.
     *
     * @param a first operand
     * @param b second operand
     * @return SQL constraint, {@code a op b}
     */
    public String formatSql(String a, String b) {
        return a + ' ' + sql + ' ' + b;
    }

    /**
     * Returns the JCR 2.0 name of this query operator.
     *
     * @see QueryObjectModelConstants
     * @return JCR name of this operator
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * Returns an array of the names of all the JCR 2.0 query operators.
     *
     * @return names of all query operators
     */
    public static String[] getAllQueryOperators() {
        return new String[] {
                EQ.toString(),
                NE.toString(),
                GT.toString(),
                GE.toString(),
                LT.toString(),
                LE.toString(),
                LIKE.toString()
        };
    }

    /**
     * Returns the operator with the given JCR name.
     *
     * @param name JCR name of an operator
     * @return operator with the given name
     */
    public static Operator getOperatorByName(String name) {
        for (Operator operator : Operator.values()) {
            if (operator.name.equals(name)) {
                return operator;
            }
        }
        throw new IllegalArgumentException("Unknown operator name: " + name);
    }

}
