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
package org.apache.jackrabbit.oak.spi.query.fulltext;

/**
 * The base class for fulltext condition expression.
 */
public abstract class FullTextExpression {
    
    /**
     * The operator precedence for OR conditions.
     */
    public static final int PRECEDENCE_OR = 1;

    /**
     * The operator precedence for AND conditions.
     */
    public static final int PRECEDENCE_AND = 2;
    
    /**
     * The operator precedence for terms.
     */
    public static final int PRECEDENCE_TERM = 3;

    /**
     * Get the operator precedence.
     * 
     * @return the precedence
     */
    public abstract int getPrecedence();
    
    /**
     * Evaluate whether the value matches the condition.
     * 
     * @param value the value
     * @return true if it matches
     */
    public abstract boolean evaluate(String value);
    
    /**
     * Simplify the expression if possible (removing duplicate conditions).
     * 
     * @return the simplified expression
     */
    abstract FullTextExpression simplify();
    
    /**
     * Get the string representation of the condition.
     */
    @Override
    public abstract String toString();
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof FullTextExpression)) {
            return false;
        }
        return toString().equals(other.toString());
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    /**
     * Let the expression call the applicable visit method of the visitor. 
     * 
     * @param v the visitor
     * @return true if the visit method returned true
     */
    public abstract boolean accept(FullTextVisitor v);
    
    /**
     * Whether the current {@link FullTextExpression} is a {@code NOT} condition or not. Default is
     * false
     * 
     * @return true if the current condition represent a NOT, false otherwise.
     */
    public boolean isNot() {
        return false;
    }
}