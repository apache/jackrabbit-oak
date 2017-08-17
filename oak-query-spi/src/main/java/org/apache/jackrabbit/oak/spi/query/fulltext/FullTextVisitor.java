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
 * A visitor for full-text expressions. This class is abstract because at least
 * one of the methods needs to be implemented to make anything useful, most
 * likely visit(FullTextTerm).
 */
public interface FullTextVisitor {
    
    /**
     * Visit an "contains" expression.
     * 
     * @param contains the "contains" expression
     * @return true if visiting should continue
     */
    boolean visit(FullTextContains contains);

    /**
     * Visit an "and" expression.
     * 
     * @param and the "and" expression
     * @return true if visiting should continue
     */
    boolean visit(FullTextAnd and);

    /**
     * Visit an "or" expression.
     * 
     * @param or the "or" expression
     * @return true if visiting should continue
     */
    boolean visit(FullTextOr or);

    /**
     * Visit a term
     * 
     * @param term the term
     * @return true if visiting should continue
     */
    boolean visit(FullTextTerm term);
    
    /**
     * The base implementation of a full-text visitor.
     */
    public abstract static class FullTextVisitorBase implements FullTextVisitor {

        @Override
        public boolean visit(FullTextContains contains) {
            return contains.getBase().accept(this);
        }

        @Override
        public boolean visit(FullTextAnd and) {
            for (FullTextExpression e : and.list) {
                if (!e.accept(this)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean visit(FullTextOr or) {
            for (FullTextExpression e : or.list) {
                if (!e.accept(this)) {
                    return false;
                }
            }
            return true;
        }

    }
    
}
