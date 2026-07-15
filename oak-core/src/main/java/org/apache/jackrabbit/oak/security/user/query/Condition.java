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
package org.apache.jackrabbit.oak.security.user.query;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.Value;


interface Condition {

    void accept(@NotNull ConditionVisitor visitor) throws RepositoryException;

    //-----------------------------------------------------< Node Condition >---
    class Node implements Condition {

        private final String pattern;

        public Node(@NotNull String pattern) {
            this.pattern = pattern;
        }

        @NotNull
        public String getPattern() {
            return pattern;
        }

        public void accept(@NotNull ConditionVisitor visitor) {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------< Property Condition >---
    abstract class Property implements Condition {

        private final String relPath;
        private final RelationOp op;

        private Property(@NotNull String relPath, @NotNull RelationOp op) {
            this.relPath = relPath;
            this.op = op;
        }

        @NotNull
        public String getRelPath() {
            return relPath;
        }

        @NotNull
        public RelationOp getOp() {
            return op;
        }
    }

    class PropertyValue extends Property {

        private final Value value;

        PropertyValue(@NotNull String relPath, @NotNull RelationOp op, @NotNull Value value) {
            super(relPath, op);
            this.value = value;
        }

        @NotNull
        public Value getValue() {
            return value;
        }

        public void accept(@NotNull ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    class PropertyLike extends Property {

        private final String pattern;

        PropertyLike(@NotNull String relPath, @NotNull String pattern) {
            super(relPath, RelationOp.LIKE);
            this.pattern = pattern;
        }

        @NotNull
        public String getPattern() {
            return pattern;
        }

        public void accept(@NotNull ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    class PropertyExists extends Property {

        PropertyExists(@NotNull String relPath) {
            super(relPath, RelationOp.EX);
        }

        public void accept(@NotNull ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------< Contains Condition >---
    class Contains implements Condition {

        private final String relPath;
        private final String searchExpr;

        public Contains(@NotNull String relPath, @NotNull String searchExpr) {
            this.relPath = relPath;
            this.searchExpr = searchExpr;
        }

        @NotNull
        public String getRelPath() {
            return relPath;
        }

        @NotNull
        public String getSearchExpr() {
            return searchExpr;
        }

        public void accept(@NotNull ConditionVisitor visitor) {
            visitor.visit(this);
        }
    }

    //--------------------------------------------< Impersonation Condition >---
    class Impersonation implements Condition {

        private final String name;

        public Impersonation(@NotNull String name) {
            this.name = name;
        }

        @NotNull
        public String getName() {
            return name;
        }

        public void accept(@NotNull ConditionVisitor visitor) {
            visitor.visit(this);
        }
    }

    //------------------------------------------------------< Not Condition >---
    class Not implements Condition {

        private final Condition condition;

        public Not(@NotNull Condition condition) {
            this.condition = condition;
        }

        @NotNull
        public Condition getCondition() {
            return condition;
        }

        public void accept(@NotNull ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------< Compound Condition >---
    abstract class Compound implements Condition, Iterable<Condition> {

        private final List<Condition> conditions = new ArrayList<>();

        Compound(@NotNull Condition condition1, @NotNull Condition condition2) {
            conditions.add(condition1);
            conditions.add(condition2);
        }

        @NotNull
        public Iterator<Condition> iterator() {
            return conditions.iterator();
        }
    }

    //------------------------------------------------------< And Condition >---
    class And extends Compound {

        public And(@NotNull Condition condition1, @NotNull Condition condition2) {
            super(condition1, condition2);
        }

        public void accept(@NotNull ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------------< Or Condition >---
    class Or extends Compound {

        public Or(@NotNull Condition condition1, @NotNull Condition condition2) {
            super(condition1, condition2);
        }

        public void accept(@NotNull ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }
}