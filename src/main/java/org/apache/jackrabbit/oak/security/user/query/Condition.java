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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.Value;


interface Condition {

    void accept(ConditionVisitor visitor) throws RepositoryException;

    //-----------------------------------------------------< Node Condition >---
    class Node implements Condition {

        private final String pattern;

        public Node(String pattern) {
            this.pattern = pattern;
        }

        public String getPattern() {
            return pattern;
        }

        public void accept(ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------< Property Condition >---
    class Property implements Condition {

        private final String relPath;
        private final RelationOp op;
        private final Value value;
        private final String pattern;

        public Property(String relPath, RelationOp op, Value value) {
            this.relPath = relPath;
            this.op = op;
            this.value = value;
            pattern = null;
        }

        public Property(String relPath, RelationOp op, String pattern) {
            this.relPath = relPath;
            this.op = op;
            value = null;
            this.pattern = pattern;
        }

        public Property(String relPath, RelationOp op) {
            this.relPath = relPath;
            this.op = op;
            value = null;
            pattern = null;
        }

        public String getRelPath() {
            return relPath;
        }

        public RelationOp getOp() {
            return op;
        }

        public Value getValue() {
            return value;
        }

        public String getPattern() {
            return pattern;
        }

        public void accept(ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------< Contains Condition >---
    class Contains implements Condition {

        private final String relPath;
        private final String searchExpr;

        public Contains(String relPath, String searchExpr) {
            this.relPath = relPath;
            this.searchExpr = searchExpr;
        }

        public String getRelPath() {
            return relPath;
        }

        public String getSearchExpr() {
            return searchExpr;
        }

        public void accept(ConditionVisitor visitor) {
            visitor.visit(this);
        }
    }

    //--------------------------------------------< Impersonation Condition >---
    class Impersonation implements Condition {

        private final String name;

        public Impersonation(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void accept(ConditionVisitor visitor) {
            visitor.visit(this);
        }
    }

    //------------------------------------------------------< Not Condition >---
    class Not implements Condition {

        private final Condition condition;

        public Not(Condition condition) {
            this.condition = condition;
        }

        public Condition getCondition() {
            return condition;
        }

        public void accept(ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------< Compound Condition >---
    abstract class Compound implements Condition, Iterable<Condition> {

        private final List<Condition> conditions = new ArrayList<Condition>();

        public Compound(Condition condition1, Condition condition2) {
            conditions.add(condition1);
            conditions.add(condition2);
        }

        public Iterator<Condition> iterator() {
            return conditions.iterator();
        }
    }

    //------------------------------------------------------< And Condition >---
    class And extends Compound {

        public And(Condition condition1, Condition condition2) {
            super(condition1, condition2);
        }

        public void accept(ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }

    //-------------------------------------------------------< Or Condition >---
    class Or extends Compound {

        public Or(Condition condition1, Condition condition2) {
            super(condition1, condition2);
        }

        public void accept(ConditionVisitor visitor) throws RepositoryException {
            visitor.visit(this);
        }
    }
}