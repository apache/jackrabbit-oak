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
package org.apache.jackrabbit.oak.query.ast;

/**
 * A visitor to access all elements.
 */
public interface AstVisitor {

    boolean visit(AndImpl node);

    boolean visit(BindVariableValueImpl node);

    boolean visit(ChildNodeImpl node);

    boolean visit(ChildNodeJoinConditionImpl node);

    boolean visit(CoalesceImpl node);

    boolean visit(ColumnImpl node);

    boolean visit(ComparisonImpl node);

    boolean visit(InImpl node);

    boolean visit(DescendantNodeImpl node);

    boolean visit(DescendantNodeJoinConditionImpl node);

    boolean visit(EquiJoinConditionImpl node);

    boolean visit(FullTextSearchImpl node);

    boolean visit(FullTextSearchScoreImpl node);

    boolean visit(JoinImpl node);

    boolean visit(LengthImpl node);

    boolean visit(LiteralImpl node);

    boolean visit(LowerCaseImpl node);

    boolean visit(NodeLocalNameImpl node);

    boolean visit(NodeNameImpl node);

    boolean visit(NotImpl node);

    boolean visit(OrderingImpl node);

    boolean visit(OrImpl node);

    boolean visit(PropertyExistenceImpl node);

    boolean visit(PropertyInexistenceImpl node);

    boolean visit(PropertyValueImpl node);

    boolean visit(SameNodeImpl node);

    boolean visit(SameNodeJoinConditionImpl node);

    boolean visit(SelectorImpl node);

    boolean visit(UpperCaseImpl node);

    boolean visit(NativeFunctionImpl node);

    boolean visit(SimilarImpl node);
    
    boolean visit(SpellcheckImpl node);

    boolean visit(SuggestImpl suggest);
}