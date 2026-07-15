/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.query.qom;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.qom.ChildNode;
import javax.jcr.query.qom.ChildNodeJoinCondition;
import javax.jcr.query.qom.Column;
import javax.jcr.query.qom.Comparison;
import javax.jcr.query.qom.Constraint;
import javax.jcr.query.qom.DescendantNode;
import javax.jcr.query.qom.DescendantNodeJoinCondition;
import javax.jcr.query.qom.DynamicOperand;
import javax.jcr.query.qom.EquiJoinCondition;
import javax.jcr.query.qom.FullTextSearch;
import javax.jcr.query.qom.FullTextSearchScore;
import javax.jcr.query.qom.Join;
import javax.jcr.query.qom.JoinCondition;
import javax.jcr.query.qom.Length;
import javax.jcr.query.qom.Literal;
import javax.jcr.query.qom.LowerCase;
import javax.jcr.query.qom.NodeLocalName;
import javax.jcr.query.qom.NodeName;
import javax.jcr.query.qom.Not;
import javax.jcr.query.qom.Or;
import javax.jcr.query.qom.Ordering;
import javax.jcr.query.qom.PropertyExistence;
import javax.jcr.query.qom.PropertyValue;
import javax.jcr.query.qom.QueryObjectModel;
import javax.jcr.query.qom.QueryObjectModelFactory;
import javax.jcr.query.qom.SameNode;
import javax.jcr.query.qom.SameNodeJoinCondition;
import javax.jcr.query.qom.Selector;
import javax.jcr.query.qom.Source;
import javax.jcr.query.qom.StaticOperand;
import javax.jcr.query.qom.UpperCase;

import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.query.QueryManagerImpl;

/**
 * The implementation of the corresponding JCR interface.
 */
public class QueryObjectModelFactoryImpl implements QueryObjectModelFactory {

    private final SessionContext sessionContext;
    private final QueryManagerImpl queryManager;

    public QueryObjectModelFactoryImpl(QueryManagerImpl queryManager, SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.queryManager = queryManager;
    }

    @Override
    public AndImpl and(Constraint constraint1, Constraint constraint2) {
        return new AndImpl((ConstraintImpl) constraint1, (ConstraintImpl) constraint2);
    }

    @Override
    public OrderingImpl ascending(DynamicOperand operand) {
        return new OrderingImpl((DynamicOperandImpl) operand, Order.ASCENDING);
    }

    @Override
    public BindVariableValueImpl bindVariable(String bindVariableName) {
        return new BindVariableValueImpl(bindVariableName);
    }

    @Override
    public ChildNode childNode(String selectorName, String path) {
        return new ChildNodeImpl(selectorName, path);
    }

    @Override
    public ChildNodeJoinCondition childNodeJoinCondition(
            String childSelectorName, String parentSelectorName) {
        return new ChildNodeJoinConditionImpl(childSelectorName, parentSelectorName);
    }

    @Override
    public Column column(String selectorName, 
            String propertyName, String columnName) throws RepositoryException {
        return new ColumnImpl(selectorName, getOakName(propertyName), columnName);
    }

    @Override
    public Comparison comparison(DynamicOperand operand1, 
            String operator, StaticOperand operand2) {
        return new ComparisonImpl((DynamicOperandImpl) operand1, 
                Operator.getOperatorByName(operator), (StaticOperandImpl) operand2);
    }

    @Override
    public DescendantNode descendantNode(String selectorName, String path) {
        return new DescendantNodeImpl(selectorName, path);
    }

    @Override
    public DescendantNodeJoinCondition descendantNodeJoinCondition(
            String descendantSelectorName,
            String ancestorSelectorName) {
        return new DescendantNodeJoinConditionImpl(
                descendantSelectorName, ancestorSelectorName);
    }

    @Override
    public Ordering descending(DynamicOperand operand) {
        return new OrderingImpl((DynamicOperandImpl) operand, Order.DESCENDING);
    }

    @Override
    public EquiJoinCondition equiJoinCondition(
            String selector1Name, String property1Name, 
            String selector2Name, String property2Name) throws RepositoryException {
        return new EquiJoinConditionImpl(selector1Name, getOakName(property1Name), 
                selector2Name, getOakName(property2Name));
    }

    @Override
    public FullTextSearch fullTextSearch(String selectorName, String propertyName,
            StaticOperand fullTextSearchExpression) throws RepositoryException {
        return new FullTextSearchImpl(selectorName, getOakName(propertyName), 
                (StaticOperandImpl) fullTextSearchExpression);
    }

    @Override
    public FullTextSearchScore fullTextSearchScore(String selectorName) {
        return new FullTextSearchScoreImpl(selectorName);
    }

    @Override
    public Join join(Source left, Source right, String joinType, JoinCondition joinCondition) {
        return new JoinImpl((SourceImpl) left, (SourceImpl) right, 
                JoinType.getJoinTypeByName(joinType), (JoinConditionImpl) joinCondition);
    }

    @Override
    public Length length(PropertyValue propertyValue) {
        return new LengthImpl((PropertyValueImpl) propertyValue);
    }

    @Override
    public Literal literal(Value literalValue) {
        return new LiteralImpl(literalValue);
    }

    @Override
    public LowerCase lowerCase(DynamicOperand operand) {
        return new LowerCaseImpl((DynamicOperandImpl) operand);
    }

    @Override
    public NodeLocalName nodeLocalName(String selectorName) {
        return new NodeLocalNameImpl(selectorName);
    }

    @Override
    public NodeName nodeName(String selectorName) {
        return new NodeNameImpl(selectorName);
    }

    @Override
    public Not not(Constraint constraint) {
        return new NotImpl((ConstraintImpl) constraint);
    }

    @Override
    public Or or(Constraint constraint1, Constraint constraint2) {
        return new OrImpl((ConstraintImpl) constraint1, (ConstraintImpl) constraint2);
    }

    @Override
    public PropertyExistence propertyExistence(String selectorName, 
            String propertyName) throws RepositoryException {
        return new PropertyExistenceImpl(selectorName, 
                getOakName(propertyName));
    }

    @Override
    public PropertyValue propertyValue(String selectorName, 
            String propertyName) throws RepositoryException {
        return new PropertyValueImpl(selectorName, getOakName(propertyName));
    }

    @Override
    public SameNode sameNode(String selectorName, String path) {
        return new SameNodeImpl(selectorName, path);
    }

    @Override
    public SameNodeJoinCondition sameNodeJoinCondition(String selector1Name, 
            String selector2Name, String selector2Path) {
        return new SameNodeJoinConditionImpl(selector1Name, selector2Name, selector2Path);
    }

    @Override
    public Selector selector(String nodeTypeName, String selectorName) 
            throws RepositoryException {
        return new SelectorImpl(getOakName(nodeTypeName), selectorName);
    }

    @Override
    public UpperCase upperCase(DynamicOperand operand) {
        return new UpperCaseImpl((DynamicOperandImpl) operand);
    }

    @Override
    public QueryObjectModel createQuery(Source source, Constraint constraint, 
            Ordering[] orderings, Column[] columns) {
        QueryObjectModelImpl qom = new QueryObjectModelImpl(queryManager,
                sessionContext.getValueFactory(),
                source, constraint, orderings, columns);
        qom.bindVariables();
        return qom;
    }
    
    private String getOakName(String jcrName) throws RepositoryException {
        if (jcrName == null) {
            return null;
        }
        return sessionContext.getOakName(jcrName);
    }

}
