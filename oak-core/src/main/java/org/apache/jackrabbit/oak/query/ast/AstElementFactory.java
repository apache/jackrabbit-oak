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
package org.apache.jackrabbit.oak.query.ast;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for syntax tree elements.
 */
public class AstElementFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AstElementFactory.class);

    public AndImpl and(ConstraintImpl constraint1, ConstraintImpl constraint2) {
        return new AndImpl(constraint1, constraint2);
    }

    public OrderingImpl ascending(DynamicOperandImpl operand) {
        return new OrderingImpl(operand, Order.ASCENDING);
    }

    public BindVariableValueImpl bindVariable(String bindVariableName) {
        return new BindVariableValueImpl(bindVariableName);
    }

    public ChildNodeImpl childNode(String selectorName, String path) {
        return new ChildNodeImpl(selectorName, path);
    }

    public ChildNodeJoinConditionImpl childNodeJoinCondition(String childSelectorName, String parentSelectorName)
            {
        return new ChildNodeJoinConditionImpl(childSelectorName, parentSelectorName);
    }

    public CoalesceImpl coalesce(DynamicOperandImpl operand1, DynamicOperandImpl operand2) {
        return new CoalesceImpl(operand1, operand2);
    }

    public ColumnImpl column(String selectorName, String propertyName, String columnName) {
        if (propertyName.startsWith(QueryConstants.REP_FACET)) {
            return new FacetColumnImpl(selectorName, propertyName, columnName);
        } else {
            return new ColumnImpl(selectorName, propertyName, columnName);
        }
    }

    public ComparisonImpl comparison(DynamicOperandImpl operand1, Operator operator, StaticOperandImpl operand2) {
        return new ComparisonImpl(operand1, operator, operand2);
    }

    public DescendantNodeImpl descendantNode(String selectorName, String path) {
        return new DescendantNodeImpl(selectorName, path);
    }

    public DescendantNodeJoinConditionImpl descendantNodeJoinCondition(String descendantSelectorName,
            String ancestorSelectorName) {
        return new DescendantNodeJoinConditionImpl(descendantSelectorName, ancestorSelectorName);
    }

    public OrderingImpl descending(DynamicOperandImpl operand) {
        return new OrderingImpl(operand, Order.DESCENDING);
    }

    public EquiJoinConditionImpl equiJoinCondition(String selector1Name, String property1Name, String selector2Name,
            String property2Name) {
        return new EquiJoinConditionImpl(selector1Name, property1Name, selector2Name, property2Name);
    }

    public FullTextSearchImpl fullTextSearch(String selectorName, String propertyName,
            StaticOperandImpl fullTextSearchExpression) {
        return new FullTextSearchImpl(selectorName, propertyName, fullTextSearchExpression);
    }

    public FullTextSearchScoreImpl fullTextSearchScore(String selectorName) {
        return new FullTextSearchScoreImpl(selectorName);
    }

    public JoinImpl join(SourceImpl left, SourceImpl right, JoinType joinType, JoinConditionImpl joinCondition) {
        return new JoinImpl(left, right, joinType, joinCondition);
    }

    public LengthImpl length(DynamicOperandImpl operand) {
        return new LengthImpl(operand);
    }

    public LiteralImpl literal(PropertyValue literalValue) {
        return new LiteralImpl(literalValue);
    }

    public LowerCaseImpl lowerCase(DynamicOperandImpl operand) {
        return new LowerCaseImpl(operand);
    }

    public NodeLocalNameImpl nodeLocalName(String selectorName) {
        return new NodeLocalNameImpl(selectorName);
    }

    public NodeNameImpl nodeName(String selectorName) {
        return new NodeNameImpl(selectorName);
    }

    public NotImpl not(ConstraintImpl constraint) {
        return new NotImpl(constraint);
    }

    public OrImpl or(ConstraintImpl constraint1, ConstraintImpl constraint2) {
        return new OrImpl(constraint1, constraint2);
    }

    public PropertyExistenceImpl propertyExistence(String selectorName, String propertyName) {
        return new PropertyExistenceImpl(selectorName, propertyName);
    }
    
    public PropertyInexistenceImpl propertyInexistence(String selectorName, String propertyName) {
        return new PropertyInexistenceImpl(selectorName, propertyName);
    }

    public PropertyValueImpl propertyValue(String selectorName, String propertyName) {
        return new PropertyValueImpl(selectorName, propertyName);
    }

    public PropertyValueImpl propertyValue(String selectorName, String propertyName, String propertyType) {
        return new PropertyValueImpl(selectorName, propertyName, propertyType);
    }

    public SameNodeImpl sameNode(String selectorName, String path) {
        return new SameNodeImpl(selectorName, path);
    }

    public SameNodeJoinConditionImpl sameNodeJoinCondition(String selector1Name, String selector2Name, String selector2Path) {
        return new SameNodeJoinConditionImpl(selector1Name, selector2Name, selector2Path);
    }

    public SelectorImpl selector(NodeTypeInfo nodeTypeInfo, String selectorName) {
        return new SelectorImpl(nodeTypeInfo, selectorName);
    }

    public UpperCaseImpl upperCase(DynamicOperandImpl operand) {
        return new UpperCaseImpl(operand);
    }

    public ConstraintImpl in(DynamicOperandImpl left,
            ArrayList<StaticOperandImpl> list) {
        return new InImpl(left, list);
    }

    public NativeFunctionImpl nativeFunction(String selectorName, String language, StaticOperandImpl expression) {
        return new NativeFunctionImpl(selectorName, language, expression);
    }

    public SimilarImpl similar(String selectorName, String propertyName,
            StaticOperandImpl path) {
        return new SimilarImpl(selectorName, propertyName, path);
    }

    public ConstraintImpl spellcheck(String selectorName, StaticOperandImpl expression) {
        return new SpellcheckImpl(selectorName, expression);
    }

    public ConstraintImpl suggest(String selectorName, StaticOperandImpl expression) {
        return new SuggestImpl(selectorName, expression);
    }
    
    /**
     * <p>
     * as the {@link AstElement#copyOf()} can return {@code this} is the cloning is not implemented
     * by the subclass, this method add some spice around it by checking for this case and tracking
     * a DEBUG message in the logs.
     * </p>
     * 
     * @param e the element to be cloned. Cannot be null.
     * @return same as {@link AstElement#copyOf()}
     */
    @Nonnull
    public static AstElement copyElementAndCheckReference(@Nonnull final AstElement e) {
        AstElement clone = checkNotNull(e).copyOf();
        
        if (clone == e && LOG.isDebugEnabled()) {
            LOG.debug(
                "Failed to clone the AstElement. Returning same reference; the client may fail. {} - {}",
                e.getClass().getName(), e);
        }
        
        return clone;
    }

}
