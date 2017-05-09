/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query.ast;

import static org.apache.jackrabbit.oak.query.ast.AstElementFactory.copyElementAndCheckReference;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.query.plan.ExecutionPlan;
import org.apache.jackrabbit.oak.query.plan.JoinExecutionPlan;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A join. This object contains the left hand side source, the right hand side
 * source, the join type, and the join condition.
 */
public class JoinImpl extends SourceImpl {
    private final JoinConditionImpl joinCondition;
    private JoinType joinType;
    private SourceImpl left;
    private SourceImpl right;

    private boolean leftNeedExecute, rightNeedExecute;
    private boolean leftNeedNext;
    private boolean foundJoinedRow;
    private boolean end;
    private NodeState rootState;
    
    private JoinExecutionPlan plan;

    public JoinImpl(SourceImpl left, SourceImpl right, JoinType joinType,
            JoinConditionImpl joinCondition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
    }
    
    @Override
    public ArrayList<SourceImpl> getInnerJoinSelectors() {
        ArrayList<SourceImpl> list = new ArrayList<SourceImpl>();
        switch (joinType) {
        case INNER:
            list.addAll(left.getInnerJoinSelectors());
            list.addAll(right.getInnerJoinSelectors());
            break;
        case LEFT_OUTER:
            list.addAll(left.getInnerJoinSelectors());
            break;
        case RIGHT_OUTER:
            list.addAll(right.getInnerJoinSelectors());
        }
        return list;
    }
    
    @Override
    public List<JoinConditionImpl> getInnerJoinConditions() {
        ArrayList<JoinConditionImpl> set = new ArrayList<JoinConditionImpl>();
        switch (joinType) {
        case INNER:
            set.add(joinCondition);
            set.addAll(left.getInnerJoinConditions());
            set.addAll(right.getInnerJoinConditions());
            break;
        }
        return set;
    }

    public JoinConditionImpl getJoinCondition() {
        return joinCondition;
    }

    public SourceImpl getLeft() {
        return left;
    }

    public SourceImpl getRight() {
        return right;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String getPlan(NodeState rootState) {
        StringBuilder buff = new StringBuilder();
        buff.append(left.getPlan(rootState)).
            append(' ').
            append(joinType).
            append(' ').
            append(right.getPlan(rootState)).
            append(" on ").
            append(joinCondition);
        return buff.toString();
    }

    @Override
    public String getIndexCostInfo(NodeState rootState) {
        StringBuilder buff = new StringBuilder();
        buff.append(toString());
        buff.append("{ ");
        buff.append(left.getIndexCostInfo(rootState));
        buff.append(", ");
        buff.append(right.getIndexCostInfo(rootState));
        buff.append(" }");
        return buff.toString();
    }

    @Override
    public String toString() {
        return left + " " + joinType +
                " " + right + " on " + joinCondition;
    }
    
    @Override
    public void unprepare() {
        left.unprepare();
        right.unprepare();
        plan = null;
    }
    
    private void applyJoinConditions() {
        switch (joinType) {
        case INNER:
            left.addJoinCondition(joinCondition, false);
            right.addJoinCondition(joinCondition, true);
            break;
        case LEFT_OUTER:
            left.setOuterJoin(true, false);
            right.setOuterJoin(false, true);
            left.addJoinCondition(joinCondition, false);
            right.addJoinCondition(joinCondition, true);
            break;
        case RIGHT_OUTER:
            // swap left and right
            // TODO right outer join: verify whether converting
            // to left outer join is always correct (given the current restrictions)
            joinType = JoinType.LEFT_OUTER;
            SourceImpl temp = left;
            left = right;
            right = temp;
            left.setOuterJoin(true, false);
            right.setOuterJoin(false, true);
            left.addJoinCondition(joinCondition, false);
            right.addJoinCondition(joinCondition, true);
            break;
        }
    }
    
    @Override
    public void prepare(ExecutionPlan p) {
        if (!(p instanceof JoinExecutionPlan)) {
            throw new IllegalArgumentException("Not a join plan");
        }
        JoinExecutionPlan joinPlan = (JoinExecutionPlan) p;
        if (joinPlan.getJoin() != this) {
            throw new IllegalArgumentException("Not a plan for this join");
        }
        this.plan = joinPlan;
        applyJoinConditions();
        left.prepare(joinPlan.getLeftPlan());
        right.prepare(joinPlan.getRightPlan());
    }

    @Override
    public ExecutionPlan prepare() {
        if (plan != null) {
            return plan;
        }
        applyJoinConditions();
        // the estimated cost is the cost of the left selector,
        // plus twice the cost of the right selector (we expect
        // two rows for the right selector for each node 
        // on the left selector)
        ExecutionPlan leftPlan = left.prepare();
        ExecutionPlan rightPlan = right.prepare();
        double cost = leftPlan.getEstimatedCost() + 2 * rightPlan.getEstimatedCost();
        plan = new JoinExecutionPlan(this, leftPlan, rightPlan, cost);
        return plan;
    }

    @Override
    public SelectorImpl getSelector(String selectorName) {
        SelectorImpl s = left.getSelector(selectorName);
        if (s == null) {
            s = right.getSelector(selectorName);
        }
        return s;
    }

    @Override
    public void execute(NodeState rootState) {
        this.rootState = rootState;
        leftNeedExecute = true;
        end = false;
    }

    @Override
    public Filter createFilter(boolean preparing) {
        // TODO is a join filter needed?
        return left.createFilter(preparing);
    }

    @Override
    public void setQueryConstraint(ConstraintImpl queryConstraint) {
        left.setQueryConstraint(queryConstraint);
        right.setQueryConstraint(queryConstraint);
    }    

    @Override
    public void setOuterJoin(boolean outerJoinLeftHandSide, boolean outerJoinRightHandSide) {
        left.setOuterJoin(outerJoinLeftHandSide, outerJoinRightHandSide);
        right.setOuterJoin(outerJoinLeftHandSide, outerJoinRightHandSide);
    }
    
    @Override
    public void addJoinCondition(JoinConditionImpl joinCondition, boolean forThisSelector) {
        left.addJoinCondition(joinCondition, forThisSelector);
        right.addJoinCondition(joinCondition, forThisSelector);
    }

    @Override
    public boolean next() {
        if (end) {
            return false;
        }
        if (leftNeedExecute) {
            left.execute(rootState);
            leftNeedExecute = false;
            leftNeedNext = true;
        }
        while (true) {
            if (leftNeedNext) {
                if (!left.next()) {
                    end = true;
                    return false;
                }
                leftNeedNext = false;
                rightNeedExecute = true;
            }
            if (rightNeedExecute) {
                right.execute(rootState);
                foundJoinedRow = false;
                rightNeedExecute = false;
            }
            if (!right.next()) {
                leftNeedNext = true;
            } else {
                if (joinCondition.evaluate()) {
                    foundJoinedRow = true;
                    return true;
                }
            }
            // for an outer join, if no matching result was found,
            // one row returned (with all values set to null)
            if (right.isOuterJoinRightHandSide() && leftNeedNext && !foundJoinedRow) {
                return true;
            }
        }
    }
    
    @Override
    public boolean isOuterJoinRightHandSide() {
        return left.isOuterJoinRightHandSide() || right.isOuterJoinRightHandSide();
    }

    @Override
    public long getSize(SizePrecision precision, long max) {
        // we don't know
        return -1;
    }

    @Override
    public AstElement copyOf() {
        return new JoinImpl(
            (SourceImpl) copyElementAndCheckReference(left),
            (SourceImpl) copyElementAndCheckReference(right),
            joinType,
            (JoinConditionImpl) copyElementAndCheckReference(joinCondition)
            );
    }
}
