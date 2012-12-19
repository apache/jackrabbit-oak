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

import org.apache.jackrabbit.oak.query.Query;
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

    public JoinImpl(SourceImpl left, SourceImpl right, JoinType joinType,
            JoinConditionImpl joinCondition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
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
        return left.getPlan(rootState) + ' ' + joinType +
                " " + right.getPlan(rootState) + " on " + joinCondition;
    }

    @Override
    public String toString() {
        return left + " " + joinType +
                " " + right + " on " + joinCondition;
    }

    @Override
    public void init(Query query) {
        switch (joinType) {
        case INNER:
            left.addJoinCondition(joinCondition, false);
            right.addJoinCondition(joinCondition, true);
            break;
        case LEFT_OUTER:
            right.setOuterJoin(true);
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
            right.setOuterJoin(true);
            left.addJoinCondition(joinCondition, false);
            right.addJoinCondition(joinCondition, true);
            break;
        }
        left.setQueryConstraint(queryConstraint);
        right.setQueryConstraint(queryConstraint);
        right.init(query);
        left.init(query);
    }

    @Override
    public void prepare() {
        left.prepare();
        right.prepare();
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
            if (right.outerJoin && leftNeedNext && !foundJoinedRow) {
                return true;
            }
        }
    }

}
