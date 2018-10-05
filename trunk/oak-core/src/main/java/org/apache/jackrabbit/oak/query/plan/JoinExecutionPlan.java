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
package org.apache.jackrabbit.oak.query.plan;

import org.apache.jackrabbit.oak.query.ast.JoinImpl;


/**
 * An execution plan for a join.
 */
public class JoinExecutionPlan implements ExecutionPlan {

    private final JoinImpl join;
    private final ExecutionPlan leftPlan, rightPlan;
    private final double estimatedCost;
    
    public JoinExecutionPlan(JoinImpl join, ExecutionPlan leftPlan, ExecutionPlan rightPlan, double estimatedCost) {
        this.join = join;
        this.leftPlan = leftPlan;
        this.rightPlan = rightPlan;
        this.estimatedCost = estimatedCost;
    }
    
    @Override
    public double getEstimatedCost() {
        return estimatedCost;
    }

    public JoinImpl getJoin() {
        return join;
    }

    public ExecutionPlan getLeftPlan() {
        return leftPlan;
    }

    public ExecutionPlan getRightPlan() {
        return rightPlan;
    }

}
