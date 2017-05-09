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
package org.apache.jackrabbit.oak.query.xpath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.util.ISO9075;

/**
 * An expression.
 */
abstract class Expression {
    
    static final int PRECEDENCE_OR = 1, PRECEDENCE_AND = 2, 
            PRECEDENCE_CONDITION = 3, PRECEDENCE_OPERAND = 4;
    
    /**
     * The "and" combination of two conditions.
     * 
     * @param old the first expression (may be null)
     * @param add the second expression (may be null)
     * @return the combined expression (may be null)
     */
    public static Expression and(Expression old, Expression add) {
        if (old == null) {
            return add;
        } else if (add == null) {
            return old;
        }
        return new Expression.AndCondition(old, add);
    }
    
    /**
     * Get the optimized expression.
     * 
     * @return the optimized expression
     */
    Expression optimize() {
        return this;
    }

    /**
     * Whether this is a condition.
     * 
     * @return true if it is 
     */
    boolean isCondition() {
        return false;
    }
    
    /**
     * Whether this is a or contains a full-text condition.
     * 
     * @return true if it is
     */
    boolean containsFullTextCondition() {
        return false;
    }
    
    /**
     * Get the left-hand-side expression for equality conditions. 
     * For example, for x=1, it is x. If it is not equality, return null.
     * 
     * @return the left-hand-side expression, or null
     */        
    String getCommonLeftPart() {
        return null;
    }
    
    /**
     * Get the left hand side of an expression.
     * 
     * @return the left hand side
     */
    Expression getLeft() {
        return null;
    }
    
    /**
     * Get the list of the right hand side of an expression.
     * 
     * @return the list
     */
    List<Expression> getRight() {
        return null;
    }

    /**
     * Pull an OR condition up to the right hand side of an AND condition.
     * 
     * @return the (possibly rotated) expression
     */
    Expression pullOrRight() {
        return this;
    }
    
    /**
     * Get the operator / operation precedence. The JCR specification uses:
     * 1=OR, 2=AND, 3=condition, 4=operand  
     * 
     * @return the precedence (as an example, multiplication needs to return
     *         a higher number than addition)
     */
    int getPrecedence() {
        return PRECEDENCE_OPERAND;
    }
    
    /**
     * Get the column alias name of an expression. For a property, this is the
     * property name (no matter how many selectors the query contains); for
     * other expressions it matches the toString() method.
     * 
     * @return the simple column name
     */
    String getColumnAliasName() {
        return toString();
    }
    
    /**
     * Whether the result of this expression is a name. Names are subject to
     * ISO9075 encoding.
     * 
     * @return whether this expression is a name.
     */
    boolean isName() {
        return false;
    }
    
    /**
     * Get the most specific nodetype condition, that is a condition of the form
     * "jcr:primaryType = 'x'". If there are multiple such conditions, only the
     * most strict one needs to be returned. If there are no, or conflicting
     * conditions, then null may be returned, meaning no usable condition.
     * 
     * @param selectorName the selector name
     * @return null or the nodetype value
     */
    public String getMostSpecificNodeType(String selectorName) {
        return null;
    }

    /**
     * A literal expression.
     */
    static class Literal extends Expression {
    
        final String value;
        final String rawText;
    
        Literal(String value, String rawText) {
            this.value = value;
            this.rawText = rawText;
        }
    
        public static Expression newBoolean(boolean value) {
            return new Literal(String.valueOf(value), String.valueOf(value));
        }
    
        static Literal newNumber(String s) {
            return new Literal(s, s);
        }
    
        static Literal newString(String s) {
            return new Literal(SQL2Parser.escapeStringLiteral(s), s);
        }
    
        @Override
        public String toString() {
            return value;
        }
    
    }

    /**
     * A condition.
     */
    static class Condition extends Expression {
    
        final Expression left;
        final String operator;
        Expression right;
        final int precedence;
    
        /**
         * Create a new condition.
         * 
         * @param left the left hand side operator, or null
         * @param operator the operator
         * @param right the right hand side operator, or null
         * @param precedence the operator precedence (Expression.PRECEDENCE_...)
         */
        Condition(Expression left, String operator, Expression right, int precedence) {
            this.left = left;
            this.operator = operator;
            this.right = right;
            this.precedence = precedence;
        }
        
        @Override
        int getPrecedence() {
            return precedence;
        }
             
        @Override
        String getCommonLeftPart() {
            if (!"=".equals(operator)) {
                return null;
            }
            return left.toString();
        }
        
        @Override
        public String getMostSpecificNodeType(String selectorName) {
            if (!"=".equals(operator)) {
                return null;
            }
            if (!(left instanceof Property)) {
                return null;
            }
            Property p = (Property) left;
            if (!(right instanceof Literal)) {
                return null;
            }
            Literal l = (Literal) right;
            if (!"jcr:primaryType".equals(p.name)) {
                return null;
            }
            if (selectorName != null && !selectorName.equals(p.selector.name)) {
                return null;
            }
            return l.rawText;
        }   
        
        @Override
        Expression getLeft() {
            return left;
        }
        
        @Override
        List<Expression> getRight() {
            return Collections.singletonList(right);
        }
    
        @Override
        public String toString() {
            String leftExpr;
            boolean leftExprIsName;
            if (left == null) {
                leftExprIsName = false;
                leftExpr = "";
            } else {
                leftExprIsName = left.isName();
                leftExpr = left.toString();
                if (left.getPrecedence() < precedence) {
                    leftExpr = "(" + leftExpr + ")";
                }
            }
            boolean impossible = false;
            String rightExpr;
            if (right == null) {
                rightExpr = "";
            } else {
                if (leftExprIsName && !"like".equals(operator)) {
                    // need to de-escape _x0020_ and so on
                    if (!(right instanceof Literal)) {
                        throw new IllegalArgumentException(
                                "Can only compare a name against a string literal, not " + right);
                    }
                    Literal l = (Literal) right;
                    String raw = l.rawText;
                    String decoded = ISO9075.decode(raw);
                    String encoded = ISO9075.encode(decoded);
                    rightExpr = SQL2Parser.escapeStringLiteral(decoded);
                    if (!encoded.equalsIgnoreCase(raw)) {
                        // nothing can potentially match
                        impossible = true;
                    }
                } else {
                    rightExpr = right.toString();
                }
                if (right.getPrecedence() < precedence) {
                    rightExpr = "(" + right + ")";
                }
            }
            if (impossible) {
                // a condition that can not possibly be true
                return "upper(" + leftExpr + ") = 'never matches'";
            }
            return (leftExpr + " " + operator + " " + rightExpr).trim();
        }
    
        @Override
        boolean isCondition() {
            return true;
        }
        
        @Override
        Expression optimize() {
            return this;
        }
    
    }
    
    /**
     * An "or" condition.
     */
    static class OrCondition extends Condition {

        OrCondition(Expression left, Expression right) {
            super(left, "or", right, Expression.PRECEDENCE_OR);
        }

        /**
         * Get the left-hand-side expression if it is the same for
         * both sides. For example, for x=1 or x=2, it is x,
         * but for x=1 or y=2, it is null
         * 
         * @return the left-hand-side expression, or null
         */
        @Override
        public String getCommonLeftPart() {
            String l = left.getCommonLeftPart();
            String r = right.getCommonLeftPart();
            if (l != null && r != null && l.equals(r)) {
                return l;
            }
            return null;
        }
        
        @Override
        Expression optimize() {
            Expression l = left.optimize();
            Expression r = right.optimize();
            if (l != left || r != right) {
                return new OrCondition(l, r).optimize();
            }
            String commonLeft = getCommonLeftPart();
            if (commonLeft == null) {
                // the case:
                // (other>0 or x=1) or x=2
                // can be converted to:
                // other>0 or (x=1 or x=2)
                // which can then be optimized
                if (left instanceof OrCondition) {
                    OrCondition orLeft = (OrCondition) left;
                    Expression l1 = orLeft.left;
                    Expression l2 = orLeft.right;
                    OrCondition orRight = new OrCondition(l2, right);
                    Expression o2 = orRight.optimize();
                    if (o2 != orRight) {
                        return new OrCondition(l1, o2);
                    }
                }
                return this;
            }
            // "@x = 1 or @x = 2" is converted to "@x in (1, 2)"
            if (left instanceof InCondition) {
                InCondition in = (InCondition) left;
                in.list.addAll(right.getRight());
                // return a new instance, because we changed
                // the list
                in = new InCondition(in.getLeft(), in.list);
                return in;
            }
            Expression le = left.getLeft();
            if (XPathToSQL2Converter.NODETYPE_UNION) {
                if (commonLeft.endsWith("[jcr:primaryType]")) {
                    return this;
                }
            }
            ArrayList<Expression> list = new ArrayList<Expression>();
            list.addAll(left.getRight());
            list.addAll(right.getRight());
            InCondition in = new InCondition(le, list);
            return in;
        }
        
        @Override
        boolean containsFullTextCondition() {
            return left.containsFullTextCondition() || right.containsFullTextCondition();
        }
        
    }
    
    /**
     * An "or" condition.
     */
    static class InCondition extends Expression {

        final Expression left;
        final List<Expression> list;
        
        InCondition(Expression left, List<Expression> list) {
            this.left = left;
            this.list = list;
        }
        
        @Override
        String getCommonLeftPart() {
            return left.toString();
        }
        
        @Override
        Expression getLeft() {
            return left;
        }
        
        @Override
        List<Expression> getRight() {
            return list;
        }
    
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            buff.append(left).append(" in(");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(list.get(i));
            }
            return buff.append(')').toString();
        }
    
        @Override
        boolean isCondition() {
            return true;
        }        
        
    }
    
    /**
     * An "and" condition.
     */
    static class AndCondition extends Condition {

        AndCondition(Expression left, Expression right) {
            super(left, "and", right, Expression.PRECEDENCE_AND);
        }

        @Override
        Expression optimize() {
            Expression l = left.optimize();
            Expression r = right.optimize();
            if (l != left || r != right) {
                return new AndCondition(l, r);
            }
            return this;
        }
        
        @Override
        public String getMostSpecificNodeType(String selectorName) {
            String nt = left.getMostSpecificNodeType(selectorName);
            if (nt != null) {
                return nt;
            }
            return right.getMostSpecificNodeType(selectorName);
        }
        
        @Override
        AndCondition pullOrRight() {
            ArrayList<Expression> list = getAllAndConditions();
            OrCondition or = null;
            Expression result = null;
            for(Expression e : list) {
                if (e instanceof OrCondition && or == null) {
                    or = (OrCondition) e;
                } else if (result == null) {
                    result = e;
                } else {
                    result = new AndCondition(result, e);
                }
            }
            if (or != null) {
                result = new AndCondition(result, or);
            }
            return (AndCondition) result;
        }
        
        private ArrayList<Expression> getAllAndConditions() {
            ArrayList<Expression> list = new ArrayList<Expression>();
            if (left instanceof AndCondition) {
                list.addAll(((AndCondition) left).getAllAndConditions());
            } else {
                list.add(left);
            }
            if (right instanceof AndCondition) {
                list.addAll(((AndCondition) right).getAllAndConditions());
            } else {
                list.add(right);
            }
            return list;
        }
        
        @Override
        boolean containsFullTextCondition() {
            return left.containsFullTextCondition() || right.containsFullTextCondition();
        }
        
    }
    
    /**
     * A contains call.
     */
    static class Contains extends Expression {
        
        final Expression left, right;
    
        Contains(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }
    
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("contains(");
            Expression l = left;
            if (l instanceof Property) {
                Property p = (Property) l;
                if (p.thereWasNoAt) {
                    l = new Property(p.selector, p.name + "/*", true);
                }
            }
            buff.append(l);
            buff.append(", ").append(right).append(')');
            return buff.toString();
        }
    
        @Override
        boolean isCondition() {
            return true;
        }
        
        @Override
        boolean containsFullTextCondition() {
            return true;
        }
        
        @Override
        boolean isName() {
            return left.isName();
        }
    
    }    
    
    /**
     * A native call.
     */
    static class NativeFunction extends Expression {
        
        final String selector;
        final Expression language, expression;
    
        NativeFunction(String selector, Expression language, Expression expression) {
            this.selector = selector;
            this.language = language;
            this.expression = expression;
        }
    
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("native(");
            buff.append(selector);
            buff.append(", ").append(language).append(", ").append(expression).append(')');
            return buff.toString();
        }
    
        @Override
        boolean isCondition() {
            return true;
        }

        @Override
        boolean containsFullTextCondition() {
            return true;
        }
        
        @Override
        boolean isName() {
            return false;
        }
    
    } 
    
    /**
     * A rep:similar condition.
     */
    static class Similar extends Expression {
        
        final Expression property, path;
    
        Similar(Expression property, Expression path) {
            this.property = property;
            this.path = path;
        }
    
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("similar(");
            buff.append(property);
            buff.append(", ").append(path).append(')');
            return buff.toString();
        }
    
        @Override
        boolean isCondition() {
            return true;
        }
        
        @Override
        boolean isName() {
            return false;
        }
    
    }

    /**
     * A rep:spellcheck condition.
     */
    static class Spellcheck extends Expression {

        final Expression term;

        Spellcheck(Expression term) {
            this.term = term;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("spellcheck(");
            buff.append(term);
            buff.append(')');
            return buff.toString();
        }

        @Override
        boolean isCondition() {
            return true;
        }

        @Override
        boolean isName() {
            return false;
        }

    }

    /**
     * A rep:suggest condition.
     */
    static class Suggest extends Expression {

        final Expression term;

        Suggest(Expression term) {
            this.term = term;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("suggest(");
            buff.append(term);
            buff.append(')');
            return buff.toString();
        }

        @Override
        boolean isCondition() {
            return true;
        }

        @Override
        boolean isName() {
            return false;
        }

    }

    /**
     * A function call.
     */
    static class Function extends Expression {
    
        final String name;
        final ArrayList<Expression> params = new ArrayList<Expression>();
    
        Function(String name) {
            this.name = name;
        }
    
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder(name);
            buff.append('(');
            for (int i = 0; i < params.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(params.get(i).toString());
            }
            buff.append(')');
            return buff.toString();
        }
    
        @Override
        boolean isCondition() {
            return name.equals("contains") || name.equals("not");
        }
        
        @Override
        boolean isName() {
            if ("upper".equals(name) || "lower".equals(name)) {
                return params.get(0).isName();
            }
            return "name".equals(name);
        }
    
    }

    /**
     * A cast operation.
     */
    static class Cast extends Expression {
    
        final Expression expr;
        final String type;
    
        Cast(Expression expr, String type) {
            this.expr = expr;
            this.type = type;
        }
    
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("cast(");
            buff.append(expr.toString());
            buff.append(" as ").append(type).append(')');
            return buff.toString();
        }
    
        @Override
        boolean isCondition() {
            return false;
        }
    
    }

    /**
     * A selector parameter.
     */
    static class SelectorExpr extends Expression {
    
        private final Selector selector;
    
        SelectorExpr(Selector selector) {
            this.selector = selector;
        }
    
        @Override
        public String toString() {
            return selector.name;
        }
    
    }

    /**
     * A property expression.
     */
    static class Property extends Expression {
    
        final Selector selector;
        final String name;
        private String cacheString;
        private boolean cacheOnlySelector;
        
        /**
         * If there was no "@" character in front of the property name. If that
         * was the case, then it is still considered a property, except for
         * "contains(x, 'y')", where "x" is considered to be a node.
         */
        final boolean thereWasNoAt;
    
        Property(Selector selector, String name, boolean thereWasNoAt) {
            this.selector = selector;
            this.name = name;
            this.thereWasNoAt = thereWasNoAt;
        }
    
        @Override
        public String toString() {
            if (cacheString != null) {
                if (cacheOnlySelector == selector.onlySelector) {
                    return cacheString;
                }
            }
            StringBuilder buff = new StringBuilder();
            if (!selector.onlySelector) {
                buff.append(selector.name).append('.');
            }
            if (name.equals("*")) {
                buff.append('*');
            } else {
                buff.append('[').append(name).append(']');
            }
            cacheString = buff.toString();
            cacheOnlySelector = selector.onlySelector;
            return cacheString;
        }
        
        @Override
        public String getColumnAliasName() {
            return name;
        }
    
    }

}