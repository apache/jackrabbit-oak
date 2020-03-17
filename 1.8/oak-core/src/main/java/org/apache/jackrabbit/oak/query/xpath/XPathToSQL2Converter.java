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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.QueryOptions;
import org.apache.jackrabbit.oak.query.QueryOptions.Traversal;
import org.apache.jackrabbit.oak.query.xpath.Statement.UnionStatement;
import org.apache.jackrabbit.util.ISO9075;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Locale;

/**
 * This class can can convert a XPATH query to a SQL2 query.
 */
public class XPathToSQL2Converter {
    
    /**
     * Optimize queries of the form "from [nt:base] where [jcr:primaryType] = 'x'" 
     * to "from [x] where [jcr:primaryType] = 'x'".
     * Enabled by default.
     */
    public static final boolean NODETYPE_OPTIMIZATION = Boolean.parseBoolean(
            System.getProperty("oak.xpathNodeTypeOptimization", "true"));
    
    /**
     * Convert queries of the form "where [jcr:primaryType] = 'x' or [jcr:primaryType] = 'y'"
     * to "select ... where [jcr:primaryType] = 'x' union select ... where [jcr:primaryType] = 'y'". 
     * If disabled, only one query with "where [jcr:primaryType] in ('x', 'y') is used.
     * Enabled by default.
     */
    public static final boolean NODETYPE_UNION = Boolean.parseBoolean(
            System.getProperty("oak.xpathNodeTypeUnion", "true"));

    static final Logger LOG = LoggerFactory.getLogger(XPathToSQL2Converter.class);

    // Character types, used during the tokenizer phase
    private static final int CHAR_END = -1, CHAR_VALUE = 2;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5, CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DECIMAL = 8;

    // Token types
    private static final int KEYWORD = 1, IDENTIFIER = 2, END = 4, VALUE_STRING = 5, VALUE_NUMBER = 6;
    private static final int MINUS = 12, PLUS = 13, OPEN = 14, CLOSE = 15;

    // The query as an array of characters and character types
    private String statement;
    private char[] statementChars;
    private int[] characterTypes;

    // The current state of the parser
    private int parseIndex;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private ArrayList<String> expected;
    private Selector currentSelector = new Selector();
    private ArrayList<Selector> selectors = new ArrayList<Selector>();

    /**
     * Convert the query to SQL2.
     *
     * @param query the query string
     * @return the SQL2 query
     * @throws ParseException if parsing fails
     */
    public String convert(String query) throws ParseException {
        Statement statement = convertToStatement(query);
        statement = statement.optimize();
        return statement.toString();
    }
    
    private Statement convertToStatement(String query) throws ParseException {
        
        query = query.trim();
        
        Statement statement = new Statement();

        if (query.startsWith("explain ")) {
            query = query.substring("explain".length()).trim();
            statement.setExplain(true);
        }
        if (query.startsWith("measure")) {
            query = query.substring("measure".length()).trim();
            statement.setMeasure(true);
        }
        
        if (query.isEmpty()) {
            // special case, will always result in an empty result
            query = "//jcr:root";
        }
        
        statement.setOriginalQuery(query);
        
        initialize(query);
        
        expected = new ArrayList<String>();
        read();
        
        if (currentTokenType == END) {
            throw getSyntaxError("the query may not be empty");
        }

        currentSelector.name = "a";

        String pathPattern = "";
        boolean startOfQuery = true;

        while (true) {
            
            // if true, path or nodeType conditions are not allowed
            boolean shortcut = false;
            boolean slash = readIf("/");
            
            if (!slash) {
                if (startOfQuery) {
                    // the query doesn't start with "/"
                    currentSelector.path = "/";
                    pathPattern = "/";
                    currentSelector.isChild = true;
                } else {
                    break;
                }
            } else if (readIf("jcr:root")) {
                // "/jcr:root" may only appear at the beginning
                if (!pathPattern.isEmpty()) {
                    throw getSyntaxError("jcr:root needs to be at the beginning");
                }
                if (readIf("/")) {
                    // "/jcr:root/"
                    currentSelector.path = "/";
                    pathPattern = "/";
                    if (readIf("/")) {
                        // "/jcr:root//"
                        pathPattern = "//";
                        currentSelector.isDescendant = true;
                    } else {
                        currentSelector.isChild = true;
                    }
                } else {
                    // for example "/jcr:root[condition]"
                    pathPattern = "/%";
                    currentSelector.path = "/";
                    shortcut = true;
                }
            } else if (readIf("/")) {
                // "//" was read
                pathPattern += "%";
                if (currentSelector.isDescendant) {
                    // the query started with "//", and now "//" was read
                    nextSelector(true);
                }
                currentSelector.isDescendant = true;
            } else {
                // the token "/" was read
                pathPattern += "/";
                if (startOfQuery) {
                    currentSelector.path = "/";
                } else {
                    if (currentSelector.isDescendant) {
                        // the query started with "//", and now "/" was read
                        nextSelector(true);
                    }
                    currentSelector.isChild = true;
                }
            }
            int startParseIndex = parseIndex;
            if (shortcut) {
                // "*" and so on are not allowed now
            } else if (readIf("*")) {
                // "...*"
                pathPattern += "%";
                if (!currentSelector.isDescendant) {
                    if (selectors.size() == 0 && currentSelector.path.equals("")) {
                        // the query /* is special
                        currentSelector.path = "/";
                    }
                }
            } else if (currentTokenType == IDENTIFIER) {
                // probably a path restriction
                // String name = readPathSegment();
                String identifier = readIdentifier();
                if (readIf("(")) {
                    if ("text".equals(identifier)) {
                        // "...text()"
                        currentSelector.isChild = false;
                        pathPattern += "jcr:xmltext";
                        read(")");
                        if (currentSelector.isDescendant) {
                            currentSelector.nodeName = "jcr:xmltext";
                        } else {
                            currentSelector.path = PathUtils.concat(currentSelector.path, "jcr:xmltext");
                        }                        
                    } else if ("element".equals(identifier)) {
                        // "...element(..."
                        if (readIf(")")) {
                            // any
                            pathPattern += "%";
                        } else {
                            if (readIf("*")) {
                                // any
                                pathPattern += "%";
                            } else {
                                String name = readPathSegment();
                                pathPattern += name;
                                appendNodeName(name);
                            }
                            if (readIf(",")) {
                                currentSelector.nodeType = readIdentifier();
                            }
                            read(")");
                        }
                    } else if ("rep:excerpt".equals(identifier)) {
                        readOpenDotClose(false);
                        rewindSelector();
                        Expression.Property p = new Expression.Property(currentSelector, "rep:excerpt", false);
                        statement.addSelectColumn(p);
                    } else {
                        throw getSyntaxError();
                    }
                } else {
                    String name = ISO9075.decode(identifier);
                    pathPattern += name;
                    appendNodeName(name);
                }
            } else if (readIf("@")) {
                rewindSelector();
                Expression.Property p = readProperty();
                statement.addSelectColumn(p);
            } else if (readIf("(")) {
                rewindSelector();
                do {
                    if (readIf("@")) {
                        Expression.Property p = readProperty();
                        statement.addSelectColumn(p);
                    } else if (readIf("rep:excerpt")) {
                        readOpenDotClose(true);
                        Expression.Property p = new Expression.Property(currentSelector, "rep:excerpt", false);
                        statement.addSelectColumn(p);
                    } else if (readIf("rep:spellcheck")) {
                        // only rep:spellcheck() is currently supported
                        read("(");
                        read(")");                        
                        Expression.Property p = new Expression.Property(currentSelector, "rep:spellcheck()", false);
                        statement.addSelectColumn(p);
                    } else if (readIf("rep:suggest")) {
                        readOpenDotClose(true);
                        Expression.Property p = new Expression.Property(currentSelector, "rep:suggest()", false);
                        statement.addSelectColumn(p);
                    } else if (readIf("rep:facet")) {
                        // this will also deal with relative properties
                        // (functions and so on are also working, but this is probably not needed)
                        read("(");
                        Expression e = parseExpression();
                        if (!(e instanceof Expression.Property)) {
                            throw getSyntaxError();
                        }
                        Expression.Property prop = (Expression.Property) e;
                        String property = prop.getColumnAliasName();
                        read(")");
                        rewindSelector();
                        Expression.Property p = new Expression.Property(currentSelector,
                                "rep:facet(" + property + ")", false);
                        statement.addSelectColumn(p);
                    }
                } while (readIf("|"));
                if (!readIf(")")) {
                    return convertToUnion(query, statement, startParseIndex - 1);
                }
            } else if (readIf(".")) {
                // just "." this is simply ignored, so that
                // "a/./b" is the same as "a/b"
                if (readIf(".")) {
                    // ".." means "the parent of the node"
                    // handle like a regular path restriction
                    String name = "..";
                    pathPattern += name;
                    if (!currentSelector.isChild) {
                        currentSelector.nodeName = name;
                    } else {
                        if (currentSelector.isChild) {
                            currentSelector.isChild = false;
                            currentSelector.isParent = true;
                        }
                    }
                } else {
                    if (selectors.size() > 0) {
                        currentSelector = selectors.remove(selectors.size() - 1);
                        currentSelector.condition = null;
                        currentSelector.joinCondition = null;
                    }
                }
            } else {
                throw getSyntaxError();
            }
            if (readIf("[")) {
                do {
                    Expression c = parseConstraint();
                    currentSelector.condition = Expression.and(currentSelector.condition, c);
                    read("]");
                } while (readIf("["));
            }
            startOfQuery = false;
            nextSelector(false);
        }
        if (selectors.size() == 0) {
            nextSelector(true);
        }
        // the current selector wasn't used so far
        // go back to the last one
        currentSelector = selectors.get(selectors.size() - 1);
        if (selectors.size() == 1) {
            currentSelector.onlySelector = true;
        }
        if (readIf("order")) {
            read("by");
            do {
                Order order = new Order();
                order.expr = parseExpression();
                if (readIf("descending")) {
                    order.descending = true;
                } else {
                    readIf("ascending");
                }
                statement.addOrderBy(order);
            } while (readIf(","));
        }
        QueryOptions options = null;
        if (readIf("option")) {
            read("(");
            options = new QueryOptions();
            while (true) {
                if (readIf("traversal")) {
                    String type = readIdentifier().toUpperCase(Locale.ENGLISH);
                    options.traversal = Traversal.valueOf(type);
                } else if (readIf("index")) {
                    if (readIf("name")) {
                        options.indexName = readIdentifier();
                    } else if (readIf("tag")) {
                        options.indexTag = readIdentifier();
                    }
                } else {
                    break;
                }
                readIf(",");
            }
            read(")");
        }
        if (!currentToken.isEmpty()) {
            throw getSyntaxError("<end>");
        }
        statement.setColumnSelector(currentSelector);
        statement.setSelectors(selectors);
        statement.setQueryOptions(options);
        
        Expression where = null;
        for (Selector s : selectors) {
            where = Expression.and(where, s.condition);
        }
        statement.setWhere(where);
        return statement;
    }
    
    private void appendNodeName(String name) {
        if (!currentSelector.isChild) {
            currentSelector.nodeName = name;
        } else {
            if (selectors.size() > 0) {
                // no explicit path restriction - so it's a node name restriction
                currentSelector.isChild = true;
                currentSelector.nodeName = name;
            } else {
                currentSelector.isChild = false;
                String oldPath = currentSelector.path;
                // further extending the path
                currentSelector.path = PathUtils.concat(oldPath, name);
            }
        }
    }
    
    /**
     * Switch back to the old selector when reading a property. This occurs
     * after reading a "/", but then reading a property or a list of properties.
     * For example ".../(@prop)" is actually not a child node, but the same node
     * (selector) as before.
     */
    private void rewindSelector() {
        if (selectors.size() > 0) {
            currentSelector = selectors.remove(selectors.size() - 1);
            // prevent (join) conditions are added again
            currentSelector.isChild = false;
            currentSelector.isDescendant = false;
            currentSelector.path = "";
            currentSelector.nodeName = null;
        }
    }

    private void nextSelector(boolean force) throws ParseException {
        boolean isFirstSelector = selectors.size() == 0;
        String path = currentSelector.path;
        Expression condition = currentSelector.condition;
        Expression joinCondition = null;
        if (currentSelector.nodeName != null) {
            Expression.Function f = new Expression.Function("name");
            f.params.add(new Expression.SelectorExpr(currentSelector));
            String n = currentSelector.nodeName;
            // encode again, because it will be decoded again
            n = ISO9075.encode(n);
            Expression.Condition c = new Expression.Condition(f, "=", 
                    Expression.Literal.newString(n), 
                    Expression.PRECEDENCE_CONDITION);
            condition = Expression.and(condition, c);
        }
        if (currentSelector.isDescendant) {
            if (isFirstSelector) {
                if (!path.isEmpty()) {
                    if (!PathUtils.isAbsolute(path)) {
                        path = PathUtils.concat("/", path);
                    }
                    Expression.Function c = new Expression.Function("isdescendantnode");
                    c.params.add(new Expression.SelectorExpr(currentSelector));
                    c.params.add(Expression.Literal.newString(path));
                    condition = Expression.and(condition, c);
                }
            } else {
                Expression.Function c = new Expression.Function("isdescendantnode");
                c.params.add(new Expression.SelectorExpr(currentSelector));
                c.params.add(new Expression.SelectorExpr(selectors.get(selectors.size() - 1)));
                joinCondition = c;
            } 
        } else if (currentSelector.isParent) {
            if (isFirstSelector) {
                throw getSyntaxError();
            } else {
                Expression.Function c = new Expression.Function("ischildnode");
                c.params.add(new Expression.SelectorExpr(selectors.get(selectors.size() - 1)));
                c.params.add(new Expression.SelectorExpr(currentSelector));
                joinCondition = c;
            }
        } else if (currentSelector.isChild) {
            if (isFirstSelector) {
                if (!path.isEmpty()) {
                    if (!PathUtils.isAbsolute(path)) {
                        path = PathUtils.concat("/", path);
                    }
                    Expression.Function c = new Expression.Function("ischildnode");
                    c.params.add(new Expression.SelectorExpr(currentSelector));
                    c.params.add(Expression.Literal.newString(path));
                    condition = Expression.and(condition, c);
                }
            } else {
                Expression.Function c = new Expression.Function("ischildnode");
                c.params.add(new Expression.SelectorExpr(currentSelector));
                c.params.add(new Expression.SelectorExpr(selectors.get(selectors.size() - 1)));
                joinCondition = c;
            }
        } else {
            if (!force && condition == null && joinCondition == null) {
                // a child node of a given path, such as "/test"
                // use the same selector for now, and extend the path
            } else if (PathUtils.isAbsolute(path)) {
                Expression.Function c = new Expression.Function("issamenode");
                c.params.add(new Expression.SelectorExpr(currentSelector));
                c.params.add(Expression.Literal.newString(path));
                condition = Expression.and(condition, c);
            }
        }
        if (force || condition != null || joinCondition != null) {
            String nextSelectorName = "" + (char) (currentSelector.name.charAt(0) + 1);
            if (nextSelectorName.compareTo("x") > 0) {
                throw getSyntaxError("too many joins");
            }
            Selector nextSelector = new Selector();
            nextSelector.name = nextSelectorName;
            currentSelector.condition = condition;
            currentSelector.joinCondition = Expression.and(currentSelector.joinCondition, joinCondition);
            selectors.add(currentSelector);
            currentSelector = nextSelector;
        }
    }

    private Expression parseConstraint() throws ParseException {
        Expression a = parseAnd();
        int i = 0;
        while (readIf("or")) {
            a = new Expression.OrCondition(a, parseAnd());
            if (++i % 100 == 0) {
                a = a.optimize();
            }
        }
        return a.optimize();
    }

    private Expression parseAnd() throws ParseException {
        Expression a = parseCondition();
        while (readIf("and")) {
            a = new Expression.AndCondition(a, parseCondition());
        }
        return a.optimize();
    }

    private Expression parseCondition() throws ParseException {
        Expression a;
        if (readIf("fn:not") || readIf("not")) {
            read("(");
            a = parseConstraint();
            if (a instanceof Expression.Condition && ((Expression.Condition) a).operator.equals("is not null")) {
                // not(@property) -> @property is null
                Expression.Condition c = (Expression.Condition) a;
                c = new Expression.Condition(c.left, "is null", null, Expression.PRECEDENCE_CONDITION);
                a = c;
            } else {
                Expression.Function f = new Expression.Function("not");
                f.params.add(a);
                a = f;
            }
            read(")");
        } else if (readIf("(")) {
            a = parseConstraint();
            read(")");
        } else {
            Expression e = parseExpression();
            if (e.isCondition()) {
                return e;
            }
            a = parseCondition(e);
        }
        return a.optimize();
    }

    private Expression.Condition parseCondition(Expression left) throws ParseException {
        Expression.Condition c;
        if (readIf("=")) {
            c = new Expression.Condition(left, "=", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("<>")) {
            c = new Expression.Condition(left, "<>", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("!=")) {
            c = new Expression.Condition(left, "<>", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("<")) {
            c = new Expression.Condition(left, "<", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf(">")) {
            c = new Expression.Condition(left, ">", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf("<=")) {
            c = new Expression.Condition(left, "<=", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else if (readIf(">=")) {
            c = new Expression.Condition(left, ">=", parseExpression(), Expression.PRECEDENCE_CONDITION);
        // TODO support "x eq y"? it seems this only matches for single value properties?  
        // } else if (readIf("eq")) {
        //    c = new Condition(left, "==", parseExpression(), Expression.PRECEDENCE_CONDITION);
        } else {
            c = new Expression.Condition(left, "is not null", null, Expression.PRECEDENCE_CONDITION);
        }
        return c;
    }

    private Expression parseExpression() throws ParseException {
        if (readIf("@")) {
            return readProperty();
        } else if (readIf("true")) {
            if (readIf("(")) {
                read(")");
            }
            return Expression.Literal.newBoolean(true);
        } else if (readIf("false")) {
            if (readIf("(")) {
                read(")");
            }
            return Expression.Literal.newBoolean(false);
        } else if (currentTokenType == VALUE_NUMBER) {
            Expression.Literal l = Expression.Literal.newNumber(currentToken);
            read();
            return l;
        } else if (currentTokenType == VALUE_STRING) {
            Expression.Literal l = Expression.Literal.newString(currentToken);
            read();
            return l;
        } else if (readIf("-")) {
            if (currentTokenType != VALUE_NUMBER) {
                throw getSyntaxError();
            }
            Expression.Literal l = Expression.Literal.newNumber('-' + currentToken);
            read();
            return l;
        } else if (readIf("+")) {
            if (currentTokenType != VALUE_NUMBER) {
                throw getSyntaxError();
            }
            return parseExpression();
        } else {
            return parsePropertyOrFunction();
        }
    }

    private Expression parsePropertyOrFunction() throws ParseException {
        StringBuilder buff = new StringBuilder();
        boolean isPath = false;
        while (true) {
            if (currentTokenType == IDENTIFIER) {
                String name = readPathSegment();
                buff.append(name);
            } else if (readIf("*")) {
                // any node
                buff.append('*');
                isPath = true;
            } else if (readIf(".")) {
                buff.append('.');
                if (readIf(".")) {
                    buff.append('.');
                }
                isPath = true;
            } else if (readIf("@")) {
                if (readIf("*")) {
                    // xpath supports @*, even thought jackrabbit may not
                    buff.append('*');
                } else {
                    buff.append(readPathSegment());
                }
                return new Expression.Property(currentSelector, buff.toString(), false);
            } else {
                break;
            }
            if (readIf("/")) {
                isPath = true;
                buff.append('/');
            } else {
                break;
            }
        }
        if (!isPath && readIf("(")) {
            return parseFunction(buff.toString());
        } else if (buff.length() > 0) {
            // path without all attributes, as in:
            // jcr:contains(jcr:content, 'x')
            if (buff.toString().equals(".")) {
                return new Expression.Property(currentSelector, "*", false);
            }
            return new Expression.Property(currentSelector, buff.toString(), true);
        }
        throw getSyntaxError();
    }

    private Expression parseFunction(String functionName) throws ParseException {
        if ("jcr:like".equals(functionName)) {
            Expression.Condition c = new Expression.Condition(parseExpression(), 
                    "like", null, Expression.PRECEDENCE_CONDITION);
            read(",");
            c.right = parseExpression();
            read(")");
            return c;
        } else if ("jcr:contains".equals(functionName)) {
            Expression left = parseExpression();
            read(",");
            Expression right = parseExpression();
            read(")");
            Expression.Contains f = new Expression.Contains(left, right);
            return f;
        } else if ("jcr:score".equals(functionName)) {
            Expression.Function f = new Expression.Function("score");
            f.params.add(new Expression.SelectorExpr(currentSelector));
            read(")");
            return f;
        } else if ("xs:dateTime".equals(functionName)) {
            Expression expr = parseExpression();
            Expression.Cast c = new Expression.Cast(expr, "date");
            read(")");
            return c;
        } else if ("fn:coalesce".equals(functionName)) {
            Expression.Function f = new Expression.Function("coalesce");
            f.params.add(parseExpression());
            read(",");
            f.params.add(parseExpression());
            read(")");
            return f;
        } else if ("fn:lower-case".equals(functionName)) {
            Expression.Function f = new Expression.Function("lower");
            f.params.add(parseExpression());
            read(")");
            return f;
        } else if ("fn:upper-case".equals(functionName)) {
            Expression.Function f = new Expression.Function("upper");
            f.params.add(parseExpression());
            read(")");
            return f;
        } else if ("fn:string-length".equals(functionName)) {
            Expression.Function f = new Expression.Function("length");
            f.params.add(parseExpression());
            read(")");
            return f;
        } else if ("fn:name".equals(functionName)) {
            Expression.Function f = new Expression.Function("name");
            if (!readIf(")")) {
                // only name(.) and name() are currently supported
                read(".");
                read(")");
            }
            f.params.add(new Expression.SelectorExpr(currentSelector));
            return f;
        } else if ("fn:local-name".equals(functionName)) {
            Expression.Function f = new Expression.Function("localname");
            if (!readIf(")")) {
                // only localname(.) and localname() are currently supported
                read(".");
                read(")");
            }
            f.params.add(new Expression.SelectorExpr(currentSelector));
            return f;
        } else if ("jcr:deref".equals(functionName)) {
             // TODO maybe support jcr:deref
             throw getSyntaxError("jcr:deref is not supported");
        } else if ("rep:native".equals(functionName)) {
            String selectorName = currentSelector.name;
            Expression language = parseExpression();
            read(",");
            Expression expr = parseExpression();
            read(")");
            Expression.NativeFunction f = new Expression.NativeFunction(selectorName, language, expr);
            return f;
        } else if ("rep:similar".equals(functionName)) {
            Expression property = parseExpression();
            read(",");
            Expression path = parseExpression();
            read(")");
            Expression.Similar f = new Expression.Similar(property, path);
            return f;
        } else if ("rep:spellcheck".equals(functionName)) {
            Expression term = parseExpression();
            read(")");
            return new Expression.Spellcheck(term);
        } else if ("rep:suggest".equals(functionName)) {
            Expression term = parseExpression();
            read(")");
            return new Expression.Suggest(term);
        } else {
            throw getSyntaxError("jcr:like | jcr:contains | jcr:score | xs:dateTime | " + 
                    "fn:lower-case | fn:upper-case | fn:name | rep:similar | rep:spellcheck | rep:suggest");
        }
    }

    private boolean readIf(String token) throws ParseException {
        if (isToken(token)) {
            read();
            return true;
        }
        return false;
    }

    private boolean isToken(String token) {
        boolean result = token.equals(currentToken) && !currentTokenQuoted;
        if (result) {
            return true;
        }
        addExpected(token);
        return false;
    }

    private void read(String expected) throws ParseException {
        if (!expected.equals(currentToken) || currentTokenQuoted) {
            throw getSyntaxError(expected);
        }
        read();
    }

    private Expression.Property readProperty() throws ParseException {
        if (readIf("*")) {
            return new Expression.Property(currentSelector, "*", false);
        }
        return new Expression.Property(currentSelector, readPathSegment(), false);
    }
    
    /**
     * Read open bracket (optional), and optional dot, and close bracket.
     * 
     * @param readOpenBracket whether to read the open bracket (false if this
     *            was already read)
     * @throws ParseException if close bracket or the dot were not read
     */
    private void readOpenDotClose(boolean readOpenBracket) throws ParseException {
        if (readOpenBracket) {
            read("(");
        }
        readIf(".");
        read(")");
    }

    private String readPathSegment() throws ParseException {
        String raw = readIdentifier();
        return ISO9075.decode(raw);
    }

    private String readIdentifier() throws ParseException {
        if (currentTokenType != IDENTIFIER) {
            throw getSyntaxError("identifier");
        }
        String s = currentToken;
        read();
        return s;
    }

    private void addExpected(String token) {
        if (expected != null) {
            expected.add(token);
        }
    }

    private void initialize(String query) throws ParseException {
        if (query == null) {
            query = "";
        }
        statement = query;
        int len = query.length() + 1;
        char[] command = new char[len];
        int[] types = new int[len];
        len--;
        query.getChars(0, len, command, 0);
        command[len] = ' ';
        int startLoop = 0;
        for (int i = 0; i < len; i++) {
            char c = command[i];
            int type = 0;
            switch (c) {
            case '@':
            case '|':
            case '/':
            case '-':
            case '(':
            case ')':
            case '{':
            case '}':
            case '*':
            case ',':
            case ';':
            case '+':
            case '%':
            case '?':
            case '$':
            case '[':
            case ']':
                type = CHAR_SPECIAL_1;
                break;
            case '!':
            case '<':
            case '>':
            case '=':
                type = CHAR_SPECIAL_2;
                break;
            case '.':
                type = CHAR_DECIMAL;
                break;
            case '\'':
                type = CHAR_STRING;
                types[i] = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\'') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '\"':
                type = CHAR_STRING;
                types[i] = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\"') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case ':':
            case '_':
                type = CHAR_NAME;
                break;
            default:
                if (c >= 'a' && c <= 'z') {
                    type = CHAR_NAME;
                } else if (c >= 'A' && c <= 'Z') {
                    type = CHAR_NAME;
                } else if (c >= '0' && c <= '9') {
                    type = CHAR_VALUE;
                } else {
                    if (Character.isJavaIdentifierPart(c)) {
                        type = CHAR_NAME;
                    }
                }
            }
            types[i] = (byte) type;
        }
        statementChars = command;
        types[len] = CHAR_END;
        characterTypes = types;
        parseIndex = 0;
    }

    private void checkRunOver(int i, int len, int startLoop) throws ParseException {
        if (i >= len) {
            parseIndex = startLoop;
            throw getSyntaxError();
        }
    }

    private void read() throws ParseException {
        currentTokenQuoted = false;
        if (expected != null) {
            expected.clear();
        }
        int[] types = characterTypes;
        int i = parseIndex;
        int type = types[i];
        while (type == 0) {
            type = types[++i];
        }
        int start = i;
        char[] chars = statementChars;
        char c = chars[i++];
        currentToken = "";
        switch (type) {
        case CHAR_NAME:
            while (true) {
                type = types[i];
                // the '-' can be part of a name,
                // for example in "fn:lower-case"
                // the '.' can be part of a name,
                // for example in "@offloading.status"
                if (type != CHAR_NAME && type != CHAR_VALUE 
                        && chars[i] != '-'
                        && chars[i] != '.') {
                    break;
                }
                i++;
            }
            currentToken = statement.substring(start, i);
            if (currentToken.isEmpty()) {
                throw getSyntaxError();
            }
            currentTokenType = IDENTIFIER;
            parseIndex = i;
            return;
        case CHAR_SPECIAL_2:
            if (types[i] == CHAR_SPECIAL_2) {
                i++;
            }
            currentToken = statement.substring(start, i);
            currentTokenType = KEYWORD;
            parseIndex = i;
            break;
        case CHAR_SPECIAL_1:
            currentToken = statement.substring(start, i);
            switch (c) {
            case '+':
                currentTokenType = PLUS;
                break;
            case '-':
                currentTokenType = MINUS;
                break;
            case '(':
                currentTokenType = OPEN;
                break;
            case ')':
                currentTokenType = CLOSE;
                break;
            default:
                currentTokenType = KEYWORD;
            }
            parseIndex = i;
            return;
        case CHAR_VALUE:
            long number = c - '0';
            while (true) {
                c = chars[i];
                if (c < '0' || c > '9') {
                    if (c == '.') {
                        readDecimal(start, i);
                        break;
                    }
                    if (c == 'E' || c == 'e') {
                        readDecimal(start, i);
                        break;
                    }
                    currentTokenType = VALUE_NUMBER;
                    currentToken = String.valueOf(number);
                    parseIndex = i;
                    break;
                }
                number = number * 10 + (c - '0');
                if (number > Integer.MAX_VALUE) {
                    readDecimal(start, i);
                    break;
                }
                i++;
            }
            return;
        case CHAR_DECIMAL:
            if (types[i] != CHAR_VALUE) {
                currentTokenType = KEYWORD;
                currentToken = ".";
                parseIndex = i;
                return;
            }
            readDecimal(i - 1, i);
            return;
        case CHAR_STRING:
            currentTokenQuoted = true;
            if (chars[i - 1] == '\'') {
                readString(i, '\'');
            } else {
                readString(i, '\"');
            }
            return;
        case CHAR_END:
            currentToken = "";
            currentTokenType = END;
            parseIndex = i;
            return;
        default:
            throw getSyntaxError();
        }
    }

    private void readString(int i, char end) throws ParseException {
        char[] chars = statementChars;
        String result = null;
        while (true) {
            for (int begin = i;; i++) {
                if (chars[i] == end) {
                    if (result == null) {
                        result = statement.substring(begin, i);
                    } else {
                        result += statement.substring(begin - 1, i);
                    }
                    break;
                }
            }
            if (chars[++i] != end) {
                break;
            }
            i++;
        }
        currentToken = result;
        parseIndex = i;
        currentTokenType = VALUE_STRING;
    }

    private void readDecimal(int start, int i) throws ParseException {
        char[] chars = statementChars;
        int[] types = characterTypes;
        while (true) {
            int t = types[i];
            if (t != CHAR_DECIMAL && t != CHAR_VALUE) {
                break;
            }
            i++;
        }
        if (chars[i] == 'E' || chars[i] == 'e') {
            i++;
            if (chars[i] == '+' || chars[i] == '-') {
                i++;
            }
            if (types[i] != CHAR_VALUE) {
                throw getSyntaxError();
            }
            while (types[++i] == CHAR_VALUE) {
                // go until the first non-number
            }
        }
        parseIndex = i;
        String sub = statement.substring(start, i);
        try {
            new BigDecimal(sub);
        } catch (NumberFormatException e) {
            throw new ParseException("Data conversion error converting " + sub + " to BigDecimal: " + e, i);
        }
        currentToken = sub;
        currentTokenType = VALUE_NUMBER;
    }

    private ParseException getSyntaxError() {
        if (expected == null || expected.isEmpty()) {
            return getSyntaxError(null);
        } else {
            StringBuilder buff = new StringBuilder();
            for (String exp : expected) {
                if (buff.length() > 0) {
                    buff.append(", ");
                }
                buff.append(exp);
            }
            return getSyntaxError(buff.toString());
        }
    }

    private ParseException getSyntaxError(String expected) {
        int index = Math.max(0, Math.min(parseIndex, statement.length() - 1));
        String query = statement.substring(0, index) + "(*)" + statement.substring(index).trim();
        if (expected != null) {
            query += "; expected: " + expected;
        }
        return new ParseException("Query:\n" + query, index);
    }
    
    private Statement convertToUnion(String query, Statement statement,
            int startParseIndex) throws ParseException {
        int start = query.indexOf("(", startParseIndex);
        String begin = query.substring(0, start);
        XPathToSQL2Converter converter = new XPathToSQL2Converter();
        String partList = query.substring(start);
        converter.initialize(partList);
        converter.read();
        int lastParseIndex = converter.parseIndex;
        int lastOrIndex = lastParseIndex;
        converter.read("(");
        int level = 0;
        ArrayList<String> parts = new ArrayList<String>();
        int parseIndex;
        while (true) {
            parseIndex = converter.parseIndex;
            if (converter.readIf("(")) {
                level++;
            } else if (converter.readIf(")")) {
                if (level-- <= 0) {
                    break;
                }
            } else if (converter.readIf("|") && level == 0) {
                String or = partList.substring(lastOrIndex, parseIndex - 1);
                parts.add(or);
                lastOrIndex = parseIndex;
            } else if (converter.currentTokenType == END) {
                throw getSyntaxError("empty query or missing ')'");
            } else {
                converter.read();
            }
        }
        String or = partList.substring(lastOrIndex, parseIndex - 1);
        parts.add(or);        
        String end = partList.substring(parseIndex);
        Statement result = null;
        ArrayList<Order> orderList = null;
        QueryOptions queryOptions = null;
        for(String p : parts) {
            String q = begin + p + end;
            converter = new XPathToSQL2Converter();
            Statement stat = converter.convertToStatement(q);
            orderList = stat.orderList;
            queryOptions = stat.queryOptions;
            // reset fields that are used in the union,
            // but no longer in the individual statements
            // (can not use clear, because it is shared)
            stat.orderList = new ArrayList<Order>();
            stat.queryOptions = null;
            if (result == null) {
                result = stat;
            } else {
                UnionStatement union = new UnionStatement(result, stat);
                result = union;
            }
        }
        result.orderList = orderList;
        result.queryOptions = queryOptions;
        result.setExplain(statement.explain);
        result.setMeasure(statement.measure);
        return result;
    }

}

