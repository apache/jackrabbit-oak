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
package org.apache.jackrabbit.oak.query;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.QueryOptions.Traversal;
import org.apache.jackrabbit.oak.query.ast.AstElementFactory;
import org.apache.jackrabbit.oak.query.ast.BindVariableValueImpl;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.ConstraintImpl;
import org.apache.jackrabbit.oak.query.ast.DynamicOperandImpl;
import org.apache.jackrabbit.oak.query.ast.JoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.JoinType;
import org.apache.jackrabbit.oak.query.ast.LiteralImpl;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyExistenceImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyInexistenceImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyValueImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.ast.SourceImpl;
import org.apache.jackrabbit.oak.query.ast.StaticOperandImpl;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData.QueryExecutionStats;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SQL2 parser can convert a JCR-SQL2 query to a query. The 'old' SQL query
 * language (here named SQL-1) is also supported.
 */
public class SQL2Parser {
    
    private static final Logger LOG = LoggerFactory.getLogger(SQL2Parser.class);

    // Character types, used during the tokenizer phase
    private static final int CHAR_END = -1, CHAR_IGNORE = 0;
    private static final int CHAR_VALUE = 2, CHAR_QUOTED = 3;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5, CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DECIMAL = 8, CHAR_BRACKETED = 9;

    // Token types
    private static final int KEYWORD = 1, IDENTIFIER = 2, PARAMETER = 3, END = 4, VALUE = 5;
    private static final int MINUS = 12, PLUS = 13, OPEN = 14, CLOSE = 15;

    private final NodeTypeInfoProvider nodeTypes;

    // The query as an array of characters and character types
    private String statement;
    private char[] statementChars;
    private int[] characterTypes;

    // The current state of the parser
    private int parseIndex;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private PropertyValue currentValue;
    private ArrayList<String> expected;

    // The bind variables
    private HashMap<String, BindVariableValueImpl> bindVariables;

    // The list of selectors of this query
    private final Map<String, SelectorImpl> selectors = newHashMap();

    // SQL injection protection: if disabled, literals are not allowed
    private boolean allowTextLiterals = true;
    private boolean allowNumberLiterals = true;
    private boolean includeSelectorNameInWildcardColumns = true;

    private final AstElementFactory factory = new AstElementFactory();

    private boolean supportSQL1;

    private NamePathMapper namePathMapper;
    
    private final QueryEngineSettings settings;
    
    private boolean literalUsageLogged;

    private final QueryExecutionStats stats;

    /**
     * Create a new parser. A parser can be re-used, but it is not thread safe.
     * 
     * @param namePathMapper the name-path mapper to use
     * @param nodeTypes the nodetypes
     * @param settings the query engine settings
     */
    public SQL2Parser(NamePathMapper namePathMapper, NodeTypeInfoProvider nodeTypes, QueryEngineSettings settings,
            QueryExecutionStats stats) {
        this.namePathMapper = namePathMapper;
        this.nodeTypes = checkNotNull(nodeTypes);
        this.settings = checkNotNull(settings);
        this.stats = checkNotNull(stats);
    }

    /**
     * Parse the statement and return the query.
     *
     * @param query the query string
     * @param initialise if performing the query init ({@code true}) or not ({@code false})
     * @return the query
     * @throws ParseException if parsing fails
     */
    public Query parse(final String query, final boolean initialise) throws ParseException {
        // TODO possibly support union,... as available at
        // http://docs.jboss.org/modeshape/latest/manuals/reference/html/jcr-query-and-search.html

        initialize(query);
        selectors.clear();
        expected = new ArrayList<String>();
        bindVariables = new HashMap<String, BindVariableValueImpl>();
        read();
        boolean explain = false, measure = false;
        if (readIf("EXPLAIN")) {
            explain = true;
        }
        if (readIf("MEASURE")) {
            measure = true;
        }
        Query q = parseSelect();
        while (true) {
            if (!readIf("UNION")) {
                break;
            }
            boolean unionAll = readIf("ALL");
            QueryImpl q2 = parseSelect();
            q = new UnionQueryImpl(unionAll, q, q2, settings);
        }
        OrderingImpl[] orderings = null;
        if (readIf("ORDER")) {
            read("BY");
            orderings = parseOrder();
        }
        QueryOptions options = new QueryOptions();
        if (readIf("OPTION")) {
            read("(");
            while (true) {
                if (readIf("TRAVERSAL")) {
                    String n = readName().toUpperCase(Locale.ENGLISH);
                    options.traversal = Traversal.valueOf(n);
                } else if (readIf("INDEX")) {
                    if (readIf("NAME")) {
                        options.indexName = readName();
                    } else if (readIf("TAG")) {
                        options.indexTag = readLabel();
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
        q.setOrderings(orderings);
        q.setExplain(explain);
        q.setMeasure(measure);
        q.setInternal(isInternal(query));
        q.setQueryOptions(options);

        if (initialise) {
            try {
                q.init();
            } catch (Exception e) {
                ParseException e2 = new ParseException(statement + ": " + e.getMessage(), 0);
                e2.initCause(e);
                throw e2;
            }
        }

        return q;
    }
    
    /**
     * as {@link #parse(String, boolean)} by providing {@code true} to the initialisation flag.
     * 
     * @param query
     * @return the parsed query
     * @throws ParseException
     */
    public Query parse(final String query) throws ParseException {
        return parse(query, true);
    }
    
    private QueryImpl parseSelect() throws ParseException {
        read("SELECT");
        boolean distinct = readIf("DISTINCT");
        ArrayList<ColumnOrWildcard> list = parseColumns();
        if (supportSQL1) {
            addColumnIfNecessary(list, QueryConstants.JCR_PATH, QueryConstants.JCR_PATH);
            addColumnIfNecessary(list, QueryConstants.JCR_SCORE, QueryConstants.JCR_SCORE);
        }
        read("FROM");
        SourceImpl source = parseSource();
        ColumnImpl[] columnArray = resolveColumns(list);
        ConstraintImpl constraint = null;
        if (readIf("WHERE")) {
            constraint = parseConstraint();
        }
        QueryImpl q = new QueryImpl(
                statement, source, constraint, columnArray, namePathMapper, settings, stats);
        q.setDistinct(distinct);
        return q;
    }

    private static void addColumnIfNecessary(ArrayList<ColumnOrWildcard> list,
            String columnName, String propertyName) {
        for (ColumnOrWildcard c : list) {
            String col = c.columnName;
            if (columnName.equals(col)) {
                // it already exists
                return;
            }
        }
        ColumnOrWildcard column = new ColumnOrWildcard();
        column.columnName = columnName;
        column.propertyName = propertyName;
        list.add(column);
    }

    /**
     * Enable or disable support for SQL-1 queries.
     *
     * @param sql1 the new value
     */
    public void setSupportSQL1(boolean sql1) {
        this.supportSQL1 = sql1;
    }

    private SelectorImpl parseSelector() throws ParseException {
        String nodeTypeName = readName();
        if (namePathMapper != null) {
            try {
                nodeTypeName = namePathMapper.getOakName(nodeTypeName);
            } catch (RepositoryException e) {
                ParseException e2 = getSyntaxError("could not convert node type name " + nodeTypeName);
                e2.initCause(e);
                throw e2;
            }
        }
        NodeTypeInfo nodeTypeInfo = nodeTypes.getNodeTypeInfo(nodeTypeName);
        if (!nodeTypeInfo.exists()) {
            throw getSyntaxError("unknown node type");
        }

        String selectorName = nodeTypeName;
        if (readIf("AS")) {
            selectorName = readName();
        }

        return factory.selector(nodeTypeInfo, selectorName);
    }
    
    private String readLabel() throws ParseException {
        String label = readName();
        if (!label.matches("[a-zA-Z0-9_]*") || label.isEmpty() || label.length() > 128) {
            throw getSyntaxError("a-z, A-Z, 0-9, _");
        }
        return label;
    }

    private String readName() throws ParseException {
        if (currentTokenType == END) {
            throw getSyntaxError("a token");
        }
        String s;
        if (currentTokenType == VALUE) {
            s = currentValue.getValue(Type.STRING);
        } else {
            s = currentToken;
        }
        read();
        return s;
    }

    private SourceImpl parseSource() throws ParseException {
        SelectorImpl selector = parseSelector();
        selectors.put(selector.getSelectorName(), selector);
        SourceImpl source = selector;
        while (true) {
            JoinType joinType;
            if (readIf("RIGHT")) {
                read("OUTER");
                joinType = JoinType.RIGHT_OUTER;
            } else if (readIf("LEFT")) {
                read("OUTER");
                joinType = JoinType.LEFT_OUTER;
            } else if (readIf("INNER")) {
                joinType = JoinType.INNER;
            } else {
                break;
            }
            read("JOIN");
            selector = parseSelector();
            selectors.put(selector.getSelectorName(), selector);
            read("ON");
            JoinConditionImpl on = parseJoinCondition();
            source = factory.join(source, selector, joinType, on);
        }
        return source;
    }

    private JoinConditionImpl parseJoinCondition() throws ParseException {
        boolean identifier = currentTokenType == IDENTIFIER;
        String name = readName();
        JoinConditionImpl c;
        if (identifier && readIf("(")) {
            if ("ISSAMENODE".equalsIgnoreCase(name)) {
                String selector1 = readName();
                read(",");
                String selector2 = readName();
                if (readIf(",")) {
                    c = factory.sameNodeJoinCondition(selector1, selector2, readPath());
                } else {
                    c = factory.sameNodeJoinCondition(selector1, selector2, ".");
                }
            } else if ("ISCHILDNODE".equalsIgnoreCase(name)) {
                String childSelector = readName();
                read(",");
                c = factory.childNodeJoinCondition(childSelector, readName());
            } else if ("ISDESCENDANTNODE".equalsIgnoreCase(name)) {
                String descendantSelector = readName();
                read(",");
                c = factory.descendantNodeJoinCondition(descendantSelector, readName());
            } else {
                throw getSyntaxError("ISSAMENODE, ISCHILDNODE, or ISDESCENDANTNODE");
            }
            read(")");
            return c;
        } else {
            String selector1 = name;
            read(".");
            String property1 = readName();
            read("=");
            String selector2 = readName();
            read(".");
            return factory.equiJoinCondition(selector1, property1, selector2, readName());
        }
    }

    private ConstraintImpl parseConstraint() throws ParseException {
        ConstraintImpl a = parseAnd();
        while (readIf("OR")) {
            a = factory.or(a, parseAnd());
        }
        return a;
    }

    private ConstraintImpl parseAnd() throws ParseException {
        ConstraintImpl a = parseCondition();
        while (readIf("AND")) {
            a = factory.and(a, parseCondition());
        }
        return a;
    }

    private ConstraintImpl parseCondition() throws ParseException {
        ConstraintImpl a;
        if (readIf("NOT")) {
            a = factory.not(parseCondition());
        } else if (readIf("(")) {
            a = parseConstraint();
            read(")");
        } else if (currentTokenType == IDENTIFIER) {
            String identifier = readName();
            if (readIf("(")) {
                a = parseConditionFunctionIf(identifier);
                if (a == null) {
                    DynamicOperandImpl op = parseExpressionFunction(identifier);
                    a = parseCondition(op);
                }
            } else if (readIf(".")) {
                a = parseCondition(factory.propertyValue(identifier, readName()));
            } else {
                a = parseCondition(factory.propertyValue(getOnlySelectorName(), identifier));
            }
        } else if ("[".equals(currentToken)) {
            String name = readName();
            if (readIf(".")) {
                a = parseCondition(factory.propertyValue(name, readName()));
            } else {
                a = parseCondition(factory.propertyValue(getOnlySelectorName(), name));
            }
        } else if (supportSQL1) {
            StaticOperandImpl left = parseStaticOperand();
            if (readIf("IN")) {
                DynamicOperandImpl right = parseDynamicOperand();
                ConstraintImpl c = factory.comparison(right, Operator.EQUAL, left);
                return c;
            } else {
                throw getSyntaxError();
            }
        } else {
            throw getSyntaxError();
        }
        return a;
    }

    private ConstraintImpl parseCondition(DynamicOperandImpl left) throws ParseException {
        ConstraintImpl c;
        if (readIf("=")) {
            c = factory.comparison(left, Operator.EQUAL, parseStaticOperand());
        } else if (readIf("<>")) {
            c = factory.comparison(left, Operator.NOT_EQUAL, parseStaticOperand());
        } else if (readIf("<")) {
            c = factory.comparison(left, Operator.LESS_THAN, parseStaticOperand());
        } else if (readIf(">")) {
            c = factory.comparison(left, Operator.GREATER_THAN, parseStaticOperand());
        } else if (readIf("<=")) {
            c = factory.comparison(left, Operator.LESS_OR_EQUAL, parseStaticOperand());
        } else if (readIf(">=")) {
            c = factory.comparison(left, Operator.GREATER_OR_EQUAL, parseStaticOperand());
        } else if (readIf("LIKE")) {
            c = factory.comparison(left, Operator.LIKE, parseStaticOperand());
            if (supportSQL1) {
                if (readIf("ESCAPE")) {
                    StaticOperandImpl esc = parseStaticOperand();
                    if (!(esc instanceof LiteralImpl)) {
                        throw getSyntaxError("only ESCAPE '\' is supported");
                    }
                    PropertyValue v = ((LiteralImpl) esc).getLiteralValue();
                    if (!v.getValue(Type.STRING).equals("\\")) {
                        throw getSyntaxError("only ESCAPE '\' is supported");
                    }
                }
            }
        } else if (readIf("IN")) {
            read("(");
            ArrayList<StaticOperandImpl> list = new ArrayList<StaticOperandImpl>();
            do {
                StaticOperandImpl x = parseStaticOperand();
                list.add(x);
            } while (readIf(","));
            read(")");
            c = factory.in(left, list);
        } else if (readIf("IS")) {
            boolean not = readIf("NOT");
            read("NULL");
            if (!(left instanceof PropertyValueImpl)) {
                throw getSyntaxError("propertyName (NOT NULL is only supported for properties)");
            }
            PropertyValueImpl p = (PropertyValueImpl) left;
            if (not) {
                c = getPropertyExistence(p);
            } else {
                c = getPropertyInexistence(p);
            }
        } else if (readIf("NOT")) {
            if (readIf("IS")) {
                read("NULL");
                if (!(left instanceof PropertyValueImpl)) {
                    throw new ParseException(
                            "Only property values can be tested for NOT IS NULL; got: "
                            + left.getClass().getName(), parseIndex);
                }
                PropertyValueImpl pv = (PropertyValueImpl) left;
                c = getPropertyExistence(pv);
            } else {
                read("LIKE");
                c = factory.comparison(left, Operator.LIKE, parseStaticOperand());
                c = factory.not(c);
            }
        } else {
            throw getSyntaxError();
        }
        return c;
    }

    private PropertyExistenceImpl getPropertyExistence(PropertyValueImpl p) throws ParseException {
        return factory.propertyExistence(p.getSelectorName(), p.getPropertyName());
    }
    
    private PropertyInexistenceImpl getPropertyInexistence(PropertyValueImpl p) throws ParseException {
        return factory.propertyInexistence(p.getSelectorName(), p.getPropertyName());
    }

    private ConstraintImpl parseConditionFunctionIf(String functionName) throws ParseException {
        ConstraintImpl c;
        if ("CONTAINS".equalsIgnoreCase(functionName)) {
            if (readIf("*")) {
                // strictly speaking, CONTAINS(*, ...) is not supported
                // according to the spec:
                // "If only one selector exists in this query, explicit
                // specification of the selectorName preceding the
                // propertyName is optional"
                // but we anyway support it
                read(",");
                c = factory.fullTextSearch(
                        getOnlySelectorName(), null, parseStaticOperand());
            } else if (readIf(".")) {
                if (!supportSQL1) {
                    throw getSyntaxError("selector name, property name, or *");
                }
                read(",");
                c = factory.fullTextSearch(
                        getOnlySelectorName(), null, parseStaticOperand());
            } else {
                String name = readName();
                if (readIf(".")) {
                    if (readIf("*")) {
                        read(",");
                        c = factory.fullTextSearch(
                                name, null, parseStaticOperand());
                    } else {
                        String selector = name;
                        name = readName();
                        read(",");
                        c = factory.fullTextSearch(
                                selector, name, parseStaticOperand());
                    }
                } else {
                    read(",");
                    c = factory.fullTextSearch(
                            getOnlySelectorName(), name,
                            parseStaticOperand());
                }
            }
        } else if ("ISSAMENODE".equalsIgnoreCase(functionName)) {
            String name = readName();
            if (readIf(",")) {
                c = factory.sameNode(name, readAbsolutePath());
            } else {
                c = factory.sameNode(getOnlySelectorName(), name);
            }
        } else if ("ISCHILDNODE".equalsIgnoreCase(functionName)) {
            String name = readName();
            if (readIf(",")) {
                c = factory.childNode(name, readAbsolutePath());
            } else {
                c = factory.childNode(getOnlySelectorName(), name);
            }
        } else if ("ISDESCENDANTNODE".equalsIgnoreCase(functionName)) {
            String name = readName();
            if (readIf(",")) {
                c = factory.descendantNode(name, readAbsolutePath());
            } else {
                c = factory.descendantNode(getOnlySelectorName(), name);
            }
        } else if ("SIMILAR".equalsIgnoreCase(functionName)) {
            if (readIf(".") || readIf("*")) {
                read(",");
                c = factory.similar(
                        getOnlySelectorName(), null, parseStaticOperand());
            } else {
                String name = readName();
                if (readIf(".")) {
                    if (readIf("*")) {
                        read(",");
                        c = factory.fullTextSearch(
                                name, null, parseStaticOperand());
                    } else {
                        String selector = name;
                        name = readName();
                        read(",");
                        c = factory.fullTextSearch(
                                selector, name, parseStaticOperand());
                    }
                } else {
                    read(",");
                    c = factory.fullTextSearch(
                            getOnlySelectorName(), name,
                            parseStaticOperand());
                }
            }
        } else if ("NATIVE".equalsIgnoreCase(functionName)) {
            String selectorName;
            if (currentTokenType == IDENTIFIER) {
                selectorName = readName();
                read(",");
            } else {
                selectorName = getOnlySelectorName();
            }
            String language = readString().getValue(Type.STRING);
            read(",");
            c = factory.nativeFunction(selectorName, language, parseStaticOperand());
        } else if ("SPELLCHECK".equalsIgnoreCase(functionName)) {
            String selectorName;
            if (currentTokenType == IDENTIFIER) {
                selectorName = readName();
                read(",");
            } else {
                selectorName = getOnlySelectorName();
            }
            c = factory.spellcheck(selectorName, parseStaticOperand());            
        } else if ("SUGGEST".equalsIgnoreCase(functionName)) {
            String selectorName;
            if (currentTokenType == IDENTIFIER) {
                selectorName = readName();
                read(",");
            } else {
                selectorName = getOnlySelectorName();
            }
            c = factory.suggest(selectorName, parseStaticOperand());
        } else {
            return null;
        }
        read(")");
        return c;
    }

    private String readAbsolutePath() throws ParseException {
        String path = readPath();
        if (!PathUtils.isAbsolute(path)) {
            throw getSyntaxError("absolute path");
        }
        return path;
    }

    private String readPath() throws ParseException {
        return readName();
    }

    private DynamicOperandImpl parseDynamicOperand() throws ParseException {
        boolean identifier = currentTokenType == IDENTIFIER;
        String name = readName();
        if (identifier && readIf("(")) {
            return parseExpressionFunction(name);
        } else {
            return parsePropertyValue(name);
        }
    }

    private DynamicOperandImpl parseExpressionFunction(String functionName) throws ParseException {
        DynamicOperandImpl op;
        if ("LENGTH".equalsIgnoreCase(functionName)) {
            op = factory.length(parseDynamicOperand());
        } else if ("NAME".equalsIgnoreCase(functionName)) {
            if (isToken(")")) {
                op = factory.nodeName(getOnlySelectorName());
            } else {
                op = factory.nodeName(readName());
            }
        } else if ("LOCALNAME".equalsIgnoreCase(functionName)) {
            if (isToken(")")) {
                op = factory.nodeLocalName(getOnlySelectorName());
            } else {
                op = factory.nodeLocalName(readName());
            }
        } else if ("SCORE".equalsIgnoreCase(functionName)) {
            if (isToken(")")) {
                op = factory.fullTextSearchScore(getOnlySelectorName());
            } else {
                op = factory.fullTextSearchScore(readName());
            }
        } else if ("COALESCE".equalsIgnoreCase(functionName)) {
            DynamicOperandImpl op1 = parseDynamicOperand();
            read(",");
            DynamicOperandImpl op2 = parseDynamicOperand();
            op = factory.coalesce(op1, op2);
        } else if ("LOWER".equalsIgnoreCase(functionName)) {
            op = factory.lowerCase(parseDynamicOperand());
        } else if ("UPPER".equalsIgnoreCase(functionName)) {
            op = factory.upperCase(parseDynamicOperand());
        } else if ("PROPERTY".equalsIgnoreCase(functionName)) {
            PropertyValueImpl pv = parsePropertyValue(readName());
            read(",");
            op = factory.propertyValue(pv.getSelectorName(), pv.getPropertyName(), readString().getValue(Type.STRING));
        } else {
            throw getSyntaxError("LENGTH, NAME, LOCALNAME, SCORE, COALESCE, LOWER, UPPER, or PROPERTY");
        }
        read(")");
        return op;
    }

    private PropertyValueImpl parsePropertyValue(String name) throws ParseException {
        if (readIf(".")) {
            return factory.propertyValue(name, readName());
        } else {
            return factory.propertyValue(getOnlySelectorName(), name);
        }
    }

    private StaticOperandImpl parseStaticOperand() throws ParseException {
        if (currentTokenType == PLUS) {
            read();
            if (currentTokenType != VALUE) {
                throw getSyntaxError("number");
            }
            int valueType = currentValue.getType().tag();
            switch (valueType) {
            case PropertyType.LONG:
                currentValue = PropertyValues.newLong(currentValue.getValue(Type.LONG));
                break;
            case PropertyType.DOUBLE:
                currentValue = PropertyValues.newDouble(currentValue.getValue(Type.DOUBLE));
                break;
            case PropertyType.DECIMAL:
                currentValue = PropertyValues.newDecimal(currentValue.getValue(Type.DECIMAL).negate());
                break;
            default:
                throw getSyntaxError("Illegal operation: + " + currentValue);
            }
        } else if (currentTokenType == MINUS) {
            read();
            if (currentTokenType != VALUE) {
                throw getSyntaxError("number");
            }
            int valueType = currentValue.getType().tag();
            switch (valueType) {
            case PropertyType.LONG:
                currentValue = PropertyValues.newLong(-currentValue.getValue(Type.LONG));
                break;
            case PropertyType.DOUBLE:
                currentValue = PropertyValues.newDouble(-currentValue.getValue(Type.DOUBLE));
                break;
            case PropertyType.BOOLEAN:
                currentValue = PropertyValues.newBoolean(!currentValue.getValue(Type.BOOLEAN));
                break;
            case PropertyType.DECIMAL:
                currentValue = PropertyValues.newDecimal(currentValue.getValue(Type.DECIMAL).negate());
                break;
            default:
                throw getSyntaxError("Illegal operation: -" + currentValue);
            }
        }
        if (currentTokenType == VALUE) {
            LiteralImpl literal = getUncastLiteral(currentValue);
            read();
            return literal;
        } else if (currentTokenType == PARAMETER) {
            read();
            String name = readName();
            if (readIf(":")) {
                name = name + ':' + readName();
            }
            BindVariableValueImpl var = bindVariables.get(name);
            if (var == null) {
                var = factory.bindVariable(name);
                bindVariables.put(name, var);
            }
            return var;
        } else if (readIf("TRUE")) {
            LiteralImpl literal = getUncastLiteral(PropertyValues.newBoolean(true));
            return literal;
        } else if (readIf("FALSE")) {
            LiteralImpl literal = getUncastLiteral(PropertyValues.newBoolean(false));
            return literal;
        } else if (readIf("CAST")) {
            read("(");
            StaticOperandImpl op = parseStaticOperand();
            if (!(op instanceof LiteralImpl)) {
                throw getSyntaxError("literal");
            }
            LiteralImpl literal = (LiteralImpl) op;
            PropertyValue value = literal.getLiteralValue();
            read("AS");
            value = parseCastAs(value);
            read(")");
            // CastLiteral
            literal = factory.literal(value);
            return literal;
        } else {
            if (supportSQL1) {
                if (readIf("TIMESTAMP")) {
                    StaticOperandImpl op = parseStaticOperand();
                    if (!(op instanceof LiteralImpl)) {
                        throw getSyntaxError("literal");
                    }
                    LiteralImpl literal = (LiteralImpl) op;
                    PropertyValue value = literal.getLiteralValue();
                    value = PropertyValues.newDate(value.getValue(Type.DATE));
                    literal = factory.literal(value);
                    return literal;
                }
            }
            throw getSyntaxError("static operand");
        }
    }

    /**
     * Create a literal from a parsed value.
     *
     * @param value the original value
     * @return the literal
     */
    private LiteralImpl getUncastLiteral(PropertyValue value) {
        return factory.literal(value);
    }

    private PropertyValue parseCastAs(PropertyValue value)
            throws ParseException {
        if (currentTokenQuoted) {
            throw getSyntaxError("data type (STRING|BINARY|...)");
        }
        int propertyType = getPropertyTypeFromName(currentToken);
        read();

        PropertyValue v = ValueConverter.convert(value, propertyType, null);
        if (v == null) {
            throw getSyntaxError("data type (STRING|BINARY|...)");
        }
        return v;
    }

    /**
     * Get the property type from the given case insensitive name.
     *
     * @param name the property type name (case insensitive)
     * @return the type, or {@code PropertyType.UNDEFINED} if unknown
     */
    public static int getPropertyTypeFromName(String name) {
        if (matchesPropertyType(PropertyType.STRING, name)) {
            return PropertyType.STRING;
        } else if (matchesPropertyType(PropertyType.BINARY, name)) {
            return PropertyType.BINARY;
        } else if (matchesPropertyType(PropertyType.DATE, name)) {
            return PropertyType.DATE;
        } else if (matchesPropertyType(PropertyType.LONG, name)) {
            return PropertyType.LONG;
        } else if (matchesPropertyType(PropertyType.DOUBLE, name)) {
            return PropertyType.DOUBLE;
        } else if (matchesPropertyType(PropertyType.DECIMAL, name)) {
            return PropertyType.DECIMAL;
        } else if (matchesPropertyType(PropertyType.BOOLEAN, name)) {
            return PropertyType.BOOLEAN;
        } else if (matchesPropertyType(PropertyType.NAME, name)) {
            return PropertyType.NAME;
        } else if (matchesPropertyType(PropertyType.PATH, name)) {
            return PropertyType.PATH;
        } else if (matchesPropertyType(PropertyType.REFERENCE, name)) {
            return PropertyType.REFERENCE;
        } else if (matchesPropertyType(PropertyType.WEAKREFERENCE, name)) {
            return PropertyType.WEAKREFERENCE;
        } else if (matchesPropertyType(PropertyType.URI, name)) {
            return PropertyType.URI;
        }
        return PropertyType.UNDEFINED;
    }

    private static boolean matchesPropertyType(int propertyType, String name) {
        String typeName = PropertyType.nameFromValue(propertyType);
        return typeName.equalsIgnoreCase(name);
    }

    private OrderingImpl[] parseOrder() throws ParseException {
        ArrayList<OrderingImpl> orderList = new ArrayList<OrderingImpl>();
        do {
            OrderingImpl ordering;
            DynamicOperandImpl op = parseDynamicOperand();
            if (readIf("DESC")) {
                ordering = factory.descending(op);
            } else {
                readIf("ASC");
                ordering = factory.ascending(op);
            }
            orderList.add(ordering);
        } while (readIf(","));
        OrderingImpl[] orderings = new OrderingImpl[orderList.size()];
        orderList.toArray(orderings);
        return orderings;
    }

    private ArrayList<ColumnOrWildcard> parseColumns() throws ParseException {
        ArrayList<ColumnOrWildcard> list = new ArrayList<ColumnOrWildcard>();
        if (readIf("*")) {
            list.add(new ColumnOrWildcard());
        } else {
            do {
                ColumnOrWildcard column = new ColumnOrWildcard();
                if (readIf("*")) {
                    column.propertyName = null;
                } else if (readIf("EXCERPT")) {
                    column.propertyName = "rep:excerpt";
                    read("(");
                    if (!readIf(")")) {
                        if (!readIf(".")) {
                            column.selectorName = readName();
                        }
                        read(")");
                    }
                    readOptionalAlias(column);
                } else {
                    column.propertyName = readName();
                    if (column.propertyName.equals("rep:spellcheck")) {
                        if (readIf("(")) {
                            read(")");
                            column.propertyName = ":spellcheck";
                        }
                        readOptionalAlias(column);
                    } else if (readIf(".")) {
                        column.selectorName = column.propertyName;
                        if (readIf("*")) {
                            column.propertyName = null;
                        } else {
                            column.propertyName = readName();
                            if (!readOptionalAlias(column)) {
                                column.columnName =
                                        column.selectorName
                                        + "." + column.propertyName;
                            }
                        }
                    } else {
                        readOptionalAlias(column);
                    }
                }
                list.add(column);
            } while (readIf(","));
        }
        return list;
    }
    
    private boolean readOptionalAlias(ColumnOrWildcard column) throws ParseException {
        if (readIf("AS")) {
            column.columnName = readName();
            return true;
        }
        return false;
    }

    private ColumnImpl[] resolveColumns(ArrayList<ColumnOrWildcard> list) throws ParseException {
        ArrayList<ColumnImpl> columns = new ArrayList<ColumnImpl>();
        for (ColumnOrWildcard c : list) {
            if (c.propertyName == null) {
                addWildcardColumns(columns, c.selectorName);
            } else {
                String selectorName = c.selectorName;
                if (selectorName == null) {
                    selectorName = getOnlySelectorName();
                }

                String columnName = c.columnName;
                if (columnName == null) {
                    columnName = c.propertyName;
                }

                columns.add(factory.column(
                        selectorName, c.propertyName, columnName));
            }
        }
        ColumnImpl[] array = new ColumnImpl[columns.size()];
        columns.toArray(array);
        return array;
    }

    private void addWildcardColumns(
            Collection<ColumnImpl> columns, String selectorName)
            throws ParseException {
        if (selectorName == null) {
            for (SelectorImpl selector : selectors.values()) {
                addWildcardColumns(columns, selector);
            }
        } else {
            SelectorImpl selector = selectors.get(selectorName);
            if (selector != null) {
                addWildcardColumns(columns, selector);
            } else {
                throw getSyntaxError("Unknown selector: " + selectorName);
            }
        }
    }

    private void addWildcardColumns(
            Collection<ColumnImpl> columns, SelectorImpl selector) {
        String selectorName = selector.getSelectorName();
        for (String propertyName : selector.getWildcardColumns()) {
            if (namePathMapper != null) {
                propertyName = namePathMapper.getJcrName(propertyName);
            }
            String columnName;
            if (includeSelectorNameInWildcardColumns) {
                columnName = selectorName + "." + propertyName;
            } else {
                columnName = propertyName;
            }
            columns.add(factory.column(selectorName, propertyName, columnName));
        }

        if (columns.isEmpty()) {
            // OAK-1354, inject the selector name
            columns.add(factory
                    .column(selectorName, selectorName, selectorName));
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
        boolean result = token.equalsIgnoreCase(currentToken) && !currentTokenQuoted;
        if (result) {
            return true;
        }
        addExpected(token);
        return false;
    }

    private void read(String expected) throws ParseException {
        if (!expected.equalsIgnoreCase(currentToken) || currentTokenQuoted) {
            throw getSyntaxError(expected);
        }
        read();
    }

    private PropertyValue readString() throws ParseException {
        if (currentTokenType != VALUE) {
            throw getSyntaxError("string value");
        }
        PropertyValue value = currentValue;
        read();
        return value;
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
                type = CHAR_SPECIAL_1;
                break;
            case '!':
            case '<':
            case '>':
            case '|':
            case '=':
            case ':':
                type = CHAR_SPECIAL_2;
                break;
            case '.':
                type = CHAR_DECIMAL;
                break;
            case '/':
                if (command[i + 1] != '*') {
                    type = CHAR_SPECIAL_1;
                    break;
                }
                types[i] = type = CHAR_IGNORE;                
                startLoop = i;
                i += 2;
                checkRunOver(i, len, startLoop);
                while (command[i] != '*' || command[i + 1] != '/') {
                    i++;
                    checkRunOver(i, len, startLoop);
                }
                i++;          
                break;
            case '[':
                types[i] = type = CHAR_BRACKETED;
                startLoop = i;
                while (true) {
                    while (command[++i] != ']') {
                        checkRunOver(i, len, startLoop);
                    }
                    if (i >= len - 1 || command[i + 1] != ']') {
                        break;
                    }
                    i++;
                }
                break;
            case '\'':
                types[i] = type = CHAR_STRING;
                startLoop = i;
                while (command[++i] != '\'') {
                    checkRunOver(i, len, startLoop);
                }
                break;
            case '\"':
                types[i] = type = CHAR_QUOTED;
                startLoop = i;
                while (command[++i] != '\"') {
                    checkRunOver(i, len, startLoop);
                }
                break;
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
        if (parseIndex >= characterTypes.length) {
            throw getSyntaxError();
        }
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
                if (type != CHAR_NAME && type != CHAR_VALUE) {
                    c = chars[i];
                    if (supportSQL1 && c == ':') {
                        i++;
                        continue;
                    }
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
            return;
        case CHAR_SPECIAL_1:
            currentToken = statement.substring(start, i);
            switch (c) {
            case '$':
                currentTokenType = PARAMETER;
                break;
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
                    checkLiterals(false);
                    currentValue = PropertyValues.newLong(number);
                    currentTokenType = VALUE;
                    currentToken = "0";
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
        case CHAR_BRACKETED:
            currentTokenQuoted = true;
            readString(i, ']');
            currentTokenType = IDENTIFIER;
            currentToken = currentValue.getValue(Type.STRING);
            return;
        case CHAR_STRING:
            currentTokenQuoted = true;
            readString(i, '\'');
            return;
        case CHAR_QUOTED:
            currentTokenQuoted = true;
            readString(i, '\"');
            if (supportSQL1) {
                // for SQL-2, this is a literal, as defined in
                // the JCR 2.0 spec, 6.7.34 Literal - UncastLiteral
                // but for compatibility with Jackrabbit 2.x, for
                // SQL-1, this is an identifier, as in ANSI SQL
                // (not in the JCR 1.0 spec)
                // (confusing isn't it?)
                currentTokenType = IDENTIFIER;
                currentToken = currentValue.getValue(Type.STRING);
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
        currentToken = "'";
        if (end != ']') {
            checkLiterals(false);
        }
        currentValue = PropertyValues.newString(result);
        parseIndex = i;
        currentTokenType = VALUE;
    }

    private void checkLiterals(boolean text) throws ParseException {
        if (LOG.isTraceEnabled() && !literalUsageLogged) {
            literalUsageLogged = true;
            LOG.trace("Literal used");
        }
        if (text && !allowTextLiterals || !text && !allowNumberLiterals) {
            throw getSyntaxError("bind variable (literals of this type not allowed)");
        }
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
        BigDecimal bd;
        try {
            bd = new BigDecimal(sub);
        } catch (NumberFormatException e) {
            throw new ParseException("Data conversion error converting " + sub + " to BigDecimal: " + e, parseIndex);
        }
        checkLiterals(false);

        currentValue = PropertyValues.newDecimal(bd);
        currentTokenType = VALUE;
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
        return new ParseException("Query: " + query, index);
    }

    /**
     * Represents a column or a wildcard in a SQL expression.
     * This class is temporarily used during parsing.
     */
    static class ColumnOrWildcard {
        String selectorName;
        String propertyName;
        String columnName;
    }

    /**
     * Get the selector name if only one selector exists in the query.
     * If more than one selector exists, an exception is thrown.
     *
     * @return the selector name
     */
    private String getOnlySelectorName() throws ParseException {
        if (selectors.size() > 1) {
            throw getSyntaxError("Need to specify the selector name because the query contains more than one selector.");
        }
        return selectors.values().iterator().next().getSelectorName();
    }

    public static String escapeStringLiteral(String value) {
        if (value.indexOf('\'') >= 0) {
            value = value.replace("'", "''");
        }
        return '\'' + value + '\'';
    }

    /**
     * Enable or disable support for text literals in queries. The default is enabled.
     *
     * @param allowTextLiterals
     */
    public void setAllowTextLiterals(boolean allowTextLiterals) {
        this.allowTextLiterals = allowTextLiterals;
    }

    public void setAllowNumberLiterals(boolean allowNumberLiterals) {
        this.allowNumberLiterals = allowNumberLiterals;
    }

    public void setIncludeSelectorNameInWildcardColumns(boolean value) {
        this.includeSelectorNameInWildcardColumns = value;
    }

    /**
     * Whether the given statement is an internal query.
     *  
     * @param statement the statement
     * @return true for an internal query
     */
    public static boolean isInternal(String statement) {
        return statement.indexOf(QueryEngine.INTERNAL_SQL2_QUERY) >= 0;
    }

}
