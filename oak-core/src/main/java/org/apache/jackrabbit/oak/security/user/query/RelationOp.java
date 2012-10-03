package org.apache.jackrabbit.oak.security.user.query;

/**
 * Relational operators for comparing a property to a value. Correspond
 * to the general comparison operators as define in JSR-170.
 * The {@link #EX} tests for existence of a property.
 */
enum RelationOp {

    NE("!="),
    EQ("="),
    LT("<"),
    LE("<="),
    GT(">"),
    GE("=>"),
    EX(""),
    LIKE("like");

    private final String op;

    RelationOp(String op) {
        this.op = op;
    }

    String getOp() {
        return op;
    }
}
