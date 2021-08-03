package org.hkust.objects;

public enum Operator {
    GREATER_THAN(">"),
    LESS_THAN("<"),
    EQUALS("=="),
    GREATER_THAN_EQUAL(">="),
    LESS_THAN_EQUAL("<="),
    NOT_EQUAL("<>"),
    SUM("+"),
    SUBTRACT("-"),
    SUMMATION("++"),
    MULTIPLY("*"),
    PRODUCT("**"),
    DIVIDE("/"),
    AVG("avg"),
    AND("&&"),
    OR("||"),
    NOT("!"),
    IF("if"),
    CASE("case"),
    LIKE("LIKE"),
    NOT_LIKE("NOT LIKE"),
    COUNT("COUNT"),
    COUNT_DISTINCT("COUNT_DISTINCT");

    private String operator;

    Operator(String operator) {
        this.operator = operator;
    }

    public static Operator getOperator(String op) {
        for (Operator operator : values()) {
            if (operator.getType().equals(op)) {
                return operator;
            }
        }
        throw new IllegalArgumentException("Got " + op);
    }

    public String getValue() {
        if (this == NOT_EQUAL) return "!=";
        else return operator;
    }

    private String getType() {
        return operator;
    }

    @Override
    public String toString() {
        return "Operator{" +
                "operator='" + operator + '\'' +
                '}';
    }
}
