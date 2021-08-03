package org.hkust.schema;

public enum Relation {
    LINEITEM("lineitem"),
    ORDERS("orders"),
    CUSTOMER("customer"),
    NATION("nation"),
    PART("part"),
    SUPPLIER("supplier"),
    PARTSUPP("partsupp"),
    REGION("region");

    private String relation;

    Relation(String relation) {
        this.relation = relation;
    }

    public String getValue() {
        return relation;
    }

    public static Relation getRelation(String relation) {
        for (Relation r : values()) {
            if (r.getValue().equals(relation.toLowerCase())) {
                return r;
            }
        }
        throw new IllegalArgumentException();
    }

    public static String getRelationAbbr(String relation) {
        switch (relation) {
            case "lineitem" : return "l";
            case "orders" : return "o";
            case "customer" : return "c";
            case "nation" : return "n";
            case "part" : return "p";
            case "supplier" : return "s";
            case "partsupp" : return "ps";
            case "region" : return "r";
        }
        return "";
    }
}
