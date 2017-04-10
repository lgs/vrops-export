package net.virtualviking.vropsexport;

public class SchemaNode {
    private final String name;

    private final String alias;

    public SchemaNode(String name, String alias) {
        this.name = name;
        this.alias = alias != null ? alias : name;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }
}
