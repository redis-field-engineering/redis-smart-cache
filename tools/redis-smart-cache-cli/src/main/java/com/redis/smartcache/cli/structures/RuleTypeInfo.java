package com.redis.smartcache.cli.structures;

public class RuleTypeInfo implements RowInfo {
    public RuleType getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    final private RuleType type;
    final private String message;

    public RuleTypeInfo(RuleType type, String message){
        this.type = type;
        this.message = message;

    }

    @Override
    public String toRowString(int colWidth) {
        return type.getValue();
    }
}
