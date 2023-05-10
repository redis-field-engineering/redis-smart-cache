package com.redis.smartcache.cli.structures;

import com.redis.smartcache.cli.util.Util;

public class RuleTypeInfo implements RowStringable{
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
        return Util.center(type.getValue(),colWidth);
    }
}
