package com.redis.smartcache.cli.structures;

public class Action implements RowInfo {
    String action;

    public String getAction(){
        return action;
    }

    public Action(String action){
        this.action = action;
    }

    @Override
    public String toRowString(int colWidth) {
        return action;
    }
}
