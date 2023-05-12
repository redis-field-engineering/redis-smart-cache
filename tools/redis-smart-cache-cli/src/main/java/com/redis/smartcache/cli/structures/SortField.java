package com.redis.smartcache.cli.structures;

public enum SortField {
    queryTime("query-time"),
    accessFrequency("access-frequency"),
    tables("tables"),
    id("id");

    final private String value;

    SortField(String value){
        this.value = value;
    }

    public String getValue(){
        return value;
    }


    /**
     * Overridden valueOf so we can have nicer enum names.
     * @param value the Value to parse
     * @return the enum matching the string value.
     * @throws IllegalArgumentException thrown if the string value does not map to a known enum value
     */
    public static SortField valueOfOverride(String value) throws IllegalArgumentException{
        for(SortField e : values()){
            if(value.equals(e.value)){
                return e;
            }
        }
        throw new IllegalArgumentException("No Enum constant with value: " + value);
    }


}
