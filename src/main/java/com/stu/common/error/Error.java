package com.stu.common.error;

/**
 * @author Student
 */
public class Error {

    public static final java.lang.Error CacheIsFullException = new java.lang.Error("Cache is full");
    public static final java.lang.Error DataNotFoundException= new java.lang.Error("Data not found");

    public static final java.lang.Error InvalidCommandException = new java.lang.Error("Invalid command exception");
    public static final java.lang.Error TableNoIndexException = new java.lang.Error("Table has no index exception");
}
