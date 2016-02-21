package com.github.hronom.test.graphs.db.base;

public interface TripleDatabaseModel {
    boolean openForInsert();
    boolean insert(String tagNameA, String tagNameB);
    boolean closeAfterInsert();

    boolean openForIsRelated();
    boolean isRelated(String tagNameA, String tagNameB);
    boolean closeAfterIsRelated();

    boolean openForReadingAllProperties();
    boolean readAllProperties(String tagNameA);
    boolean closeAfterReadingAllProperties();
}
