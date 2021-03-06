package com.github.hronom.test.graphs.db.common.models;

import java.nio.file.Path;

public interface TripleDatabaseModel {
    boolean openForBulkLoading();
    boolean bulkInsert(Path sourcePath);
    boolean closeAfterBulkLoading();

    boolean openForSingleInserting();
    boolean singleInsert(String tagNameA, String tagNameB);
    boolean closeAfterSingleInserting();

    boolean openForIsRelated();
    boolean isRelated(String tagNameA, String tagNameB);
    boolean closeAfterIsRelated();

    boolean openForReadingAllProperties();
    boolean readAllProperties(String tagNameA);
    boolean closeAfterReadingAllProperties();

    boolean openForSingleDeleting();
    boolean singleDelete(String tagNameA, String tagNameB);
    boolean closeAfterSingleDeleting();

    long getDbSize();
}
