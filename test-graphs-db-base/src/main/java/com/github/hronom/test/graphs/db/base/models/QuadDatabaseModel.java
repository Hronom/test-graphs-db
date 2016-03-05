package com.github.hronom.test.graphs.db.base.models;

public interface QuadDatabaseModel {
    boolean initialize();

    boolean insert(String tagNameA, String tagNameB, String graph);
    boolean isRelated(String tagNameA, String tagNameB, String graph);
    boolean readAllProperties(String tagNameA, String graph);

    boolean copyToGraph(String graphA, String graphB);

    boolean commit();
}
