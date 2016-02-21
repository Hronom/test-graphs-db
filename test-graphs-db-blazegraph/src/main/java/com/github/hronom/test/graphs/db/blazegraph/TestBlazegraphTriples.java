package com.github.hronom.test.graphs.db.blazegraph;

import com.github.hronom.test.graphs.db.base.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.TriplesModelsTester;

public class TestBlazegraphTriples {
    public static void main(String[] args) {
        TripleDatabaseModel tripleDatabaseModel = new BlazegraphTripleModel();
        TriplesModelsTester.test(tripleDatabaseModel);
    }
}
