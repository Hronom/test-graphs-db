package com.github.hronom.test.graphs.db.apache.jena;

import com.github.hronom.test.graphs.db.base.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.TriplesModelsTester;

public class TestApacheJenaTriples {
    public static void main(String[] args) {
        TripleDatabaseModel tripleDatabaseModel = new ApacheJenaTripleModel();
        TriplesModelsTester.test(tripleDatabaseModel);
    }
}
