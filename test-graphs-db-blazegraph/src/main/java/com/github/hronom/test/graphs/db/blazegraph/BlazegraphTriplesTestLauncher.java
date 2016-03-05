package com.github.hronom.test.graphs.db.blazegraph;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseTestModel;
import com.github.hronom.test.graphs.db.base.testers.TriplesModelsTester;
import com.github.hronom.test.graphs.db.blazegraph.models.BlazegraphTripleTestModel;

public class BlazegraphTriplesTestLauncher {
    public static void main(String[] args) {
        TripleDatabaseTestModel tripleDatabaseTestModel = new BlazegraphTripleTestModel();
        TriplesModelsTester.test(tripleDatabaseTestModel);
    }
}
