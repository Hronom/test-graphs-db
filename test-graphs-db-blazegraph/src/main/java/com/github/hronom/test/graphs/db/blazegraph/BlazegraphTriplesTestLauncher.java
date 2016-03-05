package com.github.hronom.test.graphs.db.blazegraph;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.testers.TriplesModelsTester;
import com.github.hronom.test.graphs.db.blazegraph.models.BlazegraphTripleModel;

public class BlazegraphTriplesTestLauncher {
    public static void main(String[] args) {
        TripleDatabaseModel tripleDatabaseModel = new BlazegraphTripleModel();
        TriplesModelsTester.test(tripleDatabaseModel);
    }
}
