package com.github.hronom.test.graphs.db.blazegraph;

import com.github.hronom.test.graphs.db.base.QuadDatabaseModel;
import com.github.hronom.test.graphs.db.base.QuadsModelsTester;
import com.github.hronom.test.graphs.db.base.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.TriplesModelsTester;

public class TestBlazegraphQuads {
    public static void main(String[] args) {
        QuadDatabaseModel quadDatabaseModel = new BlazegraphQuadModel();
        quadDatabaseModel.initialize();
        QuadsModelsTester.test(quadDatabaseModel);
    }
}
