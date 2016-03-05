package com.github.hronom.test.graphs.db.blazegraph;

import com.github.hronom.test.graphs.db.base.models.QuadDatabaseModel;
import com.github.hronom.test.graphs.db.base.testers.QuadsModelsTester;

public class TestBlazegraphQuads {
    public static void main(String[] args) {
        QuadDatabaseModel quadDatabaseModel = new BlazegraphQuadModel();
        quadDatabaseModel.initialize();
        QuadsModelsTester.test(quadDatabaseModel);
    }
}
