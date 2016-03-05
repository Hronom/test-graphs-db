package com.github.hronom.test.graphs.db.blazegraph;

import com.github.hronom.test.graphs.db.base.models.QuadDatabaseTestModel;
import com.github.hronom.test.graphs.db.base.testers.QuadsModelsTester;
import com.github.hronom.test.graphs.db.blazegraph.models.BlazegraphQuadTestModel;

public class BlazegraphQuadsTestLauncher {
    public static void main(String[] args) {
        QuadDatabaseTestModel quadDatabaseTestModel = new BlazegraphQuadTestModel();
        quadDatabaseTestModel.initialize();
        QuadsModelsTester.test(quadDatabaseTestModel);
    }
}
