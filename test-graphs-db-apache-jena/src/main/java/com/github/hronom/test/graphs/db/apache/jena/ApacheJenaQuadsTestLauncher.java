package com.github.hronom.test.graphs.db.apache.jena;

import com.github.hronom.test.graphs.db.apache.jena.models.ApacheJenaQuadTestModel;
import com.github.hronom.test.graphs.db.base.models.QuadDatabaseTestModel;
import com.github.hronom.test.graphs.db.base.testers.QuadsModelsTester;

public class ApacheJenaQuadsTestLauncher {
    public static void main(String[] args) {
        QuadDatabaseTestModel quadDatabaseTestModel = new ApacheJenaQuadTestModel();
        quadDatabaseTestModel.initialize();
        QuadsModelsTester.test(quadDatabaseTestModel);
    }
}
