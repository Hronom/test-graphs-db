package com.github.hronom.test.graphs.db.apache.jena;

import com.github.hronom.test.graphs.db.base.models.QuadDatabaseModel;
import com.github.hronom.test.graphs.db.base.testers.QuadsModelsTester;

public class TestApacheJenaQuads {
    public static void main(String[] args) {
        QuadDatabaseModel quadDatabaseModel = new ApacheJenaQuadModel();
        quadDatabaseModel.initialize();
        QuadsModelsTester.test(quadDatabaseModel);
    }
}
