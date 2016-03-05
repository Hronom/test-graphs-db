package com.github.hronom.test.graphs.db.apache.jena;

import com.github.hronom.test.graphs.db.apache.jena.models.ApacheJenaTripleTestModel;
import com.github.hronom.test.graphs.db.base.models.TripleDatabaseTestModel;
import com.github.hronom.test.graphs.db.base.testers.TriplesModelsTester;

public class ApacheJenaTriplesTestLauncher {
    public static void main(String[] args) {
        TripleDatabaseTestModel tripleDatabaseTestModel = new ApacheJenaTripleTestModel();
        TriplesModelsTester.test(tripleDatabaseTestModel);
    }
}
