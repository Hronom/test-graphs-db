package com.github.hronom.test.graphs.db.apache.jena;

import com.github.hronom.test.graphs.db.apache.jena.models.ApacheJenaTripleModel;
import com.github.hronom.test.graphs.db.base.models.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.testers.TriplesModelsTester;

public class ApacheJenaTriplesTestLauncher {
    public static void main(String[] args) {
        TripleDatabaseModel tripleDatabaseModel = new ApacheJenaTripleModel();
        TriplesModelsTester.test(tripleDatabaseModel);
    }
}