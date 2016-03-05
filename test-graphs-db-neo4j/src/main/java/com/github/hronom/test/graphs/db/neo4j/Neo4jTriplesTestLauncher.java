package com.github.hronom.test.graphs.db.neo4j;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseTestModel;
import com.github.hronom.test.graphs.db.base.testers.TriplesModelsTester;
import com.github.hronom.test.graphs.db.neo4j.models.Neo4JTripleTestModel;

public class Neo4jTriplesTestLauncher {
    public static void main(String[] args) {
        TripleDatabaseTestModel tripleDatabaseTestModel = new Neo4JTripleTestModel();
        TriplesModelsTester.test(tripleDatabaseTestModel);
    }
}
