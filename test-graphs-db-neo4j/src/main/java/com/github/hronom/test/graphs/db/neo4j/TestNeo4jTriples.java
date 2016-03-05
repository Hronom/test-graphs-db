package com.github.hronom.test.graphs.db.neo4j;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.testers.TriplesModelsTester;

public class TestNeo4jTriples {
    public static void main(String[] args) {
        TripleDatabaseModel tripleDatabaseModel = new Neo4JTripleModel();
        TriplesModelsTester.test(tripleDatabaseModel);
    }
}
