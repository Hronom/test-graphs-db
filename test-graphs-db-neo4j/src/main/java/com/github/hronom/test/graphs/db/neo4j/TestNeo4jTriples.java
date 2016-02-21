package com.github.hronom.test.graphs.db.neo4j;

import com.github.hronom.test.graphs.db.base.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.TriplesModelsTester;

public class TestNeo4jTriples {
    public static void main(String[] args) {
        TripleDatabaseModel tripleDatabaseModel = new Neo4JTripleModel();
        TriplesModelsTester.test(tripleDatabaseModel);
    }
}
