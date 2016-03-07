package com.github.hronom.test.graphs.db.neo4j.models;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseTestModel;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Neo4JTripleTestModel implements TripleDatabaseTestModel {
    private Path path = Paths.get("Neo4j Triples");

    private final Label tagLabel = DynamicLabel.label("Tag");

    private final String tagNameProperty = "tagName";

    private GraphDatabaseService neo4jDatabase;

    private BatchInserter inserter = null;
    private final HashMap<String, Long> inMemoryCache = new HashMap<>();

    private enum RelationshipTypes implements RelationshipType {
        relation
    }

    @Override
    public boolean openForBulkLoading() {
        inserter = BatchInserters.inserter(path.toString());
        inserter.createDeferredSchemaIndex(tagLabel).on(tagNameProperty).create();
        return true;
    }

    @Override
    public boolean bulkInsert(Path sourcePath) {
        /*Long tagNameANode = inMemoryCache.get(tagNameA);
        if(tagNameANode == null) {
            Map<String, Object> propertiesA = new HashMap<>();
            propertiesA.put(tagNameProperty, tagNameA);
            tagNameANode = inserter.createNode(propertiesA, tagLabel);
            inMemoryCache.put(tagNameA, tagNameANode);
        }

        Long tagNameBNode = inMemoryCache.get(tagNameB);
        if(tagNameBNode == null) {
            Map<String, Object> propertiesB = new HashMap<>();
            propertiesB.put(tagNameProperty, tagNameB);
            tagNameBNode = inserter.createNode(propertiesB, tagLabel);
            inMemoryCache.put(tagNameB, tagNameBNode);
        }

        inserter.createRelationship(tagNameANode, tagNameBNode, RelationshipTypes.relation, null);*/

        return true;
    }

    @Override
    public boolean closeAfterBulkLoading() {
        inserter.shutdown();
        return true;
    }

    @Override
    public boolean openForSingleInserting() {
        openRegular();
        return true;
    }

    @Override
    public boolean singleInsert(String tagNameA, String tagNameB) {
        try (Transaction tx = neo4jDatabase.beginTx()) {
            Node tagNameANode = neo4jDatabase.findNode(tagLabel, tagNameProperty, tagNameA);
            if (tagNameANode == null) {
                tagNameANode = neo4jDatabase.createNode(tagLabel);
                tagNameANode.setProperty(tagNameProperty, tagNameA);
            }

            Node tagNameBNode = neo4jDatabase.findNode(tagLabel, tagNameProperty, tagNameB);
            if (tagNameBNode == null) {
                tagNameBNode = neo4jDatabase.createNode(tagLabel);
                tagNameBNode.setProperty(tagNameProperty, tagNameB);
            }

            boolean created = false;
            for (Relationship r : tagNameANode.getRelationships(RelationshipTypes.relation)) {
                if (r.getOtherNode(tagNameANode).equals(tagNameBNode)) {
                    created = true;
                    break;
                }
            }
            if (!created) {
                tagNameANode.createRelationshipTo(tagNameBNode, RelationshipTypes.relation);
            }
            tx.success();
        }
        return true;
    }

    @Override
    public boolean closeAfterSingleInserting() {
        neo4jDatabase.shutdown();
        return true;
    }

    @Override
    public boolean openForIsRelated() {
        openRegular();
        return true;
    }

    @Override
    public boolean isRelated(String tagNameA, String tagNameB) {
        try (Transaction tx = neo4jDatabase.beginTx()) {
            Node tagNameANode = neo4jDatabase.findNode(tagLabel, tagNameProperty, tagNameA);

            //Node tagNameBNode = neo4jDatabase.findNode(tagLabel, tagNameProperty, tagNameB);

            for (Relationship r : tagNameANode.getRelationships(RelationshipTypes.relation)) {
                Node tagNameBNode = r.getOtherNode(tagNameANode);
                String tagNameBValue = (String) tagNameBNode.getProperty(tagNameProperty);
                if (tagNameBValue.equals(tagNameB)) {
                    tx.success();
                    return true;
                }
            }
            tx.success();
            return false;
        }
    }

    @Override
    public boolean closeAfterIsRelated() {
        neo4jDatabase.shutdown();
        return true;
    }

    @Override
    public boolean openForReadingAllProperties() {
        openRegular();
        return true;
    }

    @Override
    public boolean readAllProperties(String tagNameA) {
        try (Transaction tx = neo4jDatabase.beginTx()) {
            Node tagNameANode = neo4jDatabase.findNode(tagLabel, tagNameProperty, tagNameA);
            for (Relationship r : tagNameANode.getRelationships(RelationshipTypes.relation)) {
                r.getOtherNode(tagNameANode);
            }
            tx.success();
            return true;
        }
    }

    @Override
    public boolean closeAfterReadingAllProperties() {
        neo4jDatabase.shutdown();
        return true;
    }

    @Override
    public boolean openForSingleDeleting() {
        // TODO need to implement...
        return false;
    }

    @Override
    public boolean singleDelete(String tagNameA, String tagNameB) {
        // TODO need to implement...
        return false;
    }

    @Override
    public boolean closeAfterSingleDeleting() {
        // TODO need to implement...
        return false;
    }

    @Override
    public long getDbSize() {
        // TODO need to implement...
        return -1;
    }

    private void openRegular() {
        // Open database.
        neo4jDatabase = new GraphDatabaseFactory().newEmbeddedDatabase(path.toString());

        // Registers a shutdown hook for the Neo4j instance so that it shuts down nicely when the VM
        // exits (even if you "Ctrl-C" the running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                neo4jDatabase.shutdown();
            }
        });

        IndexDefinition indexDefinition;
        try (Transaction tx = neo4jDatabase.beginTx()) {
            Schema schema = neo4jDatabase.schema();
            Iterator<IndexDefinition> iter = schema.getIndexes(tagLabel).iterator();
            if (!iter.hasNext()) {
                indexDefinition = schema.indexFor(tagLabel).on(tagNameProperty).create();
            } else {
                indexDefinition = iter.next();
            }
            tx.success();
        }

        // About indexes http://neo4j.com/docs/3.0.0-M02/tutorials-java-embedded-new-index.html
        try (Transaction tx = neo4jDatabase.beginTx()) {
            Schema schema = neo4jDatabase.schema();
            schema.awaitIndexOnline(indexDefinition, 30, TimeUnit.SECONDS);
            tx.success();
        }
    }
}
