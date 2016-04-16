package com.github.hronom.test.graphs.db.apache.jena.models;

import com.github.hronom.test.graphs.db.common.models.QuadDatabaseModel;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.setup.StoreParams;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.tdb.store.bulkloader.BulkLoader;
import org.apache.jena.tdb.sys.TDBMaker;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

public class ApacheJenaQuadModel implements QuadDatabaseModel {
    //private DatasetGraph datasetGraph;

    private Location location;
    private DatasetGraphTDB datasetGraphTDB;

    @Override
    public boolean open() {
        location = Location.create("Apache Jena");
        datasetGraphTDB = TDBMaker.createDatasetGraphTDB(location, StoreParams.getDftStoreParams());
        return true;
    }

    @Override
    public boolean bulkUpload(String graph, Path ntFilePath) {
        BulkLoader.loadNamedGraph(
            datasetGraphTDB,
            NodeFactory.createURI("http://www.test.org/graph/" + graph),
            Collections.singletonList(ntFilePath.toUri().toString()),
            true,
            true
        );
        return true;
    }

    @Override
    public boolean insert(String graph, String subject, String predicate, String object) {
        Quad quad = new Quad(
            NodeFactory.createURI("http://www.test.org/graph/" + graph),
            NodeFactory.createURI("http://www.test.org/tag/" + subject),
            NodeFactory.createURI("http://www.test.org/relatedTo"),
            NodeFactory.createURI("http://www.test.org/tag/" + object)
        );
        datasetGraphTDB.add(quad);
        return true;
    }

    @Override
    public boolean commitChanges() {
        return true;
    }

    @Override
    public boolean contains(String graph, String subject, String predicate, String object) {
        Quad quad = new Quad(
            NodeFactory.createURI("http://www.test.org/graph/" + graph),
            NodeFactory.createURI("http://www.test.org/tag/" + subject),
            NodeFactory.createURI("http://www.test.org/relatedTo"),
            NodeFactory.createURI("http://www.test.org/tag/" + object)
        );
        return datasetGraphTDB.contains(quad);
    }

    @Override
    public void close() throws Exception {

    }
}
