package com.github.hronom.test.graphs.db.apache.jena.models;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseTestModel;
import com.github.hronom.test.graphs.db.base.utils.RDFVocabulary;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.setup.StoreParams;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.tdb.sys.TDBMaker;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ApacheJenaTripleTestModel implements TripleDatabaseTestModel {
    private static final Logger logger = LogManager.getLogger();
    private final Path regularDbPath = Paths.get("Apache Jena Triples");

    private Location location;
    private DatasetGraphTDB datasetGraphTDB;
    private Dataset dataset;
    private Graph graph;

    @Override
    public boolean openForBulkLoading() {
        location = Location.create("Apache Jena Triples Bulk loaded");
        datasetGraphTDB = TDBMaker.createDatasetGraphTDB(location, StoreParams.getDftStoreParams());
        return true;
    }

    @Override
    public boolean bulkInsert(Path sourcePath) {
        try {
            TDBLoader.load(datasetGraphTDB, sourcePath.toUri().toURL().toString(), true);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean closeAfterBulkLoading() {
        datasetGraphTDB.close();
        TDBMaker.releaseLocation(location);
        return true;
    }

    @Override
    public boolean openForSingleInserting() {
        return openRegular();
    }

    @Override
    public boolean singleInsert(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameA),
            NodeFactory.createURI(RDFVocabulary.relatedToNs),
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameB)
        );
        graph.add(triple);
        return true;
    }

    @Override
    public boolean closeAfterSingleInserting() {
        return closeRegular();
    }

    @Override
    public boolean openForIsRelated() {
        return openRegular();
    }

    @Override
    public boolean isRelated(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameA),
            NodeFactory.createURI(RDFVocabulary.relatedToNs),
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameB)
        );
        return graph.contains(triple);
    }

    @Override
    public boolean closeAfterIsRelated() {
        return closeRegular();
    }

    @Override
    public boolean openForReadingAllProperties() {
        return openRegular();
    }

    @Override
    public boolean readAllProperties(String tagNameA) {
        ExtendedIterator<Triple> iter =
            graph.find(
                NodeFactory.createURI(RDFVocabulary.tagNs + tagNameA),
                Node.ANY,
                Node.ANY
            );
        while (iter.hasNext()) {
            iter.next();
        }
        return true;
    }

    @Override
    public boolean closeAfterReadingAllProperties() {
        return closeRegular();
    }

    @Override
    public boolean openForSingleDeleting() {
        return openRegular();
    }

    @Override
    public boolean singleDelete(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameA),
            NodeFactory.createURI(RDFVocabulary.relatedToNs),
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameB)
        );
        graph.delete(triple);
        return true;
    }

    @Override
    public boolean closeAfterSingleDeleting() {
        return closeRegular();
    }

    @Override
    public long getDbSize() {
        try {
            return Files.size(regularDbPath);
        } catch (IOException e) {
            logger.fatal("Fail!", e);
            return -1;
        }
    }

    private boolean openRegular() {
        //DatasetGraph datasetGraph = TDBFactory.createDatasetGraph("Apache Jena Triples");
        dataset = TDBFactory.createDataset("Apache Jena Triples");
        graph = dataset.asDatasetGraph().getDefaultGraph();
        return true;
    }

    private boolean closeRegular() {
        dataset.close();
        return true;
    }
}
