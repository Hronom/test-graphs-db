package com.github.hronom.test.graphs.db.apache.jena;

import com.github.hronom.test.graphs.db.base.TripleDatabaseModel;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.iterator.ExtendedIterator;

public class ApacheJenaTripleModel implements TripleDatabaseModel {
    private Dataset dataset;
    private Graph graph;

    @Override
    public boolean openForInsert() {
        return openRegular();
    }

    @Override
    public boolean insert(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameA),
            NodeFactory.createURI("http://www.test.org/relatedTo"),
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameB)
        );
        graph.add(triple);
        return true;
    }

    @Override
    public boolean closeAfterInsert() {
        return closeRegular();
    }

    @Override
    public boolean openForIsRelated() {
        return openRegular();
    }

    @Override
    public boolean isRelated(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameA),
            NodeFactory.createURI("http://www.test.org/relatedTo"),
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameB)
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
                NodeFactory.createURI("http://www.test.org/tag/" + tagNameA),
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
