package com.github.hronom.test.graphs.db.apache.jena.models;

import com.github.hronom.test.graphs.db.base.models.QuadDatabaseModel;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Iterator;

public class ApacheJenaQuadModel implements QuadDatabaseModel {
    private DatasetGraph datasetGraph;

    @Override
    public boolean initialize() {
        datasetGraph = TDBFactory.createDatasetGraph("Apache Jena Quads");
        return true;
    }

    @Override
    public boolean insert(String tagNameA, String tagNameB, String graph) {
        Quad quad = new Quad(
            NodeFactory.createURI("http://www.test.org/graph/" + graph),
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameA),
            NodeFactory.createURI("http://www.test.org/relatedTo"),
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameB)
        );
        datasetGraph.add(quad);
        return true;
    }

    @Override
    public boolean isRelated(String tagNameA, String tagNameB, String graph) {
        Quad quad = new Quad(
            NodeFactory.createURI("http://www.test.org/graph/" + graph),
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameA),
            NodeFactory.createURI("http://www.test.org/relatedTo"),
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameB)
        );
        return datasetGraph.contains(quad);
    }

    @Override
    public boolean readAllProperties(String tagNameA, String graph) {
        Iterator<Quad> iter = datasetGraph.find(
            NodeFactory.createURI("http://www.test.org/graph/" + graph),
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
    public boolean copyToGraph(String graphA, String graphB) {
        Graph graphAGraph =
            datasetGraph.getGraph(NodeFactory.createURI("http://www.test.org/graph/" + graphA));

        ExtendedIterator<Triple> iter = GraphUtil.findAll(graphAGraph);

        Graph graphBGraph =
            datasetGraph.getGraph(NodeFactory.createURI("http://www.test.org/graph/" + graphB));
        GraphUtil.add(graphBGraph, iter);

        return true;
    }

    @Override
    public boolean commit() {
        return true;
    }
}
