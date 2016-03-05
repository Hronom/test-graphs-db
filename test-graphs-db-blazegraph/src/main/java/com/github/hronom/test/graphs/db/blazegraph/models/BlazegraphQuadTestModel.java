package com.github.hronom.test.graphs.db.blazegraph.models;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.github.hronom.test.graphs.db.base.models.QuadDatabaseTestModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import java.util.Properties;

public class BlazegraphQuadTestModel implements QuadDatabaseTestModel {
    private static final Logger logger = LogManager.getLogger();

    private BigdataSailRepositoryConnection repositoryConnection;
    private BigdataValueFactory valueFactory;

    @Override
    public boolean initialize() {
        try {
            // load journal properties from resources
            final Properties props = new Properties();
            // changing the axiom model to none essentially disables all inference
            //props.put(BigdataSail.Options.AXIOMS_CLASS, "com.bigdata.rdf.axioms.NoAxioms");
            //props.put(BigdataSail.Options.QUADS, true);
            props.put(BigdataSail.Options.QUADS_MODE, "true");
            /*props.put(BigdataSail.Options.STATEMENT_IDENTIFIERS, false);
            props.put(BigdataSail.Options.TEXT_INDEX, true);*/

            /*props.put(BigdataSail.Options.BUFFER_CAPACITY, 100000);*/

            // turn off automatic inference in the SAIL
            props.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

            // The name of the backing file.
            props.put(BigdataSail.Options.FILE, "Blazegraph Quads.jnl");
            props.put(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskRW.toString());

            //props.put(BigdataSail.Options.COMMIT, DataLoader.CommitEnum.Incremental);

            // instantiate a sail
            final BigdataSail sail = new BigdataSail(props);
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();
            repositoryConnection = repo.getConnection();
            valueFactory = repositoryConnection.getValueFactory();
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean insert(String tagNameA, String tagNameB, String graph) {
        // prepare a statement
        final BigdataURI subject = valueFactory.createURI("http://www.test.org/tag/", tagNameA);
        final BigdataURI predicate = valueFactory.createURI("http://www.test.org/relatedTo");
        final BigdataURI object = valueFactory.createURI("http://www.test.org/tag/", tagNameB);
        final BigdataStatement stmt = valueFactory.createStatement(subject, predicate, object);
        final BigdataURI contextUri = valueFactory.createURI("http://www.test.org/graph/", graph);

        try {
            repositoryConnection.add(stmt, contextUri);
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean isRelated(String tagNameA, String tagNameB, String graph) {
        try {
            return repositoryConnection.hasStatement(
                valueFactory.createURI("http://www.test.org/tag/", tagNameA),
                valueFactory.createURI("http://www.test.org/relatedTo"),
                valueFactory.createURI("http://www.test.org/tag/", tagNameB),
                true,
                valueFactory.createURI("http://www.test.org/graph/", graph)
            );
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean readAllProperties(String tagNameA, String graph) {
        try {
            RepositoryResult<Statement> results = repositoryConnection.getStatements(
                valueFactory.createURI("http://www.test.org/tag/", tagNameA),
                null,
                null,
                true,
                valueFactory.createURI("http://www.test.org/graph/", graph)
            );
            while (results.hasNext()) {
                results.next();
            }
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean copyToGraph(String graphA, String graphB) {
        try {
            RepositoryResult<Statement> results = repositoryConnection.getStatements(
                null,
                null,
                null,
                true,
                valueFactory.createURI("http://www.test.org/graph/", graphA)
            );
            while (results.hasNext()) {
                repositoryConnection.add(
                    results.next(),
                    valueFactory.createURI("http://www.test.org/graph/", graphB)
                );
            }
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    @Override
    public boolean commit() {
        try {
            repositoryConnection.commit();
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
