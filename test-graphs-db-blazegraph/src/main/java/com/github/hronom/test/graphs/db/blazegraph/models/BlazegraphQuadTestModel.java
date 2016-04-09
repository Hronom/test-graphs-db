package com.github.hronom.test.graphs.db.blazegraph.models;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.DataLoader;
import com.github.hronom.test.graphs.db.base.models.QuadDatabaseTestModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class BlazegraphQuadTestModel implements QuadDatabaseTestModel {
    private static final Logger logger = LogManager.getLogger();

    private BigdataSail sail;
    private BigdataSailRepository repo;
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
    public boolean openForBulkLoading() {
        return openBulkLoading();
    }

    @Override
    public boolean bulkInsert(String graph, Path sourcePath) {
        try {
            final BigdataURI contextUri = valueFactory.createURI("http://www.test.org/graph/", graph);

            Properties props = new Properties();
            props.put(DataLoader.Options.CLOSURE, DataLoader.ClosureEnum.None.toString());
            DataLoader dataLoader = new DataLoader(props, repositoryConnection.getTripleStore());
            LoadStats loadStats = dataLoader.loadFiles(sourcePath.toFile(),
                null,
                RDFFormat.NTRIPLES,
                contextUri.stringValue(),
                new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".nt");
                    }
                }
            );
            dataLoader.endSource();
            repositoryConnection.commit();
        }  catch (IOException exception) {
            logger.fatal(exception);
            return false;
        } catch (RepositoryException exception) {
            logger.fatal(exception);
            return false;
        }

        return true;
    }

    @Override
    public boolean closeAfterBulkLoading() {
        return closeRegular();
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

    private boolean openBulkLoading(){
        try {
            // load journal properties from resources
            final Properties props = new Properties();
            // changing the axiom model to none essentially disables all inference
            //props.put(BigdataSail.Options.AXIOMS_CLASS, "com.bigdata.rdf.axioms.NoAxioms");
            //props.put(BigdataSail.Options.QUADS, true);
            props.put(BigdataSail.Options.QUADS_MODE, "false");
            /*props.put(BigdataSail.Options.STATEMENT_IDENTIFIERS, false);
            props.put(BigdataSail.Options.TEXT_INDEX, true);*/

            /*props.put(BigdataSail.Options.BUFFER_CAPACITY, 100000);*/

            // turn off automatic inference in the SAIL
            props.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

            // The name of the backing file.
            props.put(BigdataSail.Options.FILE, "Blazegraph Triples Bulk loaded.jnl");
            props.put(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskRW.toString());

            //props.put(BigdataSail.Options.COMMIT, DataLoader.CommitEnum.Incremental);

            // instantiate a sail
            sail = new BigdataSail(props);
            repo = new BigdataSailRepository(sail);
            repo.initialize();
            repositoryConnection = repo.getConnection();
            valueFactory = repositoryConnection.getValueFactory();
        } catch (RepositoryException e) {
            logger.fatal("Fail!", e);
            return false;
        }
        return true;
    }

    private boolean closeRegular(){
        try {
            repositoryConnection.commit();
            repositoryConnection.close();
            sail.shutDown();
        } catch (RepositoryException e) {
            logger.fatal("Fail!", e);
            return false;
        } catch (SailException e) {
            logger.fatal("Fail!", e);
            return false;
        }
        return true;
    }
}
