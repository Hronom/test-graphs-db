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
import com.github.hronom.test.graphs.db.common.models.QuadDatabaseModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class BlazegraphQuadModel implements QuadDatabaseModel {
    private static final Logger logger = LogManager.getLogger();

    private BigdataSail sail;
    private BigdataSailRepository repo;
    private BigdataSailRepositoryConnection repositoryConnection;
    private BigdataValueFactory valueFactory;

    @Override
    public boolean open() {
        try {
            System.setProperty("log4j.configuration", this.getClass().getResource("/log4j2.xml").toString());

            Properties props = new Properties();
            //props.put(BigdataSail.Options.AXIOMS_CLASS, "com.bigdata.rdf.axioms.NoAxioms");
            //props.put(BigdataSail.Options.QUADS, true);
            props.put(BigdataSail.Options.QUADS_MODE, "true");
            /*props.put(BigdataSail.Options.STATEMENT_IDENTIFIERS, false);
            props.put(BigdataSail.Options.TEXT_INDEX, true);*/

            /*props.put(BigdataSail.Options.BUFFER_CAPACITY, 100000);*/

            // turn off automatic inference in the SAIL
            props.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

            // The name of the backing file.
            props.put(BigdataSail.Options.FILE, "Blazegraph.jnl");
            props.put(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskRW.toString());

            //props.put(BigdataSail.Options.COMMIT, DataLoader.CommitEnum.Incremental);

            // instantiate a sail
            sail = new BigdataSail(props);
            repo = new BigdataSailRepository(sail);
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
    public boolean bulkUpload(String graph, Path ntFilePath) {
        try {
            BigdataURI contextUri = valueFactory.createURI("http://www.test.org/graph/", graph);

            Properties props = new Properties();
            props.put(DataLoader.Options.CLOSURE, DataLoader.ClosureEnum.None.toString());
            DataLoader dataLoader = new DataLoader(props, repositoryConnection.getTripleStore());
            LoadStats loadStats = dataLoader.loadFiles(
                ntFilePath.toFile(),
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
    public boolean insert(String graph, String subject, String predicate, String object) {
        try {
            BigdataURI subjectUri = valueFactory.createURI("http://www.test.org/tag/", subject);
            BigdataURI predicateUri = valueFactory.createURI("http://www.test.org/relatedTo");
            BigdataURI objectUri = valueFactory.createURI("http://www.test.org/tag/", object);
            BigdataStatement stmt = valueFactory.createStatement(subjectUri, predicateUri, objectUri);
            BigdataURI contextUri = valueFactory.createURI("http://www.test.org/graph/", graph);
            repositoryConnection.add(stmt, contextUri);
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean commitChanges() {
        try {
            repositoryConnection.commit();
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean contains(String graph, String subject, String predicate, String object) {
        try {
            return repositoryConnection.hasStatement(
                valueFactory.createURI("http://www.test.org/tag/", subject),
                valueFactory.createURI("http://www.test.org/relatedTo"),
                valueFactory.createURI("http://www.test.org/tag/", object),
                true,
                valueFactory.createURI("http://www.test.org/graph/", graph)
            );
        } catch (RepositoryException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        repositoryConnection.commit();
        repositoryConnection.close();
        sail.shutDown();
    }
}
