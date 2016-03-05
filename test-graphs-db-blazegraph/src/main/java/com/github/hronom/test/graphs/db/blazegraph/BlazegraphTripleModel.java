package com.github.hronom.test.graphs.db.blazegraph;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.DataLoader;
import com.github.hronom.test.graphs.db.base.models.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.utils.RDFVocabulary;

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

public class BlazegraphTripleModel implements TripleDatabaseModel {
    private static final Logger logger = LogManager.getLogger();

    private BigdataSail sail;
    private BigdataSailRepository repo;
    private BigdataSailRepositoryConnection repositoryConnection;
    private BigdataValueFactory valueFactory;

    @Override
    public boolean openForBulkLoading() {
        return openBulkLoading();
    }

    @Override
    public boolean bulkLoad(Path sourcePath) {
        try {
            Properties props = new Properties();
            props.put(DataLoader.Options.CLOSURE, DataLoader.ClosureEnum.None.toString());
            DataLoader dataLoader = new DataLoader(props, repositoryConnection.getTripleStore());
            LoadStats loadStats = dataLoader.loadFiles(sourcePath.toFile(),
                null,
                RDFFormat.NTRIPLES,
                null,
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
    public boolean openForInsert() {
        return openRegular();
    }

    @Override
    public boolean insert(String tagNameA, String tagNameB) {
        // prepare a statement
        final BigdataURI subject = valueFactory.createURI(RDFVocabulary.tagNs, tagNameA);
        final BigdataURI predicate = valueFactory.createURI(RDFVocabulary.relatedToNs);
        final BigdataURI object = valueFactory.createURI(RDFVocabulary.tagNs, tagNameB);
        final BigdataStatement stmt = valueFactory.createStatement(subject, predicate, object);

        try {
            repositoryConnection.add(stmt);
        } catch (RepositoryException e) {
            logger.fatal("Fail!", e);
            return false;
        }
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
        try {
            return repositoryConnection.hasStatement(
                valueFactory.createURI(RDFVocabulary.tagNs, tagNameA),
                valueFactory.createURI(RDFVocabulary.relatedToNs),
                valueFactory.createURI(RDFVocabulary.tagNs, tagNameB),
                true
            );
        } catch (RepositoryException e) {
            logger.fatal("Fail!", e);
            return false;
        }
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
        try {
            RepositoryResult<Statement> results = repositoryConnection.getStatements(
                    valueFactory.createURI(RDFVocabulary.tagNs, tagNameA),
                    null,
                    null,
                    true
                );
            while (results.hasNext()) {
                results.next();
            }
        } catch (RepositoryException e) {
            logger.fatal("Fail!", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean closeAfterReadingAllProperties() {
        return closeRegular();
    }

    @Override
    public boolean openForRenewing() {
        return openRegular();
    }

    @Override
    public boolean deleting(String tagNameA, String tagNameB) {
        // prepare a statement
        final BigdataURI subject = valueFactory.createURI(RDFVocabulary.tagNs, tagNameA);
        final BigdataURI predicate = valueFactory.createURI(RDFVocabulary.relatedToNs);
        final BigdataURI object = valueFactory.createURI(RDFVocabulary.tagNs, tagNameB);
        final BigdataStatement stmt = valueFactory.createStatement(subject, predicate, object);

        try {
            repositoryConnection.remove(stmt);
        } catch (RepositoryException e) {
            logger.fatal("Fail!", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean inserting(String tagNameA, String tagNameB) {
        // prepare a statement
        final BigdataURI subject = valueFactory.createURI(RDFVocabulary.tagNs, tagNameA);
        final BigdataURI predicate = valueFactory.createURI(RDFVocabulary.relatedToNs);
        final BigdataURI object = valueFactory.createURI(RDFVocabulary.tagNs, tagNameB);
        final BigdataStatement stmt = valueFactory.createStatement(subject, predicate, object);

        try {
            repositoryConnection.add(stmt);
        } catch (RepositoryException e) {
            logger.fatal("Fail!", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean closeAfterRenewing() {
        return closeRegular();
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

    private boolean openRegular(){
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
            props.put(BigdataSail.Options.FILE, "Blazegraph Triples.jnl");
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
