package com.github.hronom.test.graphs.db.blazegraph;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import java.util.Properties;

/**
 * Try to insert ten millions of triples from two different dataset into one database but with two different graphs.
 */
public class BlazegraphTxTest {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        try {
            //System.setProperty("log4j.configuration", this.getClass().getResource("/log4j2.xml").toString());

            Properties props = new Properties();
            //props.put(BigdataSail.Options.AXIOMS_CLASS, "com.bigdata.rdf.axioms.NoAxioms");
            props.put(BigdataSail.Options.QUADS, "true");
            props.put(BigdataSail.Options.QUADS_MODE, "true");
            /*props.put(BigdataSail.Options.STATEMENT_IDENTIFIERS, false);
            props.put(BigdataSail.Options.TEXT_INDEX, true);*/

            /*props.put(BigdataSail.Options.BUFFER_CAPACITY, 100000);*/

            // turn off automatic inference in the SAIL
            props.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

            // The name of the backing file.
            props.put(BigdataSail.Options.FILE, "Blazegraph_transactions.jnl");
            props.put(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskRW.toString());

            // https://wiki.blazegraph.com/wiki/index.php/CommonProblems#java.lang.IllegalStateException:_UNISOLATED_connection_is_not_reentrant
            props.put(BigdataSail.Options.ISOLATABLE_INDICES, "true");
            // Set JUSTIFY to false:
            // https://sourceforge.net/p/bigdata/discussion/676946/thread/730fe1c1/
            props.put(BigdataSail.Options.JUSTIFY, "false");
            //props.put(Journal.Options.GROUP_COMMIT, "true");

            //props.put(BigdataSail.Options.COMMIT, DataLoader.CommitEnum.Incremental);

            // instantiate a sail
            BigdataSail sail = new BigdataSail(props);
            BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();

            BigdataSailRepositoryConnection repositoryConnection = repo.getReadWriteConnection();
            BigdataSailRepositoryConnection repositoryConnection2 = repo.getReadWriteConnection();

            fill(repositoryConnection, "1", "2");
            remove(repositoryConnection, "2", "2");
            fill(repositoryConnection2, "2", "2");
            fill(repositoryConnection2, "3", "2");

            repositoryConnection.commit();
            repositoryConnection2.commit();

            BigdataSailRepositoryConnection repositoryConnection3 = repo.getConnection();
            RepositoryResult<Statement> repositoryResult =
                repositoryConnection3.getStatements(null, null, null, true);
            while(repositoryResult.hasNext()) {
                System.out.println(repositoryResult.next());
            }
        } catch (Exception exception) {
            logger.error("Fail!", exception);
        }

        /*IndexMetadata indexMetadata = new IndexMetadata("testIndex", UUID.randomUUID());

        // this index will support transactions.
        indexMetadata.setIsolatable(true);

        // register the index.
        repositoryConnection.getSailConnection().begin().registerIndex(indexMetadata);*/
    }

    private static void fill(BigdataSailRepositoryConnection repositoryConnection, String subject, String object)
        throws RepositoryException {
        BigdataValueFactory valueFactory = repositoryConnection.getValueFactory();
        BigdataURI subjectUri = valueFactory.createURI("http://www.test.org/tag/", subject);
        BigdataURI predicateUri = valueFactory.createURI("http://www.test.org/relatedTo");
        BigdataURI objectUri = valueFactory.createURI("http://www.test.org/tag/", object);
        BigdataStatement stmt = valueFactory.createStatement(subjectUri, predicateUri, objectUri);
        BigdataURI contextUri = valueFactory.createURI("http://www.test.org/graph/", "graphA");
        repositoryConnection.add(stmt, contextUri);
    }

    private static void remove(BigdataSailRepositoryConnection repositoryConnection, String subject, String object)
        throws RepositoryException {
        BigdataValueFactory valueFactory = repositoryConnection.getValueFactory();
        BigdataURI subjectUri = valueFactory.createURI("http://www.test.org/tag/", subject);
        BigdataURI predicateUri = valueFactory.createURI("http://www.test.org/relatedTo");
        BigdataURI objectUri = valueFactory.createURI("http://www.test.org/tag/", object);
        BigdataStatement stmt = valueFactory.createStatement(subjectUri, predicateUri, objectUri);
        BigdataURI contextUri = valueFactory.createURI("http://www.test.org/graph/", "graphA");
        repositoryConnection.remove(stmt, contextUri);
    }
}

