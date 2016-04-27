package com.github.hronom.test.graphs.db.blazegraph;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataURIImpl;
import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Try to insert ten millions of triples from two different dataset into one database but with two
 * different graphs.
 */
public class BlazegraphSesameRemote {
    private static final Logger logger = LogManager.getLogger();
    private static final String serviceURL = "http://localhost:9999/blazegraph";

    public static void main(String[] args) {
        try {
            final RemoteRepositoryManager repo = new RemoteRepositoryManager(serviceURL,
                false /* useLBS */
            );

            try {

                JettyResponseListener response = getStatus(repo);
                logger.info(response.getResponseBody());

                // create a new namespace if not exists
                final String namespace = "Test";
                final Properties properties = new Properties();
                properties.setProperty("com.bigdata.rdf.sail.namespace", namespace);
                if (!namespaceExists(repo, namespace)) {
                    logger.info(String.format("Create namespace %s...", namespace));
                    repo.createRepository(namespace, properties);
                    logger.info(String.format("Create namespace %s done", namespace));
                } else {
                    logger.info(String.format("Namespace %s already exists", namespace));
                }

                //get properties for namespace
                logger.info(String.format("Property list for namespace %s", namespace));
                response = getNamespaceProperties(repo, namespace);
                logger.info(response.getResponseBody());

			/*
             * Load data from file located in the resource folder
			 * src/main/resources/data.n3
			 */
                Path resource = Paths.get("ten_millions_tags.nt");
                loadDataFromResource(repo, namespace, resource);

                System.out.println("load complete.");

                // execute query
                final TupleQueryResult result = repo.getRepositoryForNamespace(namespace)
                    .prepareTupleQuery("SELECT * {?s ?p ?o} LIMIT 100").evaluate();

                //result processing
                try {
                    while (result.hasNext()) {
                        final BindingSet bs = result.next();
                        logger.info(bs);
                    }
                } catch (QueryEvaluationException e) {
                    e.printStackTrace();
                } finally {
                    result.close();
                }
            } catch (QueryEvaluationException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    repo.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


	/*
     * Status request.
	 */

    private static JettyResponseListener getStatus(final RemoteRepositoryManager repo)
        throws Exception {

        final ConnectOptions opts = new ConnectOptions(serviceURL + "/status");
        opts.method = "GET";
        return repo.doConnect(opts);
    }

    /*
     * Check namespace already exists.
     */
    private static boolean namespaceExists(
        final RemoteRepositoryManager repo, final String namespace
    ) throws Exception {

        final GraphQueryResult res = repo.getRepositoryDescriptions();
        try {
            while (res.hasNext()) {
                final Statement stmt = res.next();
                if (stmt.getPredicate().toString().equals(SD.KB_NAMESPACE.stringValue())) {
                    if (namespace.equals(stmt.getObject().stringValue())) {
                        return true;
                    }
                }
            }
        } finally {
            res.close();
        }
        return false;
    }

    /*
     * Get namespace properties.
     */
    private static JettyResponseListener getNamespaceProperties(
        final RemoteRepositoryManager repo, final String namespace
    ) throws Exception {

        final ConnectOptions opts = new ConnectOptions(
            serviceURL + "/namespace/" + namespace + "/properties");
        opts.method = "GET";
        return repo.doConnect(opts);
    }

    /*
     * Load data into a namespace.
     */
    private static void loadDataFromResource(
        final RemoteRepositoryManager repo, final String namespace, Path resource
    ) throws Exception {
        BufferedReader br = Files.newBufferedReader(resource, StandardCharsets.UTF_8);
        if (br == null) {
            throw new IOException("Could not locate resource: " + resource);
        }
        try {
            RemoteRepository.AddOp addOp = new RemoteRepository.AddOp(br, RDFFormat.NTRIPLES);
            /*URI uri = new URIImpl("http://www.test.org/graph/GraphA");
            addOp.setContext(uri);*/
            repo.getRepositoryForNamespace(namespace)
                .add(addOp);
        } finally {
            br.close();
        }
    }
}
