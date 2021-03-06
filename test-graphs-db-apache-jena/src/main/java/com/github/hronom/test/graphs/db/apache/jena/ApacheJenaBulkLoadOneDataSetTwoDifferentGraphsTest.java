package com.github.hronom.test.graphs.db.apache.jena;

import com.github.hronom.test.graphs.db.apache.jena.models.ApacheJenaQuadModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;

/**
 * Try to insert ten millions of triples from one dataset into one database but with two different graphs.
 */
public class ApacheJenaBulkLoadOneDataSetTwoDifferentGraphsTest {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws Exception {
        ApacheJenaQuadModel quadDatabaseTestModel = new ApacheJenaQuadModel();
        quadDatabaseTestModel.open();
        {
            logger.info("Start GraphA Apache Jena bulk insertion...");
            long begin = System.currentTimeMillis();
            quadDatabaseTestModel.bulkUpload("GraphA", Paths.get("ten_millions_tags.nt"));
            long end = System.currentTimeMillis();
            logger.info("GraphA Apache Jena bulk insertion time: " + (end - begin) + " ms.");
        }
        {
            logger.info("Start GraphB Apache Jena bulk insertion...");
            long begin = System.currentTimeMillis();
            quadDatabaseTestModel.bulkUpload("GraphB", Paths.get("ten_millions_tags.nt"));
            long end = System.currentTimeMillis();
            logger.info("GraphB Apache Jena bulk insertion time: " + (end - begin) + " ms.");
        }
        quadDatabaseTestModel.close();
    }
}
