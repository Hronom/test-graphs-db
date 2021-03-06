package com.github.hronom.test.graphs.db.blazegraph;

import com.github.hronom.test.graphs.db.blazegraph.models.BlazegraphQuadModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;

/**
 * Try to insert ten millions of triples from two different dataset into one database with one graph.
 */
public class BlazegraphBulkLoadTwoDataSetsOneGraphTest {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws Exception {
        BlazegraphQuadModel blazegraphQuadModel = new BlazegraphQuadModel();
        blazegraphQuadModel.open();
        {
            logger.info("Start GraphA Blazegraph bulk insertion...");
            long begin = System.currentTimeMillis();
            blazegraphQuadModel.bulkUpload("GraphA", Paths.get("ten_millions_tags.nt"));
            long end = System.currentTimeMillis();
            logger.info("GraphA Blazegraph bulk insertion time: " + (end - begin) + " ms.");
        }
        {
            logger.info("Start GraphA Blazegraph bulk insertion...");
            long begin = System.currentTimeMillis();
            blazegraphQuadModel.bulkUpload("GraphA", Paths.get("ten_millions_marks.nt"));
            long end = System.currentTimeMillis();
            logger.info("GraphA Blazegraph bulk insertion time: " + (end - begin) + " ms.");
        }
        blazegraphQuadModel.close();
    }
}
