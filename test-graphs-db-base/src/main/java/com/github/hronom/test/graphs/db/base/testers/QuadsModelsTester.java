package com.github.hronom.test.graphs.db.base.testers;

import com.github.hronom.test.graphs.db.base.utils.QuadsModelsUtils;
import com.github.hronom.test.graphs.db.base.models.QuadDatabaseModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class QuadsModelsTester {
    private static final Logger logger = LogManager.getLogger();

    private QuadsModelsTester() {
    }

    public static void test(QuadDatabaseModel quadDatabaseModel) {
        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.fill(quadDatabaseModel);
            long end = System.currentTimeMillis();
            logger.info("Fill time: " + (end - begin) + " ms.");
        }

        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.contain(quadDatabaseModel);
            long end = System.currentTimeMillis();
            logger.info("Contain time: " + (end - begin) + " ms.");
        }

        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.readAllProperties(quadDatabaseModel);
            long end = System.currentTimeMillis();
            logger.info("Read all properties time: " + (end - begin) + " ms.");
        }

        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.copyToGraph(quadDatabaseModel);
            long end = System.currentTimeMillis();
            logger.info("Copy from graph to graph time: " + (end - begin) + " ms.");
        }
    }
}
