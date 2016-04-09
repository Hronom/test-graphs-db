package com.github.hronom.test.graphs.db.base.testers;

import com.github.hronom.test.graphs.db.base.utils.QuadsModelsUtils;
import com.github.hronom.test.graphs.db.base.models.QuadDatabaseTestModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;

public final class QuadsModelsTester {
    private static final Logger logger = LogManager.getLogger();

    private QuadsModelsTester() {
    }

    public static void test(QuadDatabaseTestModel quadDatabaseTestModel) {
        {
            logger.info("Start bulk insertion...");
            long begin = System.currentTimeMillis();
            quadDatabaseTestModel.openForBulkLoading();
            quadDatabaseTestModel.bulkInsert("graphA", Paths.get("one_million.nt"));
            quadDatabaseTestModel.closeAfterBulkLoading();
            long end = System.currentTimeMillis();
            logger.info("Bulk insertion time: " + (end - begin) + " ms.");
        }

        {
            logger.info("Start bulk insertion...");
            long begin = System.currentTimeMillis();
            quadDatabaseTestModel.openForBulkLoading();
            quadDatabaseTestModel.bulkInsert("graphB", Paths.get("one_million.nt"));
            quadDatabaseTestModel.closeAfterBulkLoading();
            long end = System.currentTimeMillis();
            logger.info("Bulk insertion time: " + (end - begin) + " ms.");
        }

        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.fill(quadDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Fill time: " + (end - begin) + " ms.");
        }

        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.contain(quadDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Contain time: " + (end - begin) + " ms.");
        }

        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.readAllProperties(quadDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Read all properties time: " + (end - begin) + " ms.");
        }

        {
            long begin = System.currentTimeMillis();
            QuadsModelsUtils.copyToGraph(quadDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Copy from graph to graph time: " + (end - begin) + " ms.");
        }
    }
}
