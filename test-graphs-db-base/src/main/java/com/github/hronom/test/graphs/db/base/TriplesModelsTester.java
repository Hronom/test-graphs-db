package com.github.hronom.test.graphs.db.base;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;

public final class TriplesModelsTester {
    private static final Logger logger = LogManager.getLogger();

    private TriplesModelsTester() {
    }

    public static void test(TripleDatabaseModel tripleDatabaseModel) {
        {
            tripleDatabaseModel.openForBulkLoading();
            long begin = System.currentTimeMillis();
            tripleDatabaseModel.bulkLoad(Paths.get("one_million.nt"));
            long end = System.currentTimeMillis();
            logger.info("Bulk loading time: " + (end - begin) + " ms.");
            tripleDatabaseModel.closeAfterBulkLoading();
        }

        {
            tripleDatabaseModel.openForInsert();
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.fill(tripleDatabaseModel);
            long end = System.currentTimeMillis();
            logger.info("Fill time: " + (end - begin) + " ms.");
            tripleDatabaseModel.closeAfterInsert();
        }

        {
            tripleDatabaseModel.openForIsRelated();
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.contain(tripleDatabaseModel);
            long end = System.currentTimeMillis();
            logger.info("Contain time: " + (end - begin) + " ms.");
            tripleDatabaseModel.closeAfterIsRelated();
        }

        {
            tripleDatabaseModel.openForReadingAllProperties();
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.readAllProperties(tripleDatabaseModel);
            long end = System.currentTimeMillis();
            logger.info("Read all properties time: " + (end - begin) + " ms.");
            tripleDatabaseModel.closeAfterReadingAllProperties();
        }
    }
}
