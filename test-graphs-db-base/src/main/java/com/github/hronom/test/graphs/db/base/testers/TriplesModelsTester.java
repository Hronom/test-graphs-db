package com.github.hronom.test.graphs.db.base.testers;

import com.github.hronom.test.graphs.db.base.utils.TriplesModelsUtils;
import com.github.hronom.test.graphs.db.base.models.TripleDatabaseTestModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;

public final class TriplesModelsTester {
    private static final Logger logger = LogManager.getLogger();

    private TriplesModelsTester() {
    }

    public static void test(TripleDatabaseTestModel tripleDatabaseTestModel) {
        {
            tripleDatabaseTestModel.openForBulkLoading();
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.bulkLoad(Paths.get("one_million.nt"));
            long end = System.currentTimeMillis();
            logger.info("Bulk loading time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterBulkLoading();
        }

        {
            tripleDatabaseTestModel.openForSingleInserting();
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.fill(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Fill time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterSingleInserting();
        }

        {
            tripleDatabaseTestModel.openForIsRelated();
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.contain(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Contain time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterIsRelated();
        }

        {
            tripleDatabaseTestModel.openForReadingAllProperties();
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.readAllProperties(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Read all properties time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterReadingAllProperties();
        }

        {
            tripleDatabaseTestModel.openForReadingAllProperties();
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.readAllProperties(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Read all properties time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterReadingAllProperties();
        }
    }
}
