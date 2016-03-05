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
            logger.info("Start bulk insertion...");
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.openForBulkLoading();
            tripleDatabaseTestModel.bulkInsert(Paths.get("one_million.nt"));
            tripleDatabaseTestModel.closeAfterBulkLoading();
            long end = System.currentTimeMillis();
            logger.info("Bulk insertion time: " + (end - begin) + " ms.");
        }

        {
            logger.info("Start inserting tags by \"single insert\" method...");
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.openForSingleInserting();
            TriplesModelsUtils.insertTags(tripleDatabaseTestModel);
            tripleDatabaseTestModel.closeAfterSingleInserting();
            long end = System.currentTimeMillis();
            logger.info("Tags insertion time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseTestModel.getDbSize() + " bytes.");
        }

        {
            logger.info("Start deleting tags by \"single delete\" method...");
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.openForSingleDeleting();
            TriplesModelsUtils.deleteTags(tripleDatabaseTestModel);
            tripleDatabaseTestModel.closeAfterSingleDeleting();
            long end = System.currentTimeMillis();
            logger.info("Tags deletion time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseTestModel.getDbSize() + " bytes.");
        }

        {
            logger.info("Start inserting tags by \"single insert\" method after deleting...");
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.openForSingleInserting();
            TriplesModelsUtils.insertTags(tripleDatabaseTestModel);
            tripleDatabaseTestModel.closeAfterSingleInserting();
            long end = System.currentTimeMillis();
            logger.info("Tags insertion after deleting time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseTestModel.getDbSize() + " bytes.");
        }

        {
            logger.info("Start check of tags relations...");
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.openForIsRelated();
            TriplesModelsUtils.checkTagsRelation(tripleDatabaseTestModel);
            tripleDatabaseTestModel.closeAfterIsRelated();
            long end = System.currentTimeMillis();
            logger.info("Tags relations check time: " + (end - begin) + " ms.");
        }

        {
            logger.info("Start reading all nodes properties...");
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.openForReadingAllProperties();
            TriplesModelsUtils.readAllProperties(tripleDatabaseTestModel);
            tripleDatabaseTestModel.closeAfterReadingAllProperties();
            long end = System.currentTimeMillis();
            logger.info("Parameters of nodes read time: " + (end - begin) + " ms.");
        }
    }
}
