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
            logger.info("Start bulk insertion...");
            long begin = System.currentTimeMillis();
            tripleDatabaseTestModel.bulkInsert(Paths.get("one_million.nt"));
            long end = System.currentTimeMillis();
            logger.info("Bulk insertion time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterBulkLoading();
        }

        {
            tripleDatabaseTestModel.openForSingleInserting();
            logger.info("Start inserting tags by \"single insert\" method...");
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.insertTags(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Tags insertion time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseTestModel.getDbSize() + " bytes.");
            tripleDatabaseTestModel.closeAfterSingleInserting();
        }

        {
            tripleDatabaseTestModel.openForSingleDeleting();
            logger.info("Start deleting tags by \"single delete\" method...");
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.deleteTags(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Tags deletion time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseTestModel.getDbSize() + " bytes.");
            tripleDatabaseTestModel.closeAfterSingleDeleting();
        }

        {
            tripleDatabaseTestModel.openForSingleInserting();
            logger.info("Start inserting tags by \"single insert\" method after deleting...");
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.insertTags(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Tags insertion after deleting time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseTestModel.getDbSize() + " bytes.");
            tripleDatabaseTestModel.closeAfterSingleInserting();
        }

        {
            tripleDatabaseTestModel.openForIsRelated();
            logger.info("Start check of tags relations...");
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.checkTagsRelation(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Tags relations check time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterIsRelated();
        }

        {
            tripleDatabaseTestModel.openForReadingAllProperties();
            logger.info("Start reading all nodes properties...");
            long begin = System.currentTimeMillis();
            TriplesModelsUtils.readAllProperties(tripleDatabaseTestModel);
            long end = System.currentTimeMillis();
            logger.info("Parameters of nodes read time: " + (end - begin) + " ms.");
            tripleDatabaseTestModel.closeAfterReadingAllProperties();
        }
    }
}
