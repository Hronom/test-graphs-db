package com.github.hronom.test.graphs.db.common.testers;

import com.github.hronom.test.graphs.db.common.utils.TriplesModelsUtils;
import com.github.hronom.test.graphs.db.common.models.TripleDatabaseModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;

public final class TriplesModelsTester {
    private static final Logger logger = LogManager.getLogger();

    private TriplesModelsTester() {
    }

    public static void test(TripleDatabaseModel tripleDatabaseModel) {
        {
            logger.info("Start bulk insertion...");
            long begin = System.currentTimeMillis();
            tripleDatabaseModel.openForBulkLoading();
            tripleDatabaseModel.bulkInsert(Paths.get("one_million.nt"));
            tripleDatabaseModel.closeAfterBulkLoading();
            long end = System.currentTimeMillis();
            logger.info("Bulk insertion time: " + (end - begin) + " ms.");
        }

        {
            logger.info("Start inserting tags by \"single insert\" method...");
            long begin = System.currentTimeMillis();
            tripleDatabaseModel.openForSingleInserting();
            TriplesModelsUtils.insertTags(tripleDatabaseModel);
            tripleDatabaseModel.closeAfterSingleInserting();
            long end = System.currentTimeMillis();
            logger.info("Tags insertion time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseModel.getDbSize() + " bytes.");
        }

        {
            logger.info("Start deleting tags by \"single delete\" method...");
            long begin = System.currentTimeMillis();
            tripleDatabaseModel.openForSingleDeleting();
            TriplesModelsUtils.deleteTags(tripleDatabaseModel);
            tripleDatabaseModel.closeAfterSingleDeleting();
            long end = System.currentTimeMillis();
            logger.info("Tags deletion time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseModel.getDbSize() + " bytes.");
        }

        {
            logger.info("Start inserting tags by \"single insert\" method after deleting...");
            long begin = System.currentTimeMillis();
            tripleDatabaseModel.openForSingleInserting();
            TriplesModelsUtils.insertTags(tripleDatabaseModel);
            tripleDatabaseModel.closeAfterSingleInserting();
            long end = System.currentTimeMillis();
            logger.info("Tags insertion after deleting time: " + (end - begin) + " ms., DB size: " +
                        tripleDatabaseModel.getDbSize() + " bytes.");
        }

        {
            logger.info("Start check of tags relations...");
            long begin = System.currentTimeMillis();
            tripleDatabaseModel.openForIsRelated();
            TriplesModelsUtils.checkTagsRelation(tripleDatabaseModel);
            tripleDatabaseModel.closeAfterIsRelated();
            long end = System.currentTimeMillis();
            logger.info("Tags relations check time: " + (end - begin) + " ms.");
        }

        {
            logger.info("Start reading all nodes properties...");
            long begin = System.currentTimeMillis();
            tripleDatabaseModel.openForReadingAllProperties();
            TriplesModelsUtils.readAllProperties(tripleDatabaseModel);
            tripleDatabaseModel.closeAfterReadingAllProperties();
            long end = System.currentTimeMillis();
            logger.info("Parameters of nodes read time: " + (end - begin) + " ms.");
        }
    }
}
