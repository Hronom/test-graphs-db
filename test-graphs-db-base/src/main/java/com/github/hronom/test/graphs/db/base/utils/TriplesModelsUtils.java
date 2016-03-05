package com.github.hronom.test.graphs.db.base.utils;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class TriplesModelsUtils {
    private static final Logger logger = LogManager.getLogger();

    private static final long totalCount = 1_000_000;

    private TriplesModelsUtils() {
    }

    public static boolean fill(TripleDatabaseModel tripleDatabaseModel) {
        long beginTime = System.currentTimeMillis();

        for (long i = 2; i < totalCount; i++) {
            if (!tripleDatabaseModel.insert("Tag" + i, "Tag" + (i + 1))) {
                return false;
            }

            if (!tripleDatabaseModel.insert("Tag" + (i - 1), "Tag" + (i - 2))) {
                return false;
            }

            long currentTime = System.currentTimeMillis();
            if ((currentTime - beginTime) > 3000) {
                beginTime = System.currentTimeMillis();
                logger.info("Count of inserted tags: " + i);
            }
        }

        logger.info("Count of inserted tags: " + totalCount);

        return true;
    }

    public static boolean contain(TripleDatabaseModel tripleDatabaseModel) {
        long beginTime = System.currentTimeMillis();

        for (long i = 2; i < totalCount; i++) {
            if (!tripleDatabaseModel.isRelated("Tag" + i, "Tag" + (i + 1))) {
                return false;
            }

            if (!tripleDatabaseModel.isRelated("Tag" + (i - 1), "Tag" + (i - 2))) {
                return false;
            }

            long currentTime = System.currentTimeMillis();
            if ((currentTime - beginTime) > 3000) {
                beginTime = System.currentTimeMillis();
                logger.info("Count of checks: " + i);
            }
        }

        logger.info("Count of checks: " + totalCount);

        return true;
    }

    public static boolean readAllProperties(TripleDatabaseModel tripleDatabaseModel) {
        long beginTime = System.currentTimeMillis();

        for (long i = 2; i < totalCount; i++) {
            if (!tripleDatabaseModel.readAllProperties("Tag" + i)) {
                return false;
            }

            if (!tripleDatabaseModel.readAllProperties("Tag" + (i - 1))) {
                return false;
            }

            long currentTime = System.currentTimeMillis();
            if ((currentTime - beginTime) > 3000) {
                beginTime = System.currentTimeMillis();
                logger.info("Count of processed tags: " + i);
            }
        }

        logger.info("Count of processed tags: " + totalCount);

        return true;
    }
}
