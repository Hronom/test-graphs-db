package com.github.hronom.test.graphs.db.base;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class QuadsModelsUtils {
    private static final Logger logger = LogManager.getLogger();

    private static final long totalCount = 1_000_000;

    private QuadsModelsUtils() {
    }

    public static boolean fill(QuadDatabaseModel quadDatabaseModel) {
        long beginTime = System.currentTimeMillis();

        for (long i = 2; i < totalCount; i++) {
            // Graph 1.
            if (!quadDatabaseModel.insert("Tag" + i, "Tag" + (i + 1), "graph1")) {
                return false;
            }

            if (!quadDatabaseModel.insert("Tag" + (i - 1), "Tag" + (i - 2), "graph1")) {
                return false;
            }

            // Graph 2.
            if (!quadDatabaseModel.insert("Tag" + i, "Tag" + (i + 1), "graph2")) {
                return false;
            }

            if (!quadDatabaseModel.insert("Tag" + (i - 1), "Tag" + (i - 2), "graph2")) {
                return false;
            }

            // Graph 3.
            if (!quadDatabaseModel.insert("Tag" + (i - 3), "Tag" + (i - 3), "graph3")) {
                return false;
            }

            long currentTime = System.currentTimeMillis();
            if ((currentTime - beginTime) > 3000) {
                beginTime = System.currentTimeMillis();
                logger.info("Count of inserted tags: " + i);
            }
        }

        quadDatabaseModel.commit();

        logger.info("Count of inserted tags: " + totalCount);

        return true;
    }

    public static boolean contain(QuadDatabaseModel quadDatabaseModel) {
        long beginTime = System.currentTimeMillis();

        for (long i = 2; i < totalCount; i++) {
            // Graph 1.
            if (!quadDatabaseModel.isRelated("Tag" + i, "Tag" + (i + 1), "graph1")) {
                return false;
            }

            if (!quadDatabaseModel.isRelated("Tag" + (i - 1), "Tag" + (i - 2), "graph1")) {
                return false;
            }

            // Graph 2.
            if (!quadDatabaseModel.isRelated("Tag" + i, "Tag" + (i + 1), "graph2")) {
                return false;
            }

            if (!quadDatabaseModel.isRelated("Tag" + (i - 1), "Tag" + (i - 2), "graph2")) {
                return false;
            }

            // Graph 3.
            if (!quadDatabaseModel.isRelated("Tag" + (i - 3), "Tag" + (i - 3), "graph3")) {
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

    public static boolean readAllProperties(QuadDatabaseModel quadDatabaseModel) {
        long beginTime = System.currentTimeMillis();

        for (long i = 2; i < totalCount; i++) {
            // Graph 1.
            if (!quadDatabaseModel.readAllProperties("Tag" + i, "graph1")) {
                return false;
            }

            if (!quadDatabaseModel.readAllProperties("Tag" + (i - 1), "graph1")) {
                return false;
            }

            // Graph 2.
            if (!quadDatabaseModel.readAllProperties("Tag" + i, "graph2")) {
                return false;
            }

            if (!quadDatabaseModel.readAllProperties("Tag" + (i - 1), "graph2")) {
                return false;
            }

            // Graph 3.
            if (!quadDatabaseModel.readAllProperties("Tag" + (i - 3), "graph3")) {
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

    public static boolean copyToGraph(QuadDatabaseModel quadDatabaseModel) {
        return quadDatabaseModel.copyToGraph("graph1", "graph4");
    }
}
