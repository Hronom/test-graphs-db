package com.github.hronom.test.graphs.db.utils;

import com.github.hronom.test.graphs.db.utils.models.FileTripleModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TenMillionsTriplesNtFileCreator {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        long count = 10_000_000L;

        FileTripleModel fileTripleModel = new FileTripleModel("ten_millions.nt");
        fileTripleModel.openForSingleInserting();

        long beginTime = System.currentTimeMillis();
        for(long i = 0; i < count; ++i) {
            fileTripleModel.singleInsert("Tag" + i, "Tag" + (i + 1L));
            long currentTime = System.currentTimeMillis();
            if(currentTime - beginTime > 3000L) {
                beginTime = System.currentTimeMillis();
                logger.info("Count of inserted tags: " + i);
            }
        }
        logger.info("Count of inserted tags: " + count);

        fileTripleModel.closeAfterSingleInserting();
    }
}