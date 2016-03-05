package com.github.hronom.test.graphs.db.utils;

import com.github.hronom.test.graphs.db.base.utils.TriplesModelsUtils;
import com.github.hronom.test.graphs.db.utils.models.OneMillionTripleTestModel;

/**
 * Hello world!
 */
public class OneMillionTriplesNtFileCreator {
    public static void main(String[] args) {
        OneMillionTripleTestModel oneMillionTripleModel = new OneMillionTripleTestModel();
        oneMillionTripleModel.openForSingleInserting();
        TriplesModelsUtils.insertTags(oneMillionTripleModel);
        oneMillionTripleModel.closeAfterSingleInserting();
    }
}
