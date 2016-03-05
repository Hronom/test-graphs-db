package com.github.hronom.test.graphs.db.utils;

import com.github.hronom.test.graphs.db.base.utils.TriplesModelsUtils;
import com.github.hronom.test.graphs.db.utils.models.OneMillionTripleModel;

/**
 * Hello world!
 */
public class OneMillionTriplesNtFileCreator {
    public static void main(String[] args) {
        OneMillionTripleModel oneMillionTripleModel = new OneMillionTripleModel();
        oneMillionTripleModel.openForInsert();
        TriplesModelsUtils.fill(oneMillionTripleModel);
        oneMillionTripleModel.closeAfterInsert();
    }
}
