package com.github.hronom.test.graphs.db.utils.models;

import com.github.hronom.test.graphs.db.base.models.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.base.utils.RDFVocabulary;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.WriterStreamRDFFlat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class OneMillionTripleModel implements TripleDatabaseModel {
    private final Path oneMillionNt = Paths.get("one_million.nt");

    private BufferedWriter bufferedWriter;
    private WriterStreamRDFFlat writerStreamRDFFlat;

    @Override
    public boolean openForBulkLoading() {
        return true;
    }

    @Override
    public boolean bulkLoad(Path sourcePath) {
        return true;
    }

    @Override
    public boolean closeAfterBulkLoading() {
        return true;
    }

    @Override
    public boolean openForInsert() {
        try {
            bufferedWriter = Files.newBufferedWriter(oneMillionNt, StandardCharsets.UTF_8);
            writerStreamRDFFlat = new WriterStreamRDFFlat(bufferedWriter);
            writerStreamRDFFlat.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    @Override
    public boolean insert(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameA),
            NodeFactory.createURI(RDFVocabulary.relatedToNs),
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameB)
        );
        writerStreamRDFFlat.triple(triple);
        return true;
    }

    @Override
    public boolean closeAfterInsert() {
        writerStreamRDFFlat.finish();
        try {
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public boolean openForIsRelated() {
        return true;
    }

    @Override
    public boolean isRelated(String tagNameA, String tagNameB) {
        return true;
    }

    @Override
    public boolean closeAfterIsRelated() {
        return true;
    }

    @Override
    public boolean openForReadingAllProperties() {
        return true;
    }

    @Override
    public boolean readAllProperties(String tagNameA) {
        return true;
    }

    @Override
    public boolean closeAfterReadingAllProperties() {
        return true;
    }

    @Override
    public boolean openForRenewing() {
        return true;
    }

    @Override
    public boolean deleting(String tagNameA, String tagNameB) {
        return true;
    }

    @Override
    public boolean inserting(String tagNameA, String tagNameB) {
        return true;
    }

    @Override
    public boolean closeAfterRenewing() {
        return true;
    }
}
