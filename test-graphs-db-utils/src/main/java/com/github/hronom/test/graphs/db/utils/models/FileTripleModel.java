package com.github.hronom.test.graphs.db.utils.models;

import com.github.hronom.test.graphs.db.common.models.TripleDatabaseModel;
import com.github.hronom.test.graphs.db.common.utils.RDFVocabulary;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.WriterStreamRDFFlat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileTripleModel implements TripleDatabaseModel {
    private final Path ntPath;

    private BufferedWriter bufferedWriter;
    private WriterStreamRDFFlat writerStreamRDFFlat;

    public FileTripleModel(String name) {
        ntPath = Paths.get(name);
    }

    @Override
    public boolean openForBulkLoading() {
        return true;
    }

    @Override
    public boolean bulkInsert(Path sourcePath) {
        return true;
    }

    @Override
    public boolean closeAfterBulkLoading() {
        return true;
    }

    @Override
    public boolean openForSingleInserting() {
        try {
            bufferedWriter = Files.newBufferedWriter(ntPath, StandardCharsets.UTF_8);
            writerStreamRDFFlat = new WriterStreamRDFFlat(bufferedWriter);
            writerStreamRDFFlat.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    @Override
    public boolean singleInsert(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameA),
            NodeFactory.createURI(RDFVocabulary.relatedToNs),
            NodeFactory.createURI(RDFVocabulary.tagNs + tagNameB)
        );
        writerStreamRDFFlat.triple(triple);
        return true;
    }

    @Override
    public boolean closeAfterSingleInserting() {
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
    public boolean openForSingleDeleting() {
        return true;
    }

    @Override
    public boolean singleDelete(String tagNameA, String tagNameB) {
        return true;
    }

    @Override
    public boolean closeAfterSingleDeleting() {
        return true;
    }

    @Override
    public long getDbSize() {
        return -1;
    }
}
