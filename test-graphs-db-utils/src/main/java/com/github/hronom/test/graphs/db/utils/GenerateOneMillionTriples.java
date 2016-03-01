package com.github.hronom.test.graphs.db.utils;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.WriterStreamRDFFlat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Hello world!
 */
public class GenerateOneMillionTriples {
    private static final Path oneMillionNt = Paths.get("one_million.nt");

    public static void main(String[] args) {
        try (BufferedWriter bufferedWriter =
                 Files.newBufferedWriter(oneMillionNt, StandardCharsets.UTF_8)) {
            WriterStreamRDFFlat writerStreamRDFFlat = new WriterStreamRDFFlat(bufferedWriter);
            writerStreamRDFFlat.start();

            for (long i = 2; i < 1_000_000; i++) {
                writerStreamRDFFlat.triple(new Triple(
                    NodeFactory.createURI("http://test.com/Tag" + i),
                    NodeFactory.createURI("http://test.com/relatedTo"),
                    NodeFactory.createURI("http://test.com/Tag" + (i + 1))
                ));
                writerStreamRDFFlat.triple(new Triple(
                    NodeFactory.createURI("http://test.com/Tag" + (i - 1)),
                    NodeFactory.createURI("http://test.com/relatedTo"),
                    NodeFactory.createURI("http://test.com/Tag" + (i - 2))
                ));
            }

            writerStreamRDFFlat.finish();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }
}
