package io.github.chrisruffalo.fileset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class FileSetTest {

    @Test
    void thousand() throws IOException {
        final Path source = Paths.get("src/test/resources/top-1000-domains.txt");
        final Path backing = Paths.get("target/tests/thousand-backing");
        Files.createDirectories(backing.getParent());

        final FileSet fileSet = new FileSet(backing);
        fileSet.load(source);
        fileSet.sort();

        Assertions.assertEquals(-1, fileSet.search("notinfile"));

        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                Assertions.assertNotEquals(-1, fileSet.search(line), String.format("could not find line %s", line));
            });
        }
    }

    @Test
    void million() throws IOException {
        final Path source = Paths.get("src/test/resources/top-1m-domains.txt");
        final Path backing = Paths.get("target/tests/million-backing");
        Files.createDirectories(backing.getParent());

        final FileSet fileSet = new FileSet(backing);
        fileSet.load(source);
        fileSet.sort();

        Assertions.assertEquals(-1, fileSet.search("notinfile"));

        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                Assertions.assertNotEquals(-1, fileSet.search(line), String.format("could not find line %s", line));
            });
        }
    }

}