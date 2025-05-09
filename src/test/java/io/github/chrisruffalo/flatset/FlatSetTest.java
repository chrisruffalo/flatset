package io.github.chrisruffalo.flatset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class FlatSetTest {

    @Test
    void nextRecordSize() {
        Assertions.assertEquals(FlatSet.INITIAL_RECORD_SIZE, FlatSet.nextRecordSize(0));
        Assertions.assertEquals(FlatSet.INITIAL_RECORD_SIZE + FlatSet.RECORD_SIZE_EXPANSION_AMOUNT, FlatSet.nextRecordSize(FlatSet.INITIAL_RECORD_SIZE) );
    }

    @Test
    void thousand() throws IOException {
        final Path source = Paths.get("src/test/resources/top-1000-domains.txt");
        final Path backing = Paths.get("target/tests/thousand-backing.txt");
        Files.createDirectories(backing.getParent());

        final FlatSet flatSet = new FlatSet(backing);
        flatSet.load(source);
        flatSet.sort();

        Assertions.assertEquals(-1, flatSet.search("notinfile"));

        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                Assertions.assertNotEquals(-1, flatSet.search(line), () -> String.format("could not find line %s", line));
            });
        }
    }

    @Test
    void million() throws IOException {
        final Path source = Paths.get("src/test/resources/top-1m-domains.txt");
        final Path backing = Paths.get("target/tests/million-backing.txt");
        Files.createDirectories(backing.getParent());

        final FlatSet flatSet = new FlatSet(backing);
        flatSet.load(source);
        flatSet.sort();

        Assertions.assertEquals(-1, flatSet.search("notinfile"));

        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                Assertions.assertNotEquals(-1, flatSet.search(line), () -> String.format("could not find line %s", line));
            });
        }
    }

    @Test
    void thousandAdd() throws IOException {
        final Path source = Paths.get("src/test/resources/top-1000-domains.txt");
        final Path backing = Paths.get("target/tests/thousand-add-backing.txt");
        Files.createDirectories(backing.getParent());

        final FlatSet flatSet = new FlatSet(backing);
        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                try {
                    flatSet.add(line);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        flatSet.sort();

        Assertions.assertEquals(-1, flatSet.search("notinfile"));

        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                Assertions.assertNotEquals(-1, flatSet.search(line), () -> String.format("could not find line %s", line));
            });
        }
    }

    @Test
    @Disabled // until we find a way to speed this up
    void millionAdd() throws IOException {
        final Path source = Paths.get("src/test/resources/top-1m-domains.txt");
        final Path backing = Paths.get("target/tests/million-add-backing.txt");
        Files.createDirectories(backing.getParent());

        final FlatSet flatSet = new FlatSet(backing);
        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                try {
                    flatSet.add(line);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        flatSet.sort();

        Assertions.assertEquals(-1, flatSet.search("notinfile"));

        try(BufferedReader reader = Files.newBufferedReader(source)) {
            reader.lines().forEach(line -> {
                Assertions.assertNotEquals(-1, flatSet.search(line), () -> String.format("could not find line %s", line));
            });
        }
    }

}