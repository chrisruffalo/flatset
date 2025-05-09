# FlatSet

## Overview
If you need to store a bunch of strings and search them relatively quickly
then this does that. It's silly and uses memory maps and is probably bad. 
It's not even a set. It's really not that thread safe either.

## Usage

```java
import io.github.chrisruffalo.flatset.FlatSet;
import java.nio.file.Paths;

final FlatSet bigSetOfStrings = new FlatSet(Paths.get("tmp/backing.set"));
bigSetOfStrings.load(Paths.get("source/file"));
// or...
bigSetOfStrings.add("some string");

// then
bigSetOfStrings.sort();

// and finally get the index of the string if it exists, -1 otherwise
bigSetOfStrings.search("some string");
```