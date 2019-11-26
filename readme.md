
# Simple Record Store

[<img src="https://github.com/simbo1905/simple-record-store/workflows/JavaCI/badge.svg">](https://github.com/simbo1905/simple-record-store/actions)

Simple Record Store is a pure Java persistent hash table with a predefined maximum key length. Records are written into 
a single file. All of the keys must fit into heap but all values are offloaded onto disk. The order of writes are 
carefully arranged so that crashes will not corrupt the state of the data on disk. The project has no runtime 
dependencies outside of the core JDK. 

## Using

The latest release on maven central is:

```xml
<dependency>
	<groupId>com.github.trex-paxos</groupId>
	<artifactId>simple-record-store</artifactId>
	<version>1.0.0-RC5</version>
</dependency>
```

See `SimpleRecordStoreApiTests.java` for examples of the minimal public API. 

The public API useS a `ByteSequence` as the key. This is a lightweight wrapper to a byte array hat implements `equals` 
and `hashCode` . This means that we know exactly how the store the keys on disk and can use it as the key to a HashMap: 

```java
    @Synchronized
    public boolean recordExists(ByteSequence key)
```

This is discussed [here](https://stackoverflow.com/a/58923559/329496). You can either wrap a byte array or copy it: 

```java
    /*
     * This takes a defensive copy of the passed bytes. This should be used if the array can be recycled by the caller.
     */
    public static ByteSequence copyOf(byte[] bytes)

    /*
     * This does not take a defensive copy of the passed bytes. This should be used only if you know that the array cannot be recycled.
     */
    public static ByteSequence of(byte[] bytes) 
```

An example of where you need to use `copyOf` would be where you are taking the bytes from a "direct" ByteBuffer where the 
array will be recycled. Examples of where you can safely use `of` to wrap the array would be where you asked a 
String to encode itself as a byte array using `getBytes`. Another example is wherre you run a customer serializer to 
generate the byte array where you don't leak a reference to the array so it won't be mutated. 

A problem with using `getBytes` on a string as a key is that the platform string encoding might change if you move the 
file around. To avoid that there are a pair of methods that turn a string into an UTF8 encoded ByteSequence which is a 
compact form that can store any string: 

```java
    /*
     * This encodes a string into a fresh UTF8 byte array wrapped as a ByteString. Note that this copies data.
     */
    public static ByteSequence stringToUtf8(String string)

    /*
     * This decodes a UTF8 byte array wrapped in a ByteString into a string. Note that this copies data.
     */
    public static String utf8ToString(ByteSequence utf8)
```

Note that the comments state that the `byte[]` is a copy which is because `String` always copies data so that it is 
immutable. 

There is `java.util.Logging` at `Level.FINE` that shows the keys and size of the data that is being inserted, 
updated, or deleted. If you have a bug please try to create a repeatable test with fine logging enabled and post the 
logging. There is additional logging at `Level.FINEST` that shows every byte that is written to, or read from, the file. 

## Details

The original code was based on Derek Hamner's 1999 article [Use a RandomAccessFile to build a low-level database](http://www.javaworld.com/jw-01-1999/jw-01-step.html)
which shows how to creates a simple key value storage file. That code isn't safe to crashes due to the ordering 
of writes. This code base has tests that use a brute force search to throw exceptions on every file operation then 
validate the data on disk is always left in a consistent state. It also adds CRC32 checks to the data that are 
validated upon read from disk. **Note** If an IOException is thrown it does _not_ mean that the write is known to have 
failed. It means that the write _may_ have failed and that the in-memory state *might* be inconsistent with what is on 
disk. The way to fix to this is to close the store and open a fresh one to reload all the headers from disk. 

This implementation: 

1. Defaults to prioritising safety, over speed, over space. You can override some defaults if you are certain that you 
 read and write patterns let you. It is wise to use the defaults and only change them if you have tests that prove 
 safety and performance are not compromised. 
1. Supports a maximum key length of 247 bytes. The default is 64 bytes. 
1. Supports maximum file of byte length Long.MAX_VALUE.
1. Supports a maximum of Integer.MAX_VALUE entries.
1. The maximum size of keys is fixed for the life of the store and is written to the file upon creation. 
1. Uses a `HashMap` to index record headers by key. 
1. Uses a `TreeMap` to index record headers by the offset of the record data within the file. 
1. Uses a `ConcurrentSkipList` to record which records have free space sorted by the size of the free space.  
1. Has no dependencies outside of the JDK and uses `java.logging` aka JUL for logging.  
1. Is thread safe. It uses an internal lock to protected all public methods. 
1. Uses an in-memory HashMap to cache record headers by key. A record header is the key and compact metadata such as the  
offset, data and checksum. This makes locating a record by key is an `O(1)` lookup.
1. Stores the key with a single byte length header and a checksum footer. 
1. The records are held in a single `RandomAccessFile` comprising of: 
   1. A four byte header which is the number of records. 
   2. An index region which is all the headers with possibly some free space at the end. The index region can be 
   preallocated when the store is first created. Once the index is filled up fresh inserts will expand this region 
   by moving the records beyond it.  
   3. The data region. Past deletes or updates may have created free space between records. Record inserts or moves will 
   attempt to fill free space. If none is available the length of the file will be expand. 
1. An insert:
   1. May cause the index region to expand to fix the new header. This is done by moving the records to the end of the file. 
   1. May insert the record into any free space between existing records that is large enough. 
   1. Else inserts the record at the end of the file expanding as necessary.  
1. An update:
   1. May write in situ if the new record has the same length
   1. May write in situ if the new record is smaller and CRC32 checks are not disabled. Free space is created. 
   1. Will move the record if it has been expanded. The move creates free space at the old position.  
   1. May move to a big enough free space or will expand the file as necessary.    
   1. Any free space created by a move follows the same rules as for deletion below. 
1. A delete:
   1. May shrink the file if it is the last record. 
   1. Else move the second record backwards if it is the first record (issue [#12](https://github.com/simbo1905/simple-record-store/issues/12)). 
   1. Else will create some free space in the middle of the file which is up update to the header of the previous record. 
   1. Will overwrite the deleted header by moving the last header over it then decrementing the headers count creating 
   free space at the end of the index space.    
1. Record headers contain a CRC32 checksum which is checked when the data is loaded load. If you write zip data that has a 
built in CRC32 you can disable this in the constructor. Note that disabling CRC32 checks will prevent updates in situ when 
records shrink. In which case the update with less data will write to a free location creating.  
1. The order of writes to the records is designed so that if there is a crash there isn't any corruption. This is confirmed 
by the unit tests that for every functional test records every file operations. The test then performs a brute force 
replay crashing at every file operation and verifying the integrity of the data on disk after each crash. 

Given that the disk may be unavailable or full the 
and reloading all the headers is expensive this code throws an error and expects the application to log what is going on 
and decide how many times to attempt to reopen the store. 

Note that the source code using Lombok to be able to write cleaner and safer code. This is compile time only dependency. 

## Configuration

The file byte position is 64 bits so thousands of peta bytes. The data value size is 32 bits so a maximum of 2.14 G. 

You can set the following properties with either an environment variable or a -D flag. The -D flag takes precedence:

| Property                                                | Default | Comment                 |
|---------------------------------------------------------|---------|-------------------------|
| com.github.simbo1905.srs.BaseRecordStore.MAX_KEY_LENGTH | 64      | Max size of key string. |
| com.github.simbo1905.srs.BaseRecordStore.PAD_DATA_TO_KEY_LENGTH | true      | Pad data records to a minimum of RECORD_HEADER_LENGTH bytes. |

Note that RECORD_HEADER_LENGTH is MAX_KEY_LENGTH+RECORD_HEADER_LENGTH. If you have UUID string keys and set the max key 
size to 36 then each record header will be 68 characters. 

If you preallocate the store to be a size equal to or greater than the number of records you will store
you can skip PAD_DATA_TO_KEY_LENGTH. If you want to store small values that are rarely inserted then you 
can turn it off to safe space but be aware that expanding the size of the index area means a loop moving 
RECORD_HEADER_LENGTH worth of records to the back fo the file. 

## Build

Build and test the code with: 

```shell script
mvn package
```

Release: 

```shell script
# do a snaphost release
mvn clean deploy

# do a dry run of the real release
mvn release:prepare -DdryRun=true

# clean the dry run
mvn release:clean

# make the actual tag 
mvn release:prepare 

# checkout and deply the tag
mvn release:perform
```

## Alternatives

Some notable pure Java alternatives that don't compromise on crash saftey are:

* [MapDB](http://www.mapdb.org) "provides Java Maps, Sets, Lists, Queues and other collections backed by off-heap 
or on-disk storage. It is a hybrid between java collection framework and embedded database engine." 
* [Xodus](https://github.com/JetBrains/xodus) "JetBrains Xodus is a transactional schema-less embedded database used by JetBrains YouTrack and JetBrains Hub."
* [MVStore](https://www.h2database.com/html/mvstore.html) "The MVStore is a persistent, log structured key-value store. It is used as default storage subsystem of H2, but it can also be used directly within an application, without using JDBC or SQL." 

Their jar files are an order of magnitude bigger:

| Alternative  | Jar Size | Transient Deps |
| ------------- | ------------- | ------------- |
| simple-record-store 1.0.0-RC6  | 23 kB  | -  |
| mapdb 3.0.7  | 730 kB | 16 jars  |
| xodus-environment 1.3.124  | 502 kB | 3 jars  |
| h2-mvstore 1.4.200 | 301 kB | -  |

 They likely have a lot more than an order of magnitude more development effort put into them. As a result they are embedded database engines that work with variable length keys that don't fit in memory and that optimise the write path. 
