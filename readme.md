
#### Simple Record Store

Derek Hamner's 1999 article [Use a RandomAccessFile to build a low-level database](http://www.javaworld.com/jw-01-1999/jw-01-step.html)
shows how to creates a simple KV storage file. That code isn't safe to crashes though due to the ordering of writes. 
This version has a test that throws an exception on every write operation and verifies that the storage isn't corrupt
the write appears to be atomic when the header is flushed. 

This implementation: 

1. Supports a file of byte length Long.MAX_VALUE and a maximum of Integer.MAX_VALUE entries.
1. Records must have a unique fixed length key. The maximum size of keys is fixed for the life of the store.  
1. Uses an in-memory HashMap of the position of records and their length to speed up queries. The intention is to make 
it easier to override with faster in-memory structures. 
1. The records are held in a single `RandomAccessFile` comprising of: 
   1. A header which is the number of records. 
   2. An index region which is all the headers with possibly some free space at the end.
   3. The record region where past deletes and updates may have created free space between records.  
1. An insert:
   1. May cause the index region to expand. This is done by moving the first records to the end of the file. 
   1. May insert the record into any free space that is large enough. 
   1. Else inserts the record at the end of the file expanding as necessary.  
1. An update:
   1. May be the same size as the old records in which case it is a direct overwrite. 
   1. May be smaller than the old record in which case free space is created. 
   1. May be bigger than the old record. If so it is moved to to the back of the file expanding as necessary.
   1. Any free space created by a move follows the same rules as for deletion below. 
1. A delete may:
   1. May shrink the file if it is the last record. 
   1. Else move the second record backwards if it is the first record (issue [#12](https://github.com/simbo1905/simple-record-store/issues/12)). 
   1. Else will create some free space in the middle of the file which is a write to the header of the previous record. 
   1. Will overwrite the deleted header by moving the last header over it decrementing the headers count.   
1. Records are written with a CRC32 checksum which is checked upon load.  
1. The order of writes to the records is designed so that if there is a crash there isn't any corruption. 

## Using

The latest release on maven central is:

```xml
<dependency>
	<groupId>com.github.trex-paxos</groupId>
	<artifactId>simple-record-store</artifactId>
	<version>0.6.0</version>
</dependency>
```

See `SimpleRecordStoreApiTests.java` for examples of the public API which is minimal. 

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

## Details

You can set the following properties with either an environemnt variable or a -D flag. The -D flag takes precedence:

| Property                                                | Default | Comment                 |
|---------------------------------------------------------|---------|-------------------------|
| com.github.simbo1905.srs.BaseRecordStore.MAX_KEY_LENGTH | 64      | Max size of key string. |

The file byte position is 64 bits so 9223 petabytes. The value size is 32 bits so a maximum of 2.14 G. 

