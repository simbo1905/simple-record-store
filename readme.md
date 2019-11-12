
# Simple Record Store

Simple Record Store is a persistent hash table with a predefined maximum key length. Records are written into a single 
file. The set of all fixed length record headers are held in a HashMap. This means that all your keys must fit into heap 
but all values are offloaded to disk. 

It is based on Derek Hamner's 1999 article [Use a RandomAccessFile to build a low-level database](http://www.javaworld.com/jw-01-1999/jw-01-step.html)
which shows how to creates a simple key value storage file. That code isn't safe to crashes due to the ordering 
of writes. This code base has tests that uses brute force search to throw exceptions on every file operation to validate 
the data on disk is consistent. It also adds CRC32 checks to the data that are validated upon read.  

This implementation: 

1. Supports a file of byte length Long.MAX_VALUE and a maximum of Integer.MAX_VALUE entries.
1. Records must have a unique key. The maximum size of keys is fixed for the life of the store.  
1. Uses an in-memory HashMap of the position of records and their length to speed up queries. 
1. The records are held in a single `RandomAccessFile` comprising of: 
   1. A four byte header which is the number of records. 
   2. An index region which is all the headers with possibly some free space at the end.
   3. The data region. Past deletes or updates may have created free space between records.  
1. An insert:
   1. May cause the index region to expand. This is done by moving the first records to the end of the file. 
   1. May insert the record into any free space between existing records that is large enough. 
   1. Else inserts the record at the end of the file expanding as necessary.  
1. An update:
   1. May write in situ if the new record has the same length
   1. May write in situ if the new record is smaller and CRC32 checks are not disabled. Free space is created. 
   1. Will move the record if it is now bigger. Moves cause new free space. 
   1. May move to a big enough free space or will expand the file and write to the end.    
   1. Any free space created by a move follows the same rules as for deletion below. 
1. A delete:
   1. May shrink the file if it is the last record. 
   1. Else move the second record backwards if it is the first record (issue [#12](https://github.com/simbo1905/simple-record-store/issues/12)). 
   1. Else will create some free space in the middle of the file which is a write to the header of the previous record. 
   1. Will overwrite the deleted header by moving the last header over it then decrementing the headers count.   
1. Record data sections are written with a  CRC32 checksum which is checked upon load. If you write zip data that has a 
built in CRC32 you can disable this in the constructor. Note that disabling CRC32 checks will prevent updates in situ. 
1. The order of writes to the records is designed so that if there is a crash there isn't any corruption. This is confirmed 
by unit tests that test a crash at every file operation. 

If any IOException is thrown that does not mean that the write is known to have failed. It means that the write may have 
failed and it is not known if state of the in memory map is consistent with that is on disk. The fix to this is to close 
the store and open a fresh one to reload all the headers from disk. Given that the disk may be unavailable or full the 
and reloading all the headers is expensive this code throws an error and expects the application to log what is going on 
and decide how many times to attempt to reopen the store. 

## Details

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


## Using

The latest release on maven central is:

```xml
<dependency>
	<groupId>com.github.trex-paxos</groupId>
	<artifactId>simple-record-store</artifactId>
	<version>0.9.2</version>
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




