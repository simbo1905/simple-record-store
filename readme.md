
#### Simple Record Store

Derek Hamner's 1999 article [Use a RandomAccessFile to build a low-level database](http://www.javaworld.com/jw-01-1999/jw-01-step.html)
shows how to creates a simple KV storage file. That code isn't safe to crashes though due to the ordering of writes. 
This version has a test that throws an exception on every write operation and verifies that the storage isn't corrupt
the write appears to be atomic when the header is flushed. 


## Build

Build and test the code with: 

	mvn package

## Details

You can set the following properties with either an environemnt variable or a -D flag. The -D flag takes precedence:

| Property                                                | Default | Comment                 |
|---------------------------------------------------------|---------|-------------------------|
| com.github.simbo1905.srs.BaseRecordStore.MAX_KEY_LENGTH | 64      | Max size of key string. |

The file byte position is 64 bits so 9223 petabytes. The value size is 32 bits so a maximum of 2.14 G. 

