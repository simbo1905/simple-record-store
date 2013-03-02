package com.github.simbo1905.srs;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that the simple random access storage 'db' works and does not get 
 * corrupted under write errors. 
 */
public class SimpleRecordsStoreTests {
	
	/**
	 * A utility to recored how many times file write operations are called 
	 * and what the stack looks like for them.
	 */
	private static final class StackCollectingWriteCallback implements
			WriteCallback {
		private final List<List<String>> writeStacks;

		private StackCollectingWriteCallback(List<List<String>> writeStacks) {
			this.writeStacks = writeStacks;
		}

		@Override
		public void onWrite() {
			List<String> stack = new ArrayList<String>();
			writeStacks.add(stack);
			StackTraceElement[] st = Thread.currentThread().getStackTrace();
			for( int index = 2; index < st.length; index++){
				String s = st[index].toString();
				stack.add(s);
				if( !s.contains("com.github.simbo1905")) {
					break;
				}
			}
		}
	}

	/**
	 * A untility to throw an exception at a particular write operation. 
	 */
	private static final class CrashAtWriteCallback implements WriteCallback {
		final int crashAtIndex;
		int calls = 0;

		private CrashAtWriteCallback(int index) {
			crashAtIndex = index;
		}

		@Override
		public void onWrite() throws IOException {
			if( crashAtIndex == calls++){
				throw new IOException("simulated write error at call index: "+crashAtIndex);
			}
		}

	}

	static final String TMP = System.getProperty("java.io.tmpdir");

	private final static Logger LOGGER = Logger.getLogger(SimpleRecordsStoreTests.class.getName()); 
	
	String fileName;
	int initialSize;
	
	public SimpleRecordsStoreTests() {
		LOGGER.setLevel(Level.ALL);
		init(TMP+"junit.records",0);
	}
	
	public void init(final String fileName, final int initialSize) {
		this.fileName = fileName;
		this.initialSize = initialSize;		
		File db = new File(this.fileName);
		if( db.exists() ){
			db.delete();
		} 
		db.deleteOnExit();
	}
	
	BaseRecordStore recordsFile = null;
	
	@After
	public void deleteDb() throws Exception {
		File db = new File(this.fileName);
		if( db.exists() ){
			db.delete();
		} 
	}

	/**
	 * Taken from http://www.javaworld.com/jw-01-1999/jw-01-step.html
	 */
	@Test
	public void originalTest() throws Exception {
		recordsFile = new FileRecordStore(fileName, initialSize);
		
		LOGGER.info("creating records file...");

		LOGGER.info("adding a record...");
		RecordWriter rw = new RecordWriter("foo.lastAccessTime");
		rw.writeObject(new Date());
		recordsFile.insertRecord(rw);

		LOGGER.info("reading record...");
		RecordReader rr = recordsFile.readRecord("foo.lastAccessTime");
		Date d = (Date) rr.readObject();
		System.out.println("\tlast access was at: " + d.toString());

		LOGGER.info("updating record...");
		rw = new RecordWriter("foo.lastAccessTime");
		rw.writeObject(new Date());
		recordsFile.updateRecord(rw);

		LOGGER.info("reading record...");
		rr = recordsFile.readRecord("foo.lastAccessTime");
		d = (Date) rr.readObject();
		System.out.println("\tlast access was at: " + d.toString());

		LOGGER.info("deleting record...");
		recordsFile.deleteRecord("foo.lastAccessTime");
		if (recordsFile.recordExists("foo.lastAccessTime")) {
			throw new Exception("Record not deleted");
		} else {
			LOGGER.info("record successfully deleted.");
		}

		LOGGER.info("test completed.");
	}
	
	@Test
	public void testInsertOneRecord() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		List<UUID> uuids = createUuid(1);
		Object uuid = uuids.get(0);
		RecordWriter rw = new RecordWriter(uuid.toString());
		rw.writeObject(uuids.get(0));
		
		// when
		this.recordsFile.insertRecord(rw);
		RecordReader record = this.recordsFile.readRecord(uuid.toString());
		
		// then
		Assert.assertThat((UUID)record.readObject(), is(uuids.get(0)));
	}
	
	@Test
	public void testInsertOneRecordWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(1);
		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {

					LOGGER.info(String.format("writing to: "+fileName));
					
					// given
					recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
					
					Object uuid = uuids.get(0);
					RecordWriter rw = new RecordWriter(uuid.toString());
					rw.writeObject(uuid);
					
					// when
					recordsFile.insertRecord(rw);
				}			
		}, uuids);
	}
	
	@Test
	public void testInsertTwoRecords() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		List<UUID> uuids = createUuid(2);
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		Object uuid1 = uuids.get(1);
		RecordWriter rw1 = new RecordWriter(uuid1.toString());
		rw1.writeObject(uuid1);

		// when
		this.recordsFile.insertRecord(rw0);
		this.recordsFile.insertRecord(rw1);
		RecordReader rr = this.recordsFile.readRecord(uuid0.toString());
		RecordReader rr1 = this.recordsFile.readRecord(uuid1.toString());
		
		// then
		Assert.assertThat((UUID)rr.readObject(), is(uuid0));
		Assert.assertThat((UUID)rr1.readObject(), is(uuid1));
	}
	
	@Test
	public void testInsertTwoRecordsWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(2);
		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
					// given
					recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
					Object uuid0 = uuids.get(0);
					RecordWriter rw0 = new RecordWriter(uuid0.toString());
					rw0.writeObject(uuid0);
					Object uuid1 = uuids.get(1);
					RecordWriter rw1 = new RecordWriter(uuid1.toString());
					rw1.writeObject(uuid1);

					// when
					recordsFile.insertRecord(rw0);
					recordsFile.insertRecord(rw1);
				}			
		}, uuids);
	}

	@Test
	public void testInsertThenDeleteRecord() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		List<UUID> uuids = createUuid(1);
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);

		// when
		this.recordsFile.insertRecord(rw0);
		this.recordsFile.deleteRecord(uuid0.toString());
		try { 
			this.recordsFile.readRecord(uuid0.toString());
			Assert.fail();
		} catch( RecordsFileException e){
			// expected
		}
	}
	
	@Test
	public void testInsertThenDeleteRecordWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(1);

		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
					// given
					recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
					Object uuid = uuids.get(0);
					RecordWriter rw = new RecordWriter(uuid.toString());
					rw.writeObject(uuid);
					
					// when
					recordsFile.insertRecord(rw);
					recordsFile.deleteRecord(uuid.toString());
				}			
		}, uuids);
	}
	
	@Test
	public void testInsertTwoThenDeleteTwoRecords() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		List<UUID> uuids = createUuid(2);
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		Object uuid1 = uuids.get(1);
		RecordWriter rw1 = new RecordWriter(uuid1.toString());
		rw1.writeObject(uuid1);

		// when
		this.recordsFile.insertRecord(rw0);
		this.recordsFile.insertRecord(rw1);
		this.recordsFile.deleteRecord(uuid0.toString());
		this.recordsFile.deleteRecord(uuid1.toString());
		try { 
			this.recordsFile.readRecord(uuid0.toString());
			Assert.fail();
		} catch( RecordsFileException e){
			// expected
		}
		try { 
			this.recordsFile.readRecord(uuid1.toString());
			Assert.fail();
		} catch( RecordsFileException e){
			// expected
		}
	}
	
	@Test
	public void testInsertTwoThenDeleteTwoRecordsWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(2);

		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				// given
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				Object uuid0 = uuids.get(0);
				RecordWriter rw0 = new RecordWriter(uuid0.toString());
				rw0.writeObject(uuid0);
				Object uuid1 = uuids.get(1);
				RecordWriter rw1 = new RecordWriter(uuid1.toString());
				rw1.writeObject(uuid1);

				// when
				recordsFile.insertRecord(rw0);
				recordsFile.insertRecord(rw1);
				recordsFile.deleteRecord(uuid0.toString());
				recordsFile.deleteRecord(uuid1.toString());
			}			
		}, uuids);
	}
	
	@Test
	public void testInsertTwoDeleteFirstInsertOne() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		List<UUID> uuids = createUuid(3);
		
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		
		Object uuid1 = uuids.get(1);
		RecordWriter rw1 = new RecordWriter(uuid1.toString());
		rw1.writeObject(uuid1);

		Object uuid2 = uuids.get(2);
		RecordWriter rw2 = new RecordWriter(uuid2.toString());
		rw2.writeObject(uuid2);

		// when
		recordsFile.insertRecord(rw0);
		recordsFile.insertRecord(rw1);
		recordsFile.deleteRecord(uuid0.toString()); // actually some re-ording on second insert so this is end of file
		recordsFile.insertRecord(rw2);
		RecordReader rr1 = recordsFile.readRecord(uuid1.toString());
		RecordReader rr2 = recordsFile.readRecord(uuid2.toString());
		
		// then
		Assert.assertThat((UUID)rr1.readObject(), is(uuid1));
		Assert.assertThat((UUID)rr2.readObject(), is(uuid2));
		try { 
			recordsFile.readRecord(uuid0.toString());
			Assert.fail();
		} catch( RecordsFileException e){
			// expected
		}
	}
	
	@Test
	public void testInsertTwoDeleteFirstInsertOneWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(3);

		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				// given
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				Object uuid0 = uuids.get(0);
				RecordWriter rw0 = new RecordWriter(uuid0.toString());
				rw0.writeObject(uuid0);
				
				Object uuid1 = uuids.get(1);
				RecordWriter rw1 = new RecordWriter(uuid1.toString());
				rw1.writeObject(uuid1);

				Object uuid2 = uuids.get(2);
				RecordWriter rw2 = new RecordWriter(uuid2.toString());
				rw2.writeObject(uuid2);

				// when
				recordsFile.insertRecord(rw0);
				recordsFile.insertRecord(rw1);
				recordsFile.deleteRecord(uuid1.toString()); // actually some re-ordering on second insert so this is end of file
				recordsFile.insertRecord(rw2);
				RecordReader rr = recordsFile.readRecord(uuid0.toString());
				rr.readObject();
				RecordReader rr2 = recordsFile.readRecord(uuid2.toString());
				rr2.readObject();
			}			
		}, uuids);	
	}
	
	@Test
	public void testInsertTwoDeleteSecondInsertOne() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		List<UUID> uuids = createUuid(3);
		
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		
		Object uuid1 = uuids.get(1);
		RecordWriter rw1 = new RecordWriter(uuid1.toString());
		rw1.writeObject(uuid1);

		Object uuid2 = uuids.get(2);
		RecordWriter rw2 = new RecordWriter(uuid2.toString());
		rw2.writeObject(uuid2);

		// when
		recordsFile.insertRecord(rw0);
		recordsFile.insertRecord(rw1);
		recordsFile.deleteRecord(uuid1.toString()); // actually some re-ordering on second insert so this earlier in file 
		recordsFile.insertRecord(rw2);
		RecordReader rr = recordsFile.readRecord(uuid0.toString());
		RecordReader rr2 = recordsFile.readRecord(uuid2.toString());
		
		// then
		Assert.assertThat((UUID)rr.readObject(), is(uuid0));
		Assert.assertThat((UUID)rr2.readObject(), is(uuid2));
		try { 
			recordsFile.readRecord(uuid1.toString());
			Assert.fail();
		} catch( RecordsFileException e){
			// expected
		}
	}
	
	@Test
	public void testInsertTwoDeleteSecondInsertOneWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(3);

		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				// given
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				
				Object uuid0 = uuids.get(0);
				RecordWriter rw0 = new RecordWriter(uuid0.toString());
				rw0.writeObject(uuid0);
				
				Object uuid1 = uuids.get(1);
				RecordWriter rw1 = new RecordWriter(uuid1.toString());
				rw1.writeObject(uuid1);

				Object uuid2 = uuids.get(2);
				RecordWriter rw2 = new RecordWriter(uuid2.toString());
				rw2.writeObject(uuid2);

				// when
				recordsFile.insertRecord(rw0);
				recordsFile.insertRecord(rw1);
				recordsFile.deleteRecord(uuid1.toString());// actually some re-ordering on second insert so this earlier in file 
				recordsFile.insertRecord(rw2);
				RecordReader rr = recordsFile.readRecord(uuid0.toString());
				rr.readObject();
				RecordReader rr2 = recordsFile.readRecord(uuid2.toString());
				rr2.readObject();
			}			
		}, uuids);
	}
	
	@Test
	public void testInsertThreeDeleteSecondInsertOne() throws Exception {
		// given
		List<UUID> uuids = createUuid(4);
		recordsFile = new FileRecordStore(fileName, initialSize);
		
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		
		Object uuid1 = uuids.get(1);
		RecordWriter rw1 = new RecordWriter(uuid1.toString());
		rw1.writeObject(uuid1);

		Object uuid2 = uuids.get(2);
		RecordWriter rw2 = new RecordWriter(uuid2.toString());
		rw2.writeObject(uuid2);

		Object uuid3 = uuids.get(3);
		RecordWriter rw3 = new RecordWriter(uuid3.toString());
		rw3.writeObject(uuid3);
		
		// when
		@SuppressWarnings("unused")
		RecordHeader rh0 = recordsFile.insertRecord0(rw0);
		@SuppressWarnings("unused")
		RecordHeader rh1 = recordsFile.insertRecord0(rw1);
		@SuppressWarnings("unused")
		RecordHeader rh2 = recordsFile.insertRecord0(rw2);
		recordsFile.deleteRecord(uuid0.toString()); // first is shifted to end to expand index and end up as middle in data section
		recordsFile.insertRecord(rw3);

		RecordReader rr0 = recordsFile.readRecord(uuid1.toString());
		RecordReader rr2 = recordsFile.readRecord(uuid2.toString());
		RecordReader rr3 = recordsFile.readRecord(uuid3.toString());
		
		// then
		Assert.assertThat((UUID)rr0.readObject(), is(uuid1));
		Assert.assertThat((UUID)rr2.readObject(), is(uuid2));
		Assert.assertThat((UUID)rr3.readObject(), is(uuid3));
		try { 
			recordsFile.readRecord(uuid0.toString());
			Assert.fail();
		} catch( RecordsFileException e){
			// expected
		}
	}
	
	@Test
	public void testInsertThreeDeleteSecondInsertOneWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(4);

		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				
				Object uuid0 = uuids.get(0);
				RecordWriter rw0 = new RecordWriter(uuid0.toString());
				rw0.writeObject(uuid0);
				
				Object uuid1 = uuids.get(1);
				RecordWriter rw1 = new RecordWriter(uuid1.toString());
				rw1.writeObject(uuid1);

				Object uuid2 = uuids.get(2);
				RecordWriter rw2 = new RecordWriter(uuid2.toString());
				rw2.writeObject(uuid2);

				Object uuid3 = uuids.get(3);
				RecordWriter rw3 = new RecordWriter(uuid3.toString());
				rw3.writeObject(uuid3);
				
				// when
				recordsFile.insertRecord(rw0);
				recordsFile.insertRecord(rw1);
				recordsFile.insertRecord(rw2);
				recordsFile.deleteRecord(uuid0.toString()); // first is shifted to end to expand index and end up as middle in data section
				recordsFile.insertRecord(rw3);

				RecordReader rr0 = recordsFile.readRecord(uuid1.toString());
				rr0.readObject();
				RecordReader rr2 = recordsFile.readRecord(uuid2.toString());
				rr2.readObject();
				RecordReader rr3 = recordsFile.readRecord(uuid3.toString());
				rr3.readObject();
			}			
		}, uuids);

	}
	
	@Test
	public void testUpdateOneRecord() throws Exception {
		// given
		List<UUID> uuids = createUuid(2);
		recordsFile = new FileRecordStore(fileName, initialSize);
		
		Object uuid0 = uuids.get(0);
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		Object uuidUpdated = uuids.get(1);
		
		// when
		recordsFile.insertRecord(rw0);
		rw0.clear();
		rw0.writeObject(uuidUpdated);
		recordsFile.updateRecord(rw0);
		
		RecordReader rr0 = recordsFile.readRecord(uuid0.toString());
		
		// then
		Assert.assertThat((UUID)rr0.readObject(), is(uuidUpdated));
	}
	
	@Test
	public void testUpdateOneRecordWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(2);

		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				
				Object uuid0 = uuids.get(0);
				RecordWriter rw0 = new RecordWriter(uuid0.toString());
				rw0.writeObject(uuid0);
				Object uuidUpdated = uuids.get(1);
				
				// when
				recordsFile.insertRecord(rw0);
				rw0.clear();
				rw0.writeObject(uuidUpdated);
				recordsFile.updateRecord(rw0);
				
				RecordReader rr0 = recordsFile.readRecord(uuid0.toString());
				rr0.readObject();
			}			
		}, uuids);
	}

	@Test
	public void testUpdateExpandOneRecord() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		
		UUID uuid0 = UUID.randomUUID();
		UUID uuid1 = UUID.randomUUID();
		UUID uuid2 = UUID.randomUUID();
		String oldPayload = uuid1.toString();
		String newPayload = uuid1.toString()+uuid2.toString();
		
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(oldPayload);
		
		// when
		recordsFile.insertRecord(rw0);
		rw0.clear();
		rw0.writeObject(newPayload);
		recordsFile.updateRecord(rw0);
		
		RecordReader rr0 = recordsFile.readRecord(uuid0.toString());
		
		// then
		Assert.assertThat((String)rr0.readObject(), is(newPayload));
	}
	
	@Test
	public void testUpdateExpandOneRecordWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(3);
		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				
				UUID uuid0 = UUID.randomUUID();
				UUID uuid1 = UUID.randomUUID();
				UUID uuid2 = UUID.randomUUID();
				String oldPayload = uuid1.toString();
				String newPayload = uuid1.toString()+uuid2.toString();
				
				RecordWriter rw0 = new RecordWriter(uuid0.toString());
				rw0.writeObject(oldPayload);
				
				// when
				recordsFile.insertRecord(rw0);
				rw0.clear();
				rw0.writeObject(newPayload);
				recordsFile.updateRecord(rw0);
				
				RecordReader rr0 = recordsFile.readRecord(uuid0.toString());
				
				rr0.readObject();
			}			
		}, uuids);
	}
	
	@Test
	public void testUpdateShrinkOneRecord() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		
		UUID uuid0 = UUID.randomUUID();
		UUID uuid1 = UUID.randomUUID();
		UUID uuid2 = UUID.randomUUID();
		
		RecordWriter rw0 = new RecordWriter(uuid0.toString());
		rw0.writeObject(uuid0);
		rw0.writeObject(uuid1);
		
		// when
		recordsFile.insertRecord(rw0);
		rw0.clear();
		rw0.writeObject(uuid2);
		recordsFile.updateRecord(rw0);
		
		RecordReader rr0 = recordsFile.readRecord(uuid0.toString());
		
		// then
		Assert.assertThat((UUID)rr0.readObject(), is(uuid2));
	}
	
	@Test
	public void testUpdateShrinkOneRecordWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(3);
		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				UUID uuid0 = uuids.get(0);
				UUID uuid1 = uuids.get(1);
				UUID uuid2 = uuids.get(2);
				
				RecordWriter rw0 = new RecordWriter(uuid0.toString());
				rw0.writeObject(uuid0);
				rw0.writeObject(uuid1);
				
				// when
				recordsFile.insertRecord(rw0);
				rw0.clear();
				rw0.writeObject(uuid2);
				recordsFile.updateRecord(rw0);
				
				RecordReader rr0 = recordsFile.readRecord(uuid0.toString());
				rr0.readObject();
			}			
		}, uuids);
	}
	
	@Test
	public void testDeleteFirstEntry() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		
		String smallEntry = UUID.randomUUID().toString();
		String largeEntry = UUID.randomUUID().toString()+UUID.randomUUID().toString()+UUID.randomUUID().toString();
		
		RecordWriter smallWriter1 = new RecordWriter("small");
		smallWriter1.writeObject(smallEntry);
		RecordWriter smallWriter2 = new RecordWriter("small2");
		smallWriter2.writeObject(smallEntry);
		RecordWriter largeWriter = new RecordWriter("large");
		largeWriter.writeObject(largeEntry);
		
		// when
		recordsFile.insertRecord(smallWriter1);
		recordsFile.insertRecord(smallWriter2); // expansion reorders first couple of entries so try three
		recordsFile.insertRecord(largeWriter);
		recordsFile.deleteRecord("small");
		recordsFile.deleteRecord("small2");
		String large = (String) recordsFile.readRecord("large").readObject();
		
		Assert.assertThat(large, is(largeEntry));
	}
	
	@Test
	public void testDeleteFirstEntryWithIOExceptions() throws Exception {
		List<UUID> uuids = createUuid(4);
		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				String smallEntry = uuids.get(0).toString();
				String largeEntry = uuids.get(1).toString()+uuids.get(1).toString()+uuids.get(3).toString();
				RecordWriter smallWriter1 = new RecordWriter("small");
				smallWriter1.writeObject(smallEntry);
				RecordWriter smallWriter2 = new RecordWriter("small2");
				smallWriter2.writeObject(smallEntry);
				RecordWriter largeWriter = new RecordWriter("large");
				largeWriter.writeObject(largeEntry);
				
				// when
				recordsFile.insertRecord(smallWriter1);
				recordsFile.insertRecord(smallWriter2); // expansion reorders first couple of entries so try three
				recordsFile.insertRecord(largeWriter);
				recordsFile.deleteRecord("small");
				recordsFile.deleteRecord("small2");
				String large = (String) recordsFile.readRecord("large").readObject();
				Assert.assertThat(large, is(largeEntry));
			}
		}, uuids);
	}
	
	@Test
	public void testBulkInsert() throws Exception {
		// given
		recordsFile = new FileRecordStore(fileName, initialSize);
		
		String smallEntry = UUID.randomUUID().toString();
		String largeEntry = UUID.randomUUID().toString()+UUID.randomUUID().toString()+UUID.randomUUID().toString();
		
		RecordWriter smallWriter1 = new RecordWriter("small");
		smallWriter1.writeObject(smallEntry);
		RecordWriter smallWriter2 = new RecordWriter("small2");
		smallWriter2.writeObject(smallEntry);
		RecordWriter largeWriter = new RecordWriter("large");
		largeWriter.writeObject(largeEntry);
		
		//recordsFile.insertRecord(smallWriter1);
		
		// when
		recordsFile.insertRecords(smallWriter1,smallWriter2,largeWriter);
		String small = (String) recordsFile.readRecord("small").readObject();
		String small2 = (String) recordsFile.readRecord("small2").readObject();
		String large = (String) recordsFile.readRecord("large").readObject();
		
		// then
		
		Assert.assertThat(small, is(smallEntry));
		Assert.assertThat(small2, is(smallEntry));
		Assert.assertThat(large, is(largeEntry));
		
		RecordHeader smallHeader = recordsFile.keyToRecordHeader("small");
		RecordHeader small2Header = recordsFile.keyToRecordHeader("small2");
		RecordHeader largeHeader = recordsFile.keyToRecordHeader("large");
		Assert.assertThat( smallHeader.dataPointer, lessThan(small2Header.dataPointer));		
		Assert.assertThat( small2Header.dataPointer, lessThan(largeHeader.dataPointer));		
	}
	
	@Test
	public void testBulkInsertWithIOExceptions() throws Exception {
		verifyWorkWithIOExceptions(new InterceptedTestOperations() {
			@Override
			public void performTestOperations(WriteCallback wc, String fileName,
					List<UUID> uuids) throws Exception {
				recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc);
				
				String smallEntry = UUID.randomUUID().toString();
				String largeEntry = UUID.randomUUID().toString()+UUID.randomUUID().toString()+UUID.randomUUID().toString();
				
				RecordWriter smallWriter1 = new RecordWriter("small");
				smallWriter1.writeObject(smallEntry);
				RecordWriter smallWriter2 = new RecordWriter("small2");
				smallWriter2.writeObject(smallEntry);
				RecordWriter largeWriter = new RecordWriter("large");
				largeWriter.writeObject(largeEntry);
				
				//recordsFile.insertRecord(smallWriter1);
				
				// when
				recordsFile.insertRecords(smallWriter1,smallWriter2,largeWriter);
				recordsFile.readRecord("small").readObject();
				recordsFile.readRecord("small2").readObject();
				recordsFile.readRecord("large").readObject();
			}
		}, null);
	}
	
	private void removeFiles(List<String> localFileNames) {
		for( String file : localFileNames ){
			File f = new File(file);
			f.delete();
		}
	}

	private String stackToString(List<String> stack) {
		StringBuilder sb = new StringBuilder();
		for( String s : stack ){
			sb.append("\\n\\t");
			sb.append(s);
		}
		return sb.toString();
	}

	private String fileName(String base) {
		String fileName = TMP+base;
		File file = new File(fileName);
		file.deleteOnExit();
		return fileName;
	}

	static interface InterceptedTestOperations {
		void performTestOperations(WriteCallback wc, String fileName, List<UUID> uuids) throws Exception;
	}
	
	
	private List<UUID> createUuid(int count) {
		List<UUID> uuids = new ArrayList<UUID>(count);
		for( int index = 0; index < count; index++ ) {
			uuids.add(UUID.randomUUID());
		}
		return uuids;
	}

	void verifyWorkWithIOExceptions(InterceptedTestOperations interceptedOperations, List<UUID> uuids) throws Exception {
		final List<List<String>> writeStacks = new ArrayList<List<String>>();
		
		WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

		final List<String> localFileNames = new ArrayList<String>();
		final String recordingFile = fileName("record");
		localFileNames.add(recordingFile);
		interceptedOperations.performTestOperations(collectsWriteStacks, recordingFile,uuids);
		
		for(int index = 0; index < writeStacks.size(); index++){
			final List<String> stack = writeStacks.get(index);
			final CrashAtWriteCallback crashAt = new CrashAtWriteCallback(index);
			final String localFileName = fileName("crash"+index);
			localFileNames.add(localFileName);
			try { 
				interceptedOperations.performTestOperations(crashAt, localFileName,uuids);
			} catch( Exception ioe ) {
				try {
					BaseRecordStore possiblyCorruptedFile = new FileRecordStore(localFileName, "r");
					int count = possiblyCorruptedFile.getNumRecords();
					for( String k : possiblyCorruptedFile.keys() ){
						RecordReader r = possiblyCorruptedFile.readRecord(k);
						Object o = r.readObject();
						assertNotNull(o);
						count--;
					}
					assertThat(count, is(0));
					removeFiles(localFileNames);
				} catch (Exception e ){
					removeFiles(localFileNames);
					final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
					throw new RuntimeException(msg,e);
				}
			}
		}
		removeFiles(localFileNames);		
	}

}
