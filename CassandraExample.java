package bigdatacourse.example;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;



/*
 * This class contains examples on basic Cassandra usage (Astra DB)
 */

public class CassandraExample {
	
	private static final String		TABLE_USER_VIEW = "user_view";
	
	private static final String		CQL_CREATE_TABLE = 
			"CREATE TABLE " + TABLE_USER_VIEW 	+"(" 		+ 
				"user_id bigint,"			+
				"ts timestamp,"				+
				"video_id bigint,"			+
				"device text,"				+
				"duration int,"				+
				"PRIMARY KEY ((user_id), ts, video_id)"	+
			") "						+
			"WITH CLUSTERING ORDER BY (ts DESC, video_id ASC)";
	
	private static final String		CQL_USER_VIEW_INSERT = 
			"INSERT INTO " + TABLE_USER_VIEW + "(user_id, ts, video_id, device, duration) VALUES(?, ?, ?, ?, ?)";

	private static final String		CQL_USER_VIEW_SELECT = 
			"SELECT * FROM " + TABLE_USER_VIEW + " WHERE user_id = ?";
	
	
	
	public static void main(String[] args) {
		
		test();
		
		System.out.println("Example complete, bye bye :)");
	}
	
	
	
	public static void test() {
		// creating the session
		CqlSession session = getCassandraSession(
			"/Users/boim/Downloads/secure-connect-hw2.zip",		// secure connect bundle path, change the path
			"clientID",											// found in the token file
			"clientSecret",										// found in the token file
			"bigdatacourse");
	
//		// creating the table
//		exampleCreateTable(session);
//
//		// creating the prepared statements
//		PreparedStatement pstmtAdd 		= 	session.prepare(CQL_USER_VIEW_INSERT);
//		PreparedStatement pstmtSelect 	= 	session.prepare(CQL_USER_VIEW_SELECT);
//	
//		// insert examples
//		long user_id 	= 	123;
//		long ts			=	System.currentTimeMillis();
//		long video_id	=	555444;
//		String device	=	"iPhone";
//		int duration	=	184;
//		
//		// insert #1
//		exampleInsertVer1(session, user_id, ts, video_id, device, duration);
//		System.out.println("exampleInsertVer1 - complete");
//		
//		// insert #1 string syntax problem (and query injection)
//		device = "iP'hone";
//		System.out.println("exampleInsertVer1 - BUG");
//		exampleInsertVer1(session, user_id, ts, video_id, device, duration);
//		
//		
//		// insert #2
//		device = "iP'hone";
//		exampleInsertVer2(session, user_id, ts, video_id, device, duration);
//		System.out.println("exampleInsertVer2 - complete");
//		
//		// insert #3
//		exampleInsertVer3(session, pstmtAdd, user_id, ts, video_id, device, duration, false);
//		System.out.println("exampleInsertVer3 - complete");
//		
//		// insert - speed comparison
//		exampleInsertSpeed(session, pstmtAdd, user_id, ts, video_id, device, duration);
//		System.out.println("exampleInsertSpeed - complete");
//
//		// insert - speed comparison (threads)
//		try {
//			exampleInsertSpeedThreads(session, pstmtAdd, user_id, ts, video_id, device, duration);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		System.out.println("exampleInsertSpeedThreads - complete");
//		
//		// insert - speed comparison (async)
//		exampleInsertSpeedAsync(session, pstmtAdd, user_id, ts, video_id, device, duration);
//		System.out.println("exampleInsertSpeedAsync - complete (NOTE - some may be dropped due to AstraDB limits");
//		
//		// select #1
//		exampleSelectVer1(session, user_id);
//		System.out.println("exampleSelectVer1 - complete");
//
//		// select #2
//		exampleSelectVer2(session, pstmtSelect, user_id);
//		System.out.println("exampleSelectVer2 - complete");

		// closing the session
		session.close();
	}
	
	
	
	public static CqlSession getCassandraSession(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		System.out.println("Initializing connection to Cassandra...");
		
		CqlSession session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
		return session;
	}
	
	
	public static void exampleCreateTable(CqlSession session) {
		session.execute(CQL_CREATE_TABLE);
		System.out.println("created table: " + TABLE_USER_VIEW);
	}
	
	
	public static void exampleInsertVer1(CqlSession session, long user_id, long ts, long video_id, String device, int duration) {
		// insert example #1 (do not use this version, just an example)
		session.execute("INSERT INTO " + TABLE_USER_VIEW + "(user_id, ts, video_id, device, duration) " + 
							"VALUES(" + user_id + "," + ts + "," + video_id + ",'" + device + "'," + duration +")");
	}

	public static void exampleInsertVer2(CqlSession session, long user_id, long ts, long video_id, String device, int duration) {
		// insert example #2 (use this for infrequent queries)
		session.execute(CQL_USER_VIEW_INSERT, user_id, ts, video_id, device, duration);
	}
	

	public static void exampleInsertVer3(CqlSession session, PreparedStatement pstmt, 
											long user_id, long ts, long video_id, String device, int duration, boolean async) {
		// insert example #3 (with prepared statement)
		// NOTE - BoundStatement are IMMUTABLE. The following code will not work
		// BoundStatement bstmt = pstmt.bind();
		// bstmt.setLong(x,x);
		// INSTEAD use a version like this example:
		BoundStatement bstmt = pstmt.bind()
			.setLong(0, 123)
			.setInstant(1, Instant.ofEpochMilli(ts))			// NOTE - for timestamps we need to use Java's Instant
			.setLong(2, video_id)
			.setString(3, device)
			.setInt(4, duration);
		
		if (async)
			session.executeAsync(bstmt);
		else
			session.execute(bstmt);
	}
	
	
	

	public static void exampleInsertSpeed(CqlSession session, PreparedStatement pstmt, 
											long user_id, long ts, long video_id, String device, int duration) {
		int count = 100;
		long startTS;
		
		// version 2
		System.out.println("version 2 - direct insert (sync)");
		startTS = System.currentTimeMillis();
		for (int i=0; i < count; i++) {
			exampleInsertVer2(session, user_id, ts, video_id, device, duration);
			System.out.println("version 2 - added " + i + "/" + count);
		}
		System.out.println("version2 - total time: " + (System.currentTimeMillis() - startTS));
			
		
		// version 3
		System.out.println("version 3 - direct insert (sync)");
		startTS = System.currentTimeMillis();
		for (int i=0; i < count; i++) {
			exampleInsertVer3(session, pstmt, user_id, ts, video_id, device, duration, false);
			System.out.println("version 3 - added " + i + "/" + count);
		}
		System.out.println("version3 - total time: " + (System.currentTimeMillis() - startTS));
		
		// NOTE - there is only slight different due to round trip query time. From local data center, the gap is bigger
	}
	
	

	public static void exampleInsertSpeedThreads(CqlSession session, PreparedStatement pstmt, 
											long user_id, long ts, long video_id, String device, int duration) throws InterruptedException {
		int count = 10000;
		int maxThreads	= 32;
		
		// creating the thread factors
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		
		// version 3
		System.out.println("version 3 - direct insert (sync)");
		long startTS = System.currentTimeMillis();
		for (int i=0; i < count; i++) {
			final int x = i;
			executor.execute(new Runnable() {
				@Override
				public void run() {
					exampleInsertVer3(session, pstmt, user_id, ts, video_id, device, duration, false);
					System.out.println("version 3 - added " + x + "/" + count);
				}
			});
		}
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		System.out.println("version3 - total time: " + (System.currentTimeMillis() - startTS));
	}
	
	

	public static void exampleInsertSpeedAsync(CqlSession session, PreparedStatement pstmt, 
											long user_id, long ts, long video_id, String device, int duration) {
		int count = 10000;
		
		// version 3
		System.out.println("version 3 - direct insert (async)");
		long startTS = System.currentTimeMillis();
		for (int i=0; i < count; i++) {
			exampleInsertVer3(session, pstmt, user_id, ts, video_id, device, duration, true);
			System.out.println("version 3 - added " + i + "/" + count);
		}
		System.out.println("version3 - total time: " + (System.currentTimeMillis() - startTS));
	}
	
	

	public static void exampleSelectVer1(CqlSession session, long user_id) {
		ResultSet rs = session.execute(CQL_USER_VIEW_SELECT, user_id);
		Row row = rs.one();
		int count = 0;
		while (row != null) {
			System.out.println(count + " -- " + row.getLong(0) + " -- " + row.getInstant(1) + " -- " + 
								row.getLong("video_id") + " -- " +  row.getString("device") + " -- " + row.getInt(4));
			row = rs.one();
			count++;
		}
	}
	

	public static void exampleSelectVer2(CqlSession session, PreparedStatement pstmt, long user_id) {
		BoundStatement bstmt = pstmt.bind().setLong(0, user_id);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		int count = 0;
		while (row != null) {
			System.out.println(count + " -- " + row.getLong(0) + " -- " + row.getInstant(1) + " -- " + 
								row.getLong("video_id") + " -- " +  row.getString("device") + " -- " + row.getInt(4));
			row = rs.one();
			count++;
		}
	}


}
