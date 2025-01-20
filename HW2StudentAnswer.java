package bigdatacourse.hw2.studentcode;

import bigdatacourse.hw2.HW2API;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HW2StudentAnswer implements HW2API {

    // -------------------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------------------
    public static final String NOT_AVAILABLE_VALUE = "na";

    private static final String TABLE_ITEMS           = "items";
    private static final String TABLE_REVIEWS_BY_ID   = "reviews_by_id";
    private static final String TABLE_REVIEWS_BY_ITEM = "reviews_by_item";

    // CREATE TABLE statements:
    
    private static final String CQL_CREATE_TABLES =
        "CREATE TABLE IF NOT EXISTS " + TABLE_ITEMS + " ("
      + "asin text,"
      + "title text,"
      + "imageURL text,"
      + "categories set<text>,"
      + "description text,"
      + "PRIMARY KEY (asin)"
      + "); "

      + "CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_ID + " ("
      + "reviewerId text,"
      + "time timestamp,"
      + "asin text,"
      + "reviewerName text,"
      + "rating int,"
      + "summary text,"
      + "reviewText text,"
      + "PRIMARY KEY ((reviewerId), time, asin)"
      + ") WITH CLUSTERING ORDER BY (time DESC, asin ASC); "

      + "CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_ITEM + " ("
      + "asin text,"
      + "time timestamp,"
      + "reviewerId text,"
      + "reviewerName text,"
      + "rating int,"
      + "summary text,"
      + "reviewText text,"
      + "PRIMARY KEY ((asin), time, reviewerId)"
      + ") WITH CLUSTERING ORDER BY (time DESC, reviewerId ASC);";

    // -------------------------------------------------------------------------------------
    // CQL Statements
    // -------------------------------------------------------------------------------------

    private static final String CQL_REVIEW_INSERT_BATCH =
        "BEGIN BATCH "
      + "INSERT INTO " + TABLE_REVIEWS_BY_ID
      + "(reviewerId, time, asin, reviewerName, rating, summary, reviewText) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?); "
      + "INSERT INTO " + TABLE_REVIEWS_BY_ITEM
      + "(asin, time, reviewerId, reviewerName, rating, summary, reviewText) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?); "
      + "APPLY BATCH";

    private static final String CQL_ITEM_INSERT =
        "INSERT INTO " + TABLE_ITEMS
      + "(asin, title, imageUrl, categories, description) "
      + "VALUES(?, ?, ?, ?, ?)";

    // -------------------------------------------------------------------------------------
    // Select statements:
    // -------------------------------------------------------------------------------------
    private static final String CQL_ITEM_SELECT =
        "SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";

    private static final String CQL_REVIEWS_BY_ID_SELECT =
        "SELECT * FROM " + TABLE_REVIEWS_BY_ID + " WHERE reviewerId = ?";

    private static final String CQL_REVIEWS_BY_ITEM_SELECT =
        "SELECT * FROM " + TABLE_REVIEWS_BY_ITEM + " WHERE asin = ?";

    // -------------------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------------------
    private CqlSession session;

    private PreparedStatement pstmtAddItem;
    private PreparedStatement pstmtAddReviewBatch;
    private PreparedStatement pstmtSelectItem;
    private PreparedStatement pstmtSelectReviewById;
    private PreparedStatement pstmtSelectReviewByItem;

    // -------------------------------------------------------------------------------------
    // HW2API implementation
    // -------------------------------------------------------------------------------------

    @Override
    public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
        if (session != null) {
            System.out.println("ERROR - cassandra is already connected");
            return;
        }
        System.out.println("Initializing connection to Cassandra...");

        this.session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
                .withAuthCredentials(username, password)
                .withKeyspace(keyspace)
                .build();

        System.out.println("Initializing connection to Cassandra... Done");
    }

    @Override
    public void close() {
        if (session == null) {
            System.out.println("Cassandra connection is already closed");
            return;
        }
        System.out.println("Closing Cassandra connection...");
        session.close();
        session = null;
        System.out.println("Closing Cassandra connection... Done");
    }

    @Override
    public void createTables() {
        // Execute the multi-statement CQL to create all tables
        for (String statement : CQL_CREATE_TABLES.split(";")) {
            String trimmed = statement.trim();
            if (!trimmed.isEmpty()) {
                session.execute(trimmed);
            }
        }
        System.out.println("Created tables: " + TABLE_ITEMS + ", " + TABLE_REVIEWS_BY_ID + " and " + TABLE_REVIEWS_BY_ITEM);
    }

    @Override
    public void initialize() {
        // Prepare all statements for later use
        this.pstmtAddItem            = session.prepare(CQL_ITEM_INSERT);
        this.pstmtAddReviewBatch     = session.prepare(CQL_REVIEW_INSERT_BATCH);
        this.pstmtSelectItem         = session.prepare(CQL_ITEM_SELECT);
        this.pstmtSelectReviewById   = session.prepare(CQL_REVIEWS_BY_ID_SELECT);
        this.pstmtSelectReviewByItem = session.prepare(CQL_REVIEWS_BY_ITEM_SELECT);

        System.out.println("Prepared Statements have been initialized!");
    }

    /**
     * Load items from a file where each line is a single JSON object (using single quotes).
     */
    @Override
    public void loadItems(String pathItemsFile) throws Exception {
        System.out.println("Loading items from file (one JSON object per line): " + pathItemsFile);

        // Configure Jackson to allow single quotes
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        ExecutorService executor = Executors.newFixedThreadPool(250);
        AtomicInteger count = new AtomicInteger(0);

        try (BufferedReader br = Files.newBufferedReader(Paths.get(pathItemsFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                // create a final (or effectively final) variable
                final String currentLine = line.trim();
                if (currentLine.isEmpty()) {
                    continue;
                }

                executor.submit(() -> {
                    try {
                        // Parse as a Map
                        Map<String, Object> itemMap = mapper.readValue(
                            currentLine,
                            new TypeReference<Map<String, Object>>() {}
                        );

                        // Extract fields
                        String asin        = getStringValue(itemMap, "asin",        NOT_AVAILABLE_VALUE);
                        String title       = getStringValue(itemMap, "title",       NOT_AVAILABLE_VALUE);
                        String imageUrl    = getStringValue(itemMap, "imageURL",    NOT_AVAILABLE_VALUE);
                        if (NOT_AVAILABLE_VALUE.equals(imageUrl)) {
                            imageUrl = getStringValue(itemMap, "imUrl", NOT_AVAILABLE_VALUE);
                        }
                        String description = getStringValue(itemMap, "description", NOT_AVAILABLE_VALUE);

                        // Build categories set
                        Set<String> categories = new HashSet<>();
                        Object catObj = itemMap.get("categories");
                        if (catObj instanceof List) {
                            for (Object sub : (List<?>) catObj) {
                                if (sub instanceof List) {
                                    for (Object c : (List<?>) sub) {
                                        categories.add(c == null ? NOT_AVAILABLE_VALUE : c.toString());
                                    }
                                } else if (sub != null) {
                                    categories.add(sub.toString());
                                }
                            }
                        }

                        // Insert into Cassandra
                        BoundStatement boundStmt = pstmtAddItem.bind(
                            asin,
                            title,
                            imageUrl,
                            categories,
                            description
                        );
                        session.execute(boundStmt);
                        count.incrementAndGet();

                    } catch (Exception e) {
                        System.err.println("Error parsing or inserting line: " + currentLine);
                        e.printStackTrace();
                    }
                });
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("Successfully loaded " + count.get() + " items into Cassandra.");
    }

    /**
     * Load reviews from a file where each line is a single JSON object (possibly using single quotes).
     */
    @Override
    public void loadReviews(String pathReviewsFile) throws Exception {
        System.out.println("Loading reviews from file (one JSON object per line): " + pathReviewsFile);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        ExecutorService executor = Executors.newFixedThreadPool(250);
        AtomicInteger count = new AtomicInteger(0);

        try (BufferedReader br = Files.newBufferedReader(Paths.get(pathReviewsFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                // create a final variable for use in the lambda
                final String currentLine = line.trim();
                if (currentLine.isEmpty()) {
                    continue;
                }

                executor.submit(() -> {
                    try {
                        Map<String, Object> reviewMap = mapper.readValue(
                            currentLine,
                            new TypeReference<Map<String, Object>>() {}
                        );

                        String asin         = getStringValue(reviewMap, "asin",         NOT_AVAILABLE_VALUE);
                        String reviewerId   = getStringValue(reviewMap, "reviewerId",   NOT_AVAILABLE_VALUE);
                        String reviewerName = getStringValue(reviewMap, "reviewerName", NOT_AVAILABLE_VALUE);

                        // rating is an int; if missing, set -1
                        int rating = -1;
                        if (reviewMap.containsKey("rating") && reviewMap.get("rating") != null) {
                            rating = ((Number) reviewMap.get("rating")).intValue();
                        }

                        String summary    = getStringValue(reviewMap, "summary",    NOT_AVAILABLE_VALUE);
                        String reviewText = getStringValue(reviewMap, "reviewText", NOT_AVAILABLE_VALUE);

                        // Handle time from 'unixReviewTime'
                        long unixTime = 0L;
                        if (reviewMap.containsKey("unixReviewTime") && reviewMap.get("unixReviewTime") != null) {
                            unixTime = ((Number) reviewMap.get("unixReviewTime")).longValue();
                        }
                        Instant time = Instant.ofEpochSecond(unixTime);

                        // Insert into both tables via BATCH
                        BoundStatement boundStmt = pstmtAddReviewBatch.bind(
                            // reviews_by_id
                            reviewerId, time, asin, reviewerName, rating, summary, reviewText,
                            // reviews_by_item
                            asin, time, reviewerId, reviewerName, rating, summary, reviewText
                        );
                        session.execute(boundStmt);
                        count.incrementAndGet();

                    } catch (Exception e) {
                        System.err.println("Error parsing or inserting review line: " + currentLine);
                        e.printStackTrace();
                    }
                });
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("Successfully loaded " + count.get() + " reviews into Cassandra.");
    }


    @Override
    public String item(String asin) {
        System.out.println("[INFO] Fetching item for asin = " + asin);

        ResultSet rs = session.execute(pstmtSelectItem.bind(asin));
        Row row = rs.one();
        if (row == null) {
            return "not exists";
        }

        // Retrieve columns
        String dbAsin       = row.getString("asin");
        String dbTitle      = row.getString("title");
        String dbImage      = row.getString("imageURL");
        Set<String> dbCats  = row.getSet("categories", String.class);
        String dbDesc       = row.getString("description");

        // Use the formatItem method
        return formatItem(dbAsin, dbTitle, dbImage, dbCats, dbDesc);
    }

    @Override
    public Iterable<String> userReviews(String reviewerID) {
        System.out.println("[INFO] Fetching userReviews for reviewerID = " + reviewerID);

        ArrayList<String> reviews = new ArrayList<>();

        ResultSet rs = session.execute(pstmtSelectReviewById.bind(reviewerID));

        for (Row row : rs) {
            Instant time      = row.get("time", Instant.class);
            String asin       = row.getString("asin");
            String reviewerId = row.getString("reviewerId");
            String name       = row.getString("reviewerName");
            int rating        = row.getInt("rating");
            String summary    = row.getString("summary");
            String reviewText = row.getString("reviewText");

            // format using formatReview(...)
            String reviewStr = formatReview(time, asin, reviewerId, name, rating, summary, reviewText);
            reviews.add(reviewStr);
        }

        System.out.println("[INFO] Found " + reviews.size() + " reviews for user " + reviewerID);
        return reviews;
    }

    @Override
    public Iterable<String> itemReviews(String asin) {
        System.out.println("[INFO] Fetching itemReviews for asin = " + asin);

        ArrayList<String> reviews = new ArrayList<>();

        ResultSet rs = session.execute(pstmtSelectReviewByItem.bind(asin));

        for (Row row : rs) {
            Instant time      = row.get("time", Instant.class);
            String dbAsin     = row.getString("asin");
            String reviewerId = row.getString("reviewerId");
            String name       = row.getString("reviewerName");
            int rating        = row.getInt("rating");
            String summary    = row.getString("summary");
            String reviewText = row.getString("reviewText");

            String reviewStr = formatReview(time, dbAsin, reviewerId, name, rating, summary, reviewText);
            reviews.add(reviewStr);
        }

        System.out.println("[INFO] Found " + reviews.size() + " reviews for item " + asin);
        return reviews;
    }

    // -------------------------------------------------------------------------------------
    // Utility / Helper Methods
    // -------------------------------------------------------------------------------------

    private String getStringValue(Map<String, Object> map, String key, String defaultVal) {
        if (!map.containsKey(key) || map.get(key) == null) {
            return defaultVal;
        }
        return map.get(key).toString();
    }

    // -------------------------------------------------------------------------------------
    // Provided formatting methods - do not change
    // -------------------------------------------------------------------------------------
    private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
        String itemDesc = "";
        itemDesc += "asin: " + asin + "\n";
        itemDesc += "title: " + title + "\n";
        itemDesc += "image: " + imageUrl + "\n";
        itemDesc += "categories: " + categories.toString() + "\n";
        itemDesc += "description: " + description + "\n";
        return itemDesc;
    }

    private String formatReview(
            Instant time,
            String asin,
            String reviewerId,
            String reviewerName,
            Integer rating,
            String summary,
            String reviewText) {

        String reviewDesc =
            "time: " + time +
            ", asin: " + asin +
            ", reviewerID: " + reviewerId +
            ", reviewerName: " + reviewerName +
            ", rating: " + rating +
            ", summary: " + summary +
            ", reviewText: " + reviewText + "\n";
        return reviewDesc;
    }
}
