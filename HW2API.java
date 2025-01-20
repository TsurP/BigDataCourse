package bigdatacourse.hw2;

public interface HW2API {

	// connects to AstraDB
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace);
	
	// close the connection to AstraDB
	public void close();
	
	// create database tables;
	public void createTables();
	
	// initialize the prepared statements 
	public void initialize();
	
	// loads the items in the file into the db
	public void loadItems(String pathItemsFile) throws Exception;
	
	// loads the reviews into the db
	public void loadReviews(String pathReviewsFile) throws Exception;
	
	// returns the item's details. the categories should be ordered 
	public String item(String asin);

	// returns the user's reviews, ordered by review time (desc) and then by the asin
	public Iterable<String> userReviews(String reviewerID);
	
	// returns the items's reviews, ordered by review time (desc) and then by the reviewerID
	public Iterable<String> itemReviews(String asin);
}
