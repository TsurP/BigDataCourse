package bigdatacourse.hw2;

import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import org.json.JSONObject;
import org.json.JSONTokener;
import bigdatacourse.hw2.studentcode.HW2StudentAnswer;



public class HW2CLI {

	// consts
	private static final String				HW2_KEYSPACE			=	"bigdatacourse";
	
	private static final String				FILE_ASTRA_DB			=	"secure-connect-hw2.zip";
	private static final String				FILE_LOGIN_TOKEN		=	"login-token.json";
	
	private static final String				FILE_DATASET_ITEMS		=	"meta_Office_Products.json";
	private static final String				FILE_DATASET_REVIEWS	=	"reviews_Office_Products.json";

	private static final String				FILE_SEPARATOR			=	System.getProperty("file.separator");
	
	
	public static void main(String[] args) throws Exception {
		HW2CLI hw2CLI = new HW2CLI();		// creating the object
		hw2CLI.parsePassedFolders(args);	// saving 
		hw2CLI.runCLI();					// running the CLI
		
		System.out.println("HW2 CLI terminated, bye bye...");
	}
	
	
	private String 		pathAstraDBFolder;		// with the "Secure Connect Bundle" and GeneratedToken.csv
	private String 		pathDatasetFolder;		// the dataset folder
	private HW2API		hw2API;					// will contain student answers
	
	
	public HW2CLI() {
		this.hw2API	=	new HW2StudentAnswer();
	}


	public void runCLI() {
		// initializing the input
		Scanner scanner = new Scanner(System.in);
		
		// print the menu
		printHelp();

		// looping
		boolean isRunning = true;
		while (isRunning) {
			// getting the input
			System.out.print("> ");
			String line		=	scanner.nextLine();
			String[] tokens	=	line.split(" ");
			String input	=	tokens[0]; 

			try {
				switch (input) {
					case "connect": {			
						hw2API.connect(
							pathAstraDBFolder + FILE_ASTRA_DB, 
							getUsername(pathAstraDBFolder + FILE_LOGIN_TOKEN), 
							getPasswrod(pathAstraDBFolder + FILE_LOGIN_TOKEN),
							HW2_KEYSPACE
						);
						break;
					}							
					case "createTables":		hw2API.createTables();			break;
					case "initialize":			hw2API.initialize();			break;
					case "loadItems":			hw2API.loadItems(pathDatasetFolder + FILE_DATASET_ITEMS);		break;
					case "loadReviews":			hw2API.loadReviews(pathDatasetFolder + FILE_DATASET_REVIEWS);	break;
					case "item": {			
						String itemReper = hw2API.item(tokens[1]);											
						System.out.print(itemReper);
						break;
					}
					case "userReviews": {
						Iterable<String> reviewsRepr = hw2API.userReviews(tokens[1]);
						for (String reviewRepr : reviewsRepr)
							System.out.print(reviewRepr);
						break;
					}
					case "itemReviews": {
						Iterable<String> reviewsReper = hw2API.itemReviews(tokens[1]);	
						for (String reviewRepr : reviewsReper)
							System.out.print(reviewRepr);
						break;
					}

					case "help":				printHelp();					break;
					case "exit":				isRunning = false;
												hw2API.close();
												break;
												
					case "":					// do nothing
												break;
												
					default:					System.out.println("Unknonw input. type 'help'"); 	
												break;
				}
			}
			catch (Exception e) {
				System.out.println(e.toString());
			}
		}
			
		// closing the scanner
		scanner.close();
	}
		
		
	
	private void printHelp() {
		System.out.println("-------------------- HW2 options ----------------------------");
		System.out.println("connect \t\t connect to the DB");
		System.out.println("createTables\t\t creates the tables");
		System.out.println("initialize\t\t initialize the logic (prepared statements)");
		System.out.println("loadItems\t\t prase and lode the items");
		System.out.println("loadReviews\t\t prase and lode the reviews");
		System.out.println("item * \t\t \t print the info for item *");
		System.out.println("userReviews *\t\t print the reviews for user *");
		System.out.println("itemReviews *\t\t print the reviews for item *");
		System.out.println("help    \t\t print available commands");
		System.out.println("exit    \t\t exit the CLI");
		System.out.println("-------------------------------------------------------------");
	}
	
	
	
	
	private void parsePassedFolders(String[] args) throws Exception {
		if (args.length != 2)
			throw new Exception("ERROR - 2 folder paths are required to be passed: first astradb and then dataset folders");
		
		String pathAstraDBFolder 	= 	args[0] + FILE_SEPARATOR;
		String pathDatasetFolder	=	args[1] + FILE_SEPARATOR;

		
		// validating files for astradb
		validateFileExists(pathAstraDBFolder, FILE_ASTRA_DB);
		validateFileExists(pathAstraDBFolder, FILE_LOGIN_TOKEN);
		
		// validating files for dataset
		validateFileExists(pathDatasetFolder, FILE_DATASET_ITEMS);
		validateFileExists(pathDatasetFolder, FILE_DATASET_REVIEWS);
		
		// saving
		this.pathAstraDBFolder	=	pathAstraDBFolder;
		this.pathDatasetFolder	=	pathDatasetFolder;
	}
	
	
	private static void validateFileExists(String path, String filename) throws Exception {
		if (new File(path + filename).exists() == false)
			throw new Exception("ERROR - can not find file " + path + filename);
	}
	
	private static String getUsername(String filePath) throws Exception {
		return loadJSONFromFile(filePath).getString("clientId");
	}
	
	private static String getPasswrod(String filePath) throws Exception {
		return loadJSONFromFile(filePath).getString("secret");
	}

	private static JSONObject loadJSONFromFile(String filePath) throws Exception {
		FileReader reader = new FileReader(filePath);
        JSONTokener tokener = new JSONTokener(reader);
        return new JSONObject(tokener); 
	}
	
}
