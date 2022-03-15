package adb;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;

import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.ucp.jdbc.PoolDataSource;

/*
 * The sample demonstrates connecting to Autonomous Database using 
 * Oracle JDBC driver and UCP as a client side connection pool.
 */
public class ADBTool {


	static String fileName = "c:/jdbc/SetM.sql";
	static int studentNo=50;
	static int userQuata =20000/(studentNo+2);

	private static String DB_NAME;
	
	static String DB_USER = "admin";
	private static String DB_PASSWORD;
	
	//static String USER_PASSWORD = "ChangeMe@1234";
	
	static String USER_PASSWORD = "ChangeMe@1234";
	
	private static ArrayList<String> statementList;
	private  ArrayList<String> userNameList;
	TableTool tableTool;
	PlsqlTool plsqlTool;
	
	public static void main(String args[]) throws Exception {

		/*
		 * String DB_NAME=null; String DB_PASSWORD = null ; try { Scanner scanner = new
		 * Scanner(System.in);
		 * System.out.print("Enter the Database Name for Autonomous Database: ");
		 * DB_NAME = scanner.nextLine(); } catch (Exception e) {
		 * System.out.println("ADBQuickStart - Exception occurred : " + e.getMessage());
		 * System.exit(1); }
		 */
		
		
		// For security purposes, you must enter the password through the console
		/*
		 * try { Scanner scanner = new Scanner(System.in);
		 * System.out.print("Enter the password for Autonomous Database: "); DB_PASSWORD
		 * = scanner.nextLine();
		 * 
		 * 
		 * } catch (Exception e) {
		 * System.out.println("ADBQuickStart - Exception occurred : " + e.getMessage());
		 * System.exit(1); }
		 */

		
		Path path = Paths.get(fileName);
		if(ReadSql.isContainBOM(path)) {
	        System.out.println("Found BOM!");
			ReadSql.removeBom(path);
		}
		
		
		statementList=ReadSql.readFromFile();
		int lineNo=0;
		for(String s:statementList) {
				lineNo++;
				System.out.println("Line No "+lineNo+ "***** is : "+s);
		}

		ADBTool adb=new ADBTool();
		adb.startApp();

	}	
	public void startApp() {
		
		
		long totalStart = System.currentTimeMillis();

		tableTool = new TableTool();
		plsqlTool=new PlsqlTool();
		// Make sure to have Oracle JDBC driver 18c or above
		// to pass TNS_ADMIN as part of a connection URL.
		// TNS_ADMIN - Should be the path where the client credentials zip
		// (wallet_dbname.zip) file is downloaded.
		// dbname_medium - It is the TNS alias present in tnsnames.ora.



		final String DB_URL = "jdbc:oracle:thin:@" + DB_NAME.trim() + "_medium?TNS_ADMIN=c:/jdbc/wallet_DB202203141455";
		// Update the Database Username and Password to point to your Autonomous
		// Database


		final String CONN_FACTORY_CLASS_NAME = "oracle.jdbc.pool.OracleDataSource";


		// Get the PoolDataSource for UCP
		PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
		try {
		// Set the connection factory first before all other properties
		pds.setConnectionFactoryClassName(CONN_FACTORY_CLASS_NAME);
		pds.setURL(DB_URL);
		pds.setUser(DB_USER);
		pds.setPassword(DB_PASSWORD);
		pds.setConnectionPoolName("JDBC_UCP_POOL");

		// Default is 0. Set the initial number of connections to be created
		// when UCP is started.
		pds.setInitialPoolSize(2);

		// Default is 0. Set the minimum number of connections
		// that is maintained by UCP at runtime.
		pds.setMinPoolSize(2);

		// Default is Integer.MAX_VALUE (2147483647). Set the maximum number of
		// connections allowed on the connection pool.
		pds.setMaxPoolSize(2);
		}catch(Exception e) {System.out.println("Can not Initialize ADMIN PoolSize ");}
		// Get the database connection from UCP.
		try (Connection conn = pds.getConnection()) {
			//System.out.println("Available connections after checkout: " + pds.getAvailableConnectionsCount());
			//System.out.println("Borrowed connections after checkout: " + pds.getBorrowedConnectionsCount());
			// Perform a database operation
			
	////////////////// create tuser table //////////////		
			tableTool.dropTuserTable(conn);
			tableTool.createTuserTable(conn);
			tableTool.populateTuserTable(conn);

	//----------------read the user from the tuser table--------------------------------------
			
			userNameList=tableTool.readUserName(conn);
			for(String s:userNameList) {
				System.out.println("username in the List is "+s);
			}

			
	//---------------------  drop and create a set of new database user------------------------------
			plsqlTool.dropAllUsers(conn);
			plsqlTool.createAllUsers(conn);
			
	//-------------------give the new created user all neccessary permission--------------------------------
			plsqlTool.createUserPermissions(conn);
			

			
			
			conn.close();
		} catch (SQLException e) {
			// System.out.println("ADBQuickStart - "
			// + "doSQLWork()- SQLException occurred : " + e.getMessage());
		}//end of admin connection
	
		//----------------for each database user create schema objects------------------------------
							
		for(String userName:userNameList) {

			try {
				pds.setUser(userName);
				pds.setPassword(USER_PASSWORD);
			}catch(Exception e) {System.out.println("Can not Initialize PoolSize ");}
			try (Connection conn = pds.getConnection()) {
				System.out.println("you are in the "+userName+" connection");
				long start = System.currentTimeMillis();
				
				tableTool.dropInittable(conn,userName);
				tableTool.createInittable(conn,userName);
				tableTool.populateInitialTable(conn, statementList,userName);
				
				plsqlTool.createUserSchemas(conn, userName);
				tableTool.dropInittable(conn,userName);
				
			long time = System.currentTimeMillis() - start;
			System.out.println("the time spent to create Account: "+userName+"---" + time + " Seconds");
			conn.close();
			}catch(Exception e) {}
		} //end of for loop
		
		//////////////////////// clear out drop each user initialtable/////////////
		/*try {
			pds.setUser(DB_USER);
			pds.setPassword(DB_PASSWORD);
		}catch(Exception e) {System.out.println("Can not Initialize PoolSize ");}
		try (Connection conn = pds.getConnection()) {
			System.out.println("clear all user initialtable ");
			long start = System.currentTimeMillis();
			
			plsqlTool.dropInitialtable(conn);
			
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to drop all user inittialtable " + time);
		conn.close();
		}catch(Exception e) {}
		
		long totalTime = System.currentTimeMillis() - totalStart;
		System.out.println("the Total time spent to execute the ADBTool is ......... : " + totalTime/1000 +" Seconds");
		*/
	}
} 