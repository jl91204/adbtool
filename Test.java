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
public class Test {


	static String fileName = "c:/jdbc/SetM.sql";
	static int studentNo=5;
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


		DB_NAME = "DB202203142057";
		DB_PASSWORD = "Oracle123456";
		
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
		
		final String DB_URL = "jdbc:oracle:thin:@" + DB_NAME.trim() + "_medium?TNS_ADMIN=c:/jdbc/wallet_DB202203142057";
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
		
	
	}
} 