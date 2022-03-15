package adb;

import java.io.File;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


public class TableTool {
	public  void dropTuserTable (Connection conn) {

		String dropString = "DROP TABLE tuser";
		
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString);

		} catch (SQLException e) {
			System.out.println("Drop Table tuser failed....");
			
		}
	}
	
	public  void createTuserTable (Connection conn) {

		String sql = "CREATE TABLE admin.tuser " + "(username varchar2(50) NOT NULL)";

		
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			System.out.println("Create tuser table error...");
		}
	}
	
	public  void populateTuserTable(Connection conn) {
		String sql = "insert into tuser (username) values(?)";
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, "teacher");
			ps.executeUpdate();
			
			for (int i = 1; i <= ADBTool.studentNo; i++) {
				ps.setString(1, "student"+i);
				ps.addBatch();
				ps.executeBatch();
			}
		
		} catch (SQLException e) {

			System.out.println("populating tuser table error...");;
		}
		
		
		
	}
	
	public  void dropInittable(Connection conn,String userName)  {

		String dropString = "DROP TABLE initialtable";
		// "CREATE TABLE initialtable("+"runstatement VARCHAR2(4000) NOT NULL)";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString);

		} catch (SQLException e) {
			//System.out.println("Drop Table initialtable failed...."+userName);
		}
	}
	
	public  void createInittable(Connection conn,String userName)  {

		String createString = "CREATE TABLE initialtable " + "(RUNID NUMBER(5,0) NOT NULL PRIMARY KEY, "
				+ "runstatement  VARCHAR2(4000) NOT NULL)";
		// "CREATE TABLE initialtable("+"runstatement VARCHAR2(4000) NOT NULL)";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(createString);
		} catch (SQLException e) {
			System.out.println("ERROR! Create Initialtable error..." +userName);
		}
	}
	
	public  void populateInitialTable(Connection conn,ArrayList<String> statementList,String userName) {
		System.out.println("populating initialtable sql statements.....");
		
		String sql = "insert into initialtable (runid,runstatement) values(?,?)";
		final int batchSize = 1000;
		int count = 1;
		long start = System.currentTimeMillis();
		try {PreparedStatement 	 ps = conn.prepareStatement(sql);

			for (String statement : statementList) {
				ps.setInt(1, count);
				ps.setString(2, statement);
				ps.addBatch();
				if (++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch(); // insert remaining records
			ps.close();
		} catch (BatchUpdateException e) {

			System.out.println("Error,Populating InitialTable error,......." + userName);
			System.out.println("You have to create schema object manually for "+userName+" by uploading and running the script file,.......");
			
		}catch (Exception e) {}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to insert all sql statements into database table: " + time);

		/*
		 * String
		 * insertString="INSERT INTO initialtable VALUES('DROP TABLE initialtable')";
		 * try (Statement stmt = conn.createStatement()) {
		 * stmt.executeUpdate(insertString); } catch (SQLException e) {
		 * JDBCUtilities.printSQLException(e); }
		 */
	}
	
	
	public  ArrayList<String> readUserName(Connection conn){
		
		ArrayList<String> list=new ArrayList<>();
	    String query = "select username from tuser";
	    
	    try{ Statement stmt = conn.createStatement();
	      ResultSet rs = stmt.executeQuery(query);
	      while (rs.next()) {
	        String userName = rs.getString("USERNAME");
	        list.add(userName);
	       // System.out.println("username is "+userName);
	      }
	      return list;
	    } catch (SQLException e) {
	      JDBCUtilities.printSQLException(e);
	      return list;
	    }
		
	}
	

}
