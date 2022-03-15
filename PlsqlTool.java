package adb;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PlsqlTool {
	/*
	private String plsql_dropAllusers = "create or replace PROCEDURE DROP_ALLUSERS_P AS \r\n" + 
			"    e_Notable EXCEPTION;\r\n" + 
			"  \r\n" + 
			"  PRAGMA EXCEPTION_INIT(e_Notable,-00942);\r\n" + 
			"BEGIN\r\n" + 
			"    for item_rec in (select username from admin.tuser)\r\n" + 
			"\r\n" + 
			"  LOOP\r\n" + 
			"    begin \r\n" + 
			"        EXECUTE IMMEDIATE 'drop user '|| item_rec.username ||' CASCADE';\r\n" + 
			"        DBMS_OUTPUT.put_line(item_rec.username ||'has been droped');\r\n" + 
			"    EXCEPTION WHEN others then\r\n" + 
			"    DBMS_OUTPUT.put_line('No user '||item_rec.username);\r\n" + 
			"    end;\r\n" + 
			"   end loop;\r\n" + 
			"END DROP_ALLUSERS_P;";
	*/
	
	private String plsql_dropAllusers = "create or replace PROCEDURE DROP_ALLUSERS_P \r\n" + 
			" AS \r\n" + 
			"BEGIN\r\n" + 
			"    EXECUTE IMMEDIATE 'drop user TEACHER CASCADE';\r\n" + 
			"    for item_rec in (select username from dba_users where username like 'STUDENT%')\r\n" + 
			"    loop\r\n" + 
			"    --DBMS_OUTPUT.put_line('username is : '||item_rec.username);\r\n" + 
			"    EXECUTE IMMEDIATE 'drop user '||item_rec.username||' CASCADE';\r\n" + 
			"    end loop;\r\n" + 
			"END DROP_ALLUSERS_P;";
	
	
	
	private String plsql_dropInitialtable="CREATE OR REPLACE PROCEDURE DROP_ALLINITIALTABLE_P AS \r\n" + 
			"    e_Notable EXCEPTION;\r\n" + 
			"  \r\n" + 
			"  PRAGMA EXCEPTION_INIT(e_Notable,-00942);\r\n" + 
			"BEGIN\r\n" + 
			"    for item_rec in (select username from admin.tuser)\r\n" + 
			"\r\n" + 
			"  LOOP\r\n" + 
			"    begin \r\n" + 
			"        EXECUTE IMMEDIATE 'drop table '|| item_rec.username ||' .initialtable';\r\n" + 
			"        DBMS_OUTPUT.put_line(item_rec.username ||' initialtable has been droped');\r\n" + 
			"    EXCEPTION WHEN others then\r\n" + 
			"    DBMS_OUTPUT.put_line('No user '||item_rec.username);\r\n" + 
			"    end;\r\n" + 
			"   end loop;\r\n" + 
			"END DROP_ALLINITIALTABLE_P;";
	

	
	private String plsql_createSchema = "create or replace PROCEDURE CREATE_SCHEMA_P AS \r\n" + 
			"e_Notable EXCEPTION;\r\n" + 
			"\r\n" + 
			"PRAGMA EXCEPTION_INIT(e_Notable,-00942);\r\n" + 
			"BEGIN\r\n" + 
			"  for item_rec in (select runid,runstatement from initialtable order by runid)\r\n" + 
			"  LOOP\r\n" + 
			"\r\n" + 
			"    begin \r\n" + 
			"        --DBMS_OUTPUT.put_line(item_rec.RUNID ||'----'||item_rec.runstatement);\r\n" + 
			"        EXECUTE IMMEDIATE item_rec.runstatement;\r\n" + 
			" \r\n" + 
			"    EXCEPTION WHEN others then\r\n" + 
			"    DBMS_OUTPUT.put_line('error for ' || item_rec.runstatement);\r\n" + 
			"    end;\r\n" + 
			"   end loop;\r\n" + 
			"    \r\n" + 
			"END;";
	
	private String plsql_createPermissions="create or replace PROCEDURE create_PERMISSIONS_P AS \r\n" + 
			"    e_Notable EXCEPTION;\r\n" + 
			"\r\n" + 
			"  PRAGMA EXCEPTION_INIT(e_Notable,-00942);\r\n" + 
			"BEGIN\r\n" + 
			"    for item_rec in (select username from admin.tuser)\r\n" + 
			"  LOOP\r\n" + 
			"    begin \r\n" + 
			"        EXECUTE IMMEDIATE 'grant create any table to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant ALTER ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant SELECT ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant UPDATE ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DELETE ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant ALTER ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY TABLE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant INSERT ANY TABLE to '|| item_rec.username;\r\n" + 
			"        \r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY INDEX to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY INDEX to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant ALTER ANY INDEX to '|| item_rec.username;\r\n" + 
			"        \r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY SEQUENCE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant ALTER ANY SEQUENCE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY SEQUENCE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant SELECT ANY SEQUENCE to '|| item_rec.username;\r\n" + 
			"        \r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY PROCEDURE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant ALTER ANY PROCEDURE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY PROCEDURE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant EXECUTE ANY PROCEDURE to '|| item_rec.username;\r\n" + 
			"\r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY SYNONYM to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE PUBLIC SYNONYM to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY SYNONYM to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP PUBLIC SYNONYM to '|| item_rec.username;\r\n" + 
			"        \r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY TRIGGER to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY TRIGGER to '|| item_rec.username;\r\n" + 
			"        \r\n" + 
			"        \r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY TYPE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant ALTER ANY TYPE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY TYPE to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant EXECUTE ANY TYPE to '|| item_rec.username;\r\n" + 
			"        \r\n" + 
			"        EXECUTE IMMEDIATE 'grant CREATE ANY VIEW to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant DROP ANY VIEW to '|| item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'grant UNDER ANY VIEW to '|| item_rec.username;\r\n" + 
			"        \r\n" + 
			"        DBMS_OUTPUT.put_line(item_rec.username ||' has been granted');\r\n" + 
			"    EXCEPTION WHEN others then\r\n" + 
			"    DBMS_OUTPUT.put_line('No user '||item_rec.username);\r\n" + 
			"    end;\r\n" + 
			"   end loop;\r\n" + 
			"\r\n" + 
			"END create_PERMISSIONS_P;";
	


	
	private String plsql_createAllUsers = "create or replace PROCEDURE CREATE_ALLUSERS_P AS \r\n" + 
			"    e_Notable EXCEPTION;\r\n" + 
			"  \r\n" + 
			"  PRAGMA EXCEPTION_INIT(e_Notable,-00942);\r\n" + 
			"BEGIN\r\n" + 
			"    for item_rec in (select username from admin.tuser)\r\n" + 
			"\r\n" + 
			"  LOOP\r\n" + 
			"    begin \r\n" + 
			"        EXECUTE IMMEDIATE 'create user '|| item_rec.username||' IDENTIFIED BY " +"\"" + ADBTool.USER_PASSWORD +"\"" + "';\r\n" + 
			"        EXECUTE IMMEDIATE 'grant dwrole to ' || item_rec.username;\r\n" + 
			"        EXECUTE IMMEDIATE 'alter user '||item_rec.username||' quota " + ADBTool.userQuata + "M on data';\r\n" + 
			"        DBMS_OUTPUT.put_line(item_rec.username ||'has been created');\r\n" + 
			"    EXCEPTION WHEN others then\r\n" + 
			"    DBMS_OUTPUT.put_line('create errors '||item_rec.username);\r\n" + 
			"    end;\r\n" + 
			"   end loop;\r\n" + 
			"END CREATE_ALLUSERS_P;";
	
	
/////////////// drop all user Initialtable //////////////
	
	private  void dropPlsql_dropInitialtable(Connection conn)  {
		String dropString_dropInitialTable = "DROP PROCEDURE DROP_ALLINITIALTABLE_P";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString_dropInitialTable);
			} catch (SQLException e) {
			//System.out.println("Not error,DROP_ALLINITIALTABLE_P does not exist, can not dropped....");
		}
		
	}
		
	private  void createPlsql_dropInitialtable(Connection conn) {
		try (CallableStatement statement = conn.prepareCall(plsql_dropInitialtable);) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("Error, Create procedure DROP_ALLINITIALTABLE_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
	}	
	
	private  void runPlsql_dropInitialtable(Connection conn)  {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql DROP_ALLINITIALTABLE_P()......... : ");
		try (CallableStatement statement = conn.prepareCall("{call DROP_ALLINITIALTABLE_P()}");) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("ERROR! Calling procedure DROP_ALLINITIALTABLE_P() failed....");
			//JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql DROP_ALLINITIALTABLE_P......... : " + time);
	}
	
	
	/////////////////// create Schema////////////////
	private  void dropPlsql_createSchema(Connection conn)  {
		String dropString_createSchema = "DROP PROCEDURE CREATE_SCHEMA_P";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString_createSchema);
			} catch (SQLException e) {
			//System.out.println("Drop plsql procedure CREATE_SCHEMA_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		
	}
		
	private  void createPlsql_createSchema(Connection conn) {
		try (CallableStatement statement = conn.prepareCall(plsql_createSchema);) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("Error, Create procedure CREATE_SCHEMA_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
	}	
	
	private  void runPlsql_createSchema(Connection conn,String username) {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql procedure CREATE_SCHEMA_P .... : for " + username);
		try (CallableStatement cstmt = conn.prepareCall("{call CREATE_SCHEMA_P()}");) {
		cstmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("ERROR! calling procedure CREATE_SCHEMA_P failed for .... "+ username);
			//JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql CREATE_SCHEMA_P for.. : "+username+" is:" + time);
	}

	///////////////////Drop all users//////////////////
	
	private  void dropPlsql_dropAllUsers(Connection conn)  {
		String dropString_createSchema = "DROP PROCEDURE DROP_ALLUSERS_P";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString_createSchema);
			} catch (SQLException e) {
			//System.out.println("Drop plsql procedure DROP_ALLUSERS_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		
	}
		
	private  void createPlsql_dropAllUsers(Connection conn) {
		System.out.println("create  DROP_ALLUSERS_P procedure......... : ");
		try (CallableStatement statement = conn.prepareCall(plsql_dropAllusers);) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("ERROR! Create procedure plsql_dropAllusers failed....");
			//JDBCUtilities.printSQLException(e);
		}
	}
	
	
	private  void runPlsql_dropAllUsers(Connection conn)  {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql DROP_ALLUSERS_P......... : ");
		try (CallableStatement statement = conn.prepareCall("{call DROP_ALLUSERS_P()}");) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("ERROR, Calling procedure DROP_ALLUSERS_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql DROP_ALLUSERS_P......... : " + time);
	}
	
	
	//////////////////////////create all users///////////////////
	
	private  void dropPlsql_createAllUsers(Connection conn)  {
		String dropString_createUsers = "DROP PROCEDURE CREATE_ALLUSERS_P";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString_createUsers);
			} catch (SQLException e) {
			System.out.println("Drop plsql procedure CREATE_ALLUSERS_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		
	}
		
	private  void createPlsql_createAllUsers(Connection conn) {
		try (CallableStatement statement = conn.prepareCall(plsql_createAllUsers);) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("ERROR! Create procedure plsql_createAllUsers failed....");
			//JDBCUtilities.printSQLException(e);
		}
	}
	
	
	private  void runPlsql_createAllUsers(Connection conn)  {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql CREATE_ALLUSERS_P ......... : ");
		try (CallableStatement cstmt = conn.prepareCall("{call CREATE_ALLUSERS_P()}");) {
		cstmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("ERROR, Calling procedure CREATE_ALLUSERS_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql CREATE_ALLUSERS_P ......... : " + time);
	}

		


	
///////////////////////user permission /////////

	private  void dropPlsql_createPermission(Connection conn)  {
		String dropString_createSchema = "DROP PROCEDURE CREATE_PERMISSIONS_P";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString_createSchema);
			} catch (SQLException e) {
			//System.out.println("Drop plsql procedure CREATE_SCHEMA_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		
	}

	private  void createPlsql_createPermission(Connection conn) {
		
		try (CallableStatement statement = conn.prepareCall(plsql_createPermissions);) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("ERROR, Create procedure CREATE_SCHEMA_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
	}	
	
	private  void runPlsql_createPermission(Connection conn)  {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql CREATE_PERMISSIONS_P ......... : ");
		try (CallableStatement cstmt = conn.prepareCall("{call CREATE_PERMISSIONS_P()}");) {
		cstmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Calling procedure CREATE_PERMISSIONS_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql CREATE_PERMISSIONS_P ......... : " + time);
	}
	
	
//////////////////////enable apex /////////
	
	private  void dropPlsql_enableApex(Connection conn)  {
		String dropString_createSchema = "DROP PROCEDURE ENABLE_APEX_P";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString_createSchema);
			} catch (SQLException e) {
			//System.out.println("Drop plsql procedure CREATE_SCHEMA_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		
	}
	private  void dropPlsql_dropApex(Connection conn)  {
		String dropString_createSchema = "DROP PROCEDURE DROP_APEX_P";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropString_createSchema);
			} catch (SQLException e) {
			//System.out.println("Drop plsql procedure CREATE_SCHEMA_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		
	}
	
	private  void createPlsql_enableApex(Connection conn) {
		
		try (CallableStatement statement = conn.prepareCall(plsql_enableApex);) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("ERROR, Create procedure enable_apex_p failed....");
			//JDBCUtilities.printSQLException(e);
		}
	}	
	
	
	private  void createPlsql_dropApex(Connection conn) {
		
		try (CallableStatement statement = conn.prepareCall(plsql_dropApex);) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("ERROR, Create procedure drop_apex_p failed....");
			//JDBCUtilities.printSQLException(e);
		}
	}
	
	private  void runPlsql_enableApex(Connection conn)  {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql ENABLE_APEX_P ......... : ");
		try (CallableStatement cstmt = conn.prepareCall("{call ENABLE_APEX_P()}");) {
		cstmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Calling procedure ENABLE_APEX_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql ENABLE_APEX_P ......... : " + time);
	}
	
	private  void runPlsql_dropApex(Connection conn)  {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql DROP_APEX_P ......... : ");
		try (CallableStatement cstmt = conn.prepareCall("{call DROP_APEX_P()}");) {
		cstmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Calling procedure DROP_APEX_P failed....");
			//JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql DROP_APEX_P ......... : " + time);
	}
	
	////////////////// call from ADBTool////////////////////////
	public  void dropAllUsers(Connection conn) {
		
		dropPlsql_dropAllUsers(conn);
		createPlsql_dropAllUsers(conn);
		runPlsql_dropAllUsers(conn);
		dropPlsql_dropAllUsers(conn);
	}
	
	public void createAllUsers(Connection conn) {
		dropPlsql_createAllUsers(conn);
		createPlsql_createAllUsers(conn);
		runPlsql_createAllUsers(conn);
		dropPlsql_createAllUsers(conn);
	}
	
	
	public void createUserPermissions(Connection conn) {
		
		dropPlsql_createPermission(conn);
		createPlsql_createPermission(conn);
		runPlsql_createPermission(conn);
		dropPlsql_createPermission(conn);
		
	}
	

			
	public void createUserSchemas(Connection conn,String userName) {
		dropPlsql_createSchema(conn);
		createPlsql_createSchema(conn);
		runPlsql_createSchema(conn, userName);
	}
	
	public void dropInitialtable(Connection conn) {
		
		dropPlsql_dropInitialtable(conn);
		createPlsql_dropInitialtable(conn);
		runPlsql_dropInitialtable(conn);
	}
	



		
	public  void runtest(Connection conn)  {
		long start = System.currentTimeMillis();
		System.out.println("Calling plsql test......... : ");
		try (CallableStatement statement = conn.prepareCall("{call admin.test()}");) {
			statement.execute();
		} catch (SQLException e) {
			System.out.println("Create procedure test failed....");
			JDBCUtilities.printSQLException(e);
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("the time spent to execute plsql test......... : " + time);
	}
}
