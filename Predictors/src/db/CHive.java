package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import utilities.CLogger;
import utilities.CProperties;

public class CHive {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static Connection connection;
	private static Statement st;
	
	public static boolean connect(){
		try {
	        Class.forName(driverName);
	        connection = DriverManager.getConnection(String.join("", "jdbc:hive2://",CProperties.getHive_host(),":",CProperties.getHive_port(),"/",CProperties.getHive_database()), CProperties.getHive_user(), CProperties.getHive_password());
	        return !connection.isClosed();
		} catch (Exception e) {
			CLogger.writeFullConsole("Error 1: CHive.class", e);
	    }
	    return false;
	}
	
	/*public static  boolean connectdes(){
		try {
	        Class.forName(driverName);
	        connection = DriverManager.getConnection(String.join("", "jdbc:hive2://",CProperties.getHive_host(),":",CProperties.getHive_port(),"/",CProperties.getHive_databasedes()), CProperties.getHive_user(), CProperties.getHive_password());
	        return !connection.isClosed();
		} catch (Exception e) {
			CLogger.writeFullConsole("Error 2: CHive.class", e);
	    }
	    return false;
	}*/
	
	public static Connection openConnection(){
		try {
	        Class.forName(driverName);
	        connection = DriverManager.getConnection(String.join("", "jdbc:hive2://",CProperties.getHive_host(),":",CProperties.getHive_port(),"/",CProperties.getHive_database()), CProperties.getHive_user(), CProperties.getHive_password());
	        return connection;
		} catch (Exception e) {
			CLogger.writeFullConsole("Error 3: CHive.class", e);
	    }
	    return null;
	}
	
	/*public static Connection openConnectiondes(){
		try {
	        Class.forName(driverName);
	        connection = DriverManager.getConnection(String.join("", "jdbc:hive2://",CProperties.getHive_host(),":",CProperties.getHive_port(),"/",CProperties.getHive_databasedes()), CProperties.getHive_user(), CProperties.getHive_password());
	        return connection;
		} catch (Exception e) {
			CLogger.writeFullConsole("Error 3: CHive.class", e);
	    }
	    return null;
	}*/
	
	public static Connection getConnection(){
		return connection;
	}
	
	public static void close(){
		try{
			connection.close();
		}
		catch(Exception e) { 
			CLogger.writeFullConsole("Error 4: CHive.class", e);
		}
	}
	
	public static void close(Connection conn){
		try {
			conn.close();
		} catch (Exception e) {
			CLogger.writeFullConsole("Error 5: CHive.class", e);
		}
	}
	
	public static ResultSet runQuery(String query){
		ResultSet ret=null;
		try{
			if(!connection.isClosed()){
				st = connection.createStatement();
				ret = st.executeQuery(query);
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 6: CHive.class", e);
		}
		return ret;
	}
	
	public static Integer countQuery(String query){
		Integer ret=null;
		try{
			if(!connection.isClosed()){
				st = connection.createStatement();
				ResultSet result = st.executeQuery(query);
				if(result.next()){
					ret = result.getInt(1);
				}
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("Error 7: CHive.class", e);
		}
		return ret;
	}
}
