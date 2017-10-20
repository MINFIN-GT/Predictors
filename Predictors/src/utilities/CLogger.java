package utilities;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CLogger {

	
	public CLogger(){
		
	}
	
	static public void writeConsole(String message){
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		System.out.println(String.join(" ", sdf.format(date),message));
	}	
	
	static public void writeFullConsole(String message, Exception e){
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		System.out.println(String.join(" ", sdf.format(date),message, "\n", e.getMessage()));
		e.printStackTrace(System.out);
	}
} 