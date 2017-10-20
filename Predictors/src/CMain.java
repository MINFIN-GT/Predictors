
import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;

import db.CHive;
import utilities.CLogger;


public class CMain {
private static Options options;
	
	static{
		options = new Options();
		options.addOption("tn_ejes", "tn-ejes", false, "calcula los datos de los ejes del TN");
		options.addOption("egc", "eventos-guatecompras", true, "cargar eventos guatecompras");
		options.addOption("egch", "eventos-guatecompras-historia", true, "cargar historia de eventos guatecompras");
	}
	
	final static  CommandLineParser parser = new DefaultParser();
	
	public static void main(String[] args) {
		try{
			DateTime today = new DateTime();
			CommandLine cline = parser.parse( options, args );
			if (CHive.connect()){
				Connection conn = CHive.getConnection();
				if(cline.hasOption("tn-ejes")){
					CLogger.writeConsole("Inicio calculos financieros de ejes del triangulo norte...");
					 
				}
				CHive.close();
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("No se ha logrado conexión con Hive", e);
		} 
	}

}
