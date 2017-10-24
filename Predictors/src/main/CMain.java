package main;

import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.Seconds;

import db.CMemSQL;
import utilities.CLogger;


public class CMain {
private static Options options;
	
	static{
		options = new Options();
		options.addOption(Option.builder("ing_ra").hasArgs().longOpt("ing_ra").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <recurso> <auxiliar>  calcula los pronosticos para los ingresos por recurso - auxiliar").build());
		options.addOption(Option.builder("ing_ra_all").hasArgs().longOpt("ing_ra_all").desc("<ejercicio> <mes de inicio> <numero de pronosticos> calcula los pronosticos para los ingresos por recurso - auxiliar (Todos los auxiliares)").build());
		options.addOption(Option.builder("ing_r").hasArgs().longOpt("ing_r").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <recurso> calcula los pronosticos para los ingresos por recurso").build());
		options.addOption(Option.builder("ing_r_all").hasArgs().longOpt("ing_r_all").desc("<ejercicio> <mes de inicio> <numero de pronosticos> calcula los pronosticos para los ingresos por recurso (Todos los recursos)").build());
		options.addOption(Option.builder("ing").hasArgs().longOpt("ing").desc("<ejercicio> <mes de inicio> <numero de pronosticos> calcula los pronosticos para los ingresos generales").build());
		options.addOption(Option.builder("help").longOpt("help").desc("ayuda de esta applicación").build());
	}
	
	final static  CommandLineParser parser = new DefaultParser();
	
	public static void main(String[] args) {
		try{
			DateTime start = new DateTime();
			CommandLine cline = parser.parse( options, args );
			if (CMemSQL.connect()){
				Connection conn = CMemSQL.getConnection();
				if(cline.hasOption("ing_ra")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos recurso - auxiliar...");
					String[] argumentos = cline.getOptionValues("ing_ra");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear()+1;
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    Integer recurso=(argumentos.length>3) ? Integer.parseInt(argumentos[3]) : 0;
				    Integer auxiliar=(argumentos.length>4) ? Integer.parseInt(argumentos[4]) : 0;
					CIngreso.getPronosticosRecursoAuxiliar(conn, recurso, auxiliar, año, mes, numero_datos, false);
				}
				if(cline.hasOption("ing_ra_all")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos recurso - auxiliar (Todos los auxiliares)...");
					String[] argumentos = cline.getOptionValues("ing_ra_all");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear()+1;
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    CIngreso.getPronosticosRecursoAuxiliarAll(conn, año, mes, numero_datos, false);
				}
				else if(cline.hasOption("ing_r")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos recurso...");
					String[] argumentos = cline.getOptionValues("ing_r");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear()+1;
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    Integer recurso=(argumentos.length>3) ? Integer.parseInt(argumentos[3]) : 0;
					CIngreso.getPronosticosRecurso(conn, recurso, año, mes, numero_datos, false);
				}
				else if(cline.hasOption("ing_r_all")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos recurso (Todos los recursos)...");
					String[] argumentos = cline.getOptionValues("ing_r_all");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear()+1;
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    CIngreso.getPronosticosRecursoAll(conn, año, mes, numero_datos, false);
				}
				else if(cline.hasOption("ing")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos totales...");
					String[] argumentos = cline.getOptionValues("ing");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear()+1;
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
					CIngreso.getPronosticosRecursosTotales(conn, año, mes, numero_datos, false);
				}
				else if(cline.hasOption("help")){
					 HelpFormatter formater = new HelpFormatter();
					 formater.printHelp(80,"Utilitario para calcular pronosticos", "", options,"");
					 System.exit(0);
				 }
				 if(!cline.hasOption("help")){
					 DateTime now = new DateTime();
					 CLogger.writeConsole("Tiempo total: " + Minutes.minutesBetween(start, now).getMinutes() + " minutos " + (Seconds.secondsBetween(start, now).getSeconds() % 60) + " segundos " +
					 (now.getMillis()%10) + " milisegundos ");
				 }
				conn.close();
			}
		}
		catch(Exception e){
			CLogger.writeFullConsole("No se ha logrado conexión con MemSQL", e);
		} 
	}

}
