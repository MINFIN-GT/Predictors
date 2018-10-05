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
		options.addOption(Option.builder("ing_ra").hasArgs().longOpt("ing_ra").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <recurso> <auxiliar> <ajustado>  calcula los pronosticos para los ingresos por recurso - auxiliar").build());
		options.addOption(Option.builder("ing_ra_all").hasArgs().longOpt("ing_ra_all").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los ingresos por recurso - auxiliar (Todos los auxiliares)").build());
		options.addOption(Option.builder("ing_r").hasArgs().longOpt("ing_r").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <recurso> <ajustado> calcula los pronosticos para los ingresos por recurso").build());
		options.addOption(Option.builder("ing_r_all").hasArgs().longOpt("ing_r_all").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los ingresos por recurso (Todos los recursos)").build());
		options.addOption(Option.builder("ing").hasArgs().longOpt("ing").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los ingresos generales").build());
		
		options.addOption(Option.builder("egr").hasArgs().longOpt("egr").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los egresos generales").build());
		options.addOption(Option.builder("egr_fp").hasArgs().longOpt("egr_fp").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los egresos generales").build());
		options.addOption(Option.builder("egr_grupo").hasArgs().longOpt("egr_grupo").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los egresos generales").build());
		options.addOption(Option.builder("egr_anticipos").hasArgs().longOpt("egr_anticipos").desc("<ejercicio> calcula los pronosticos para los egresos contables de anticipos").build());
		options.addOption(Option.builder("egr_sin_reg").hasArgs().longOpt("egr_sin_reg").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los egresos generales sin regularizaciones").build());
		options.addOption(Option.builder("egr_grupo_sin_reg").hasArgs().longOpt("egr_grupo_sin_reg").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los egresos generales sin regularizaciones").build());
		options.addOption(Option.builder("egr_sin_reg_fp").hasArgs().longOpt("egr_sin_reg_fp").desc("<ejercicio> <mes de inicio> <numero de pronosticos> <ajustado> calcula los pronosticos para los egresos generales sin regularizaciones").build());
		
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
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    Integer recurso=(argumentos.length>3) ? Integer.parseInt(argumentos[3]) : 0;
				    Integer auxiliar=(argumentos.length>4) ? Integer.parseInt(argumentos[4]) : 0;
				    boolean fecha_real=(argumentos.length>5) ? argumentos[5].compareTo("Real")==0 : false;
				    boolean ajustado=(argumentos.length>6) ? Integer.parseInt(argumentos[6])==1 : false;
					CIngreso.getPronosticosRecursoAuxiliar(conn, recurso, auxiliar, año, mes, numero_datos, ajustado, fecha_real);
				}
				if(cline.hasOption("ing_ra_all")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos recurso - auxiliar (Todos los auxiliares)...");
					String[] argumentos = cline.getOptionValues("ing_ra_all");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    boolean fecha_real=(argumentos.length>3) ? argumentos[3].compareTo("Real")==0 : false;
				    boolean ajustado=(argumentos.length>4) ? Integer.parseInt(argumentos[4])==1 : false;
				    CIngreso.getPronosticosRecursoAuxiliarAll(conn, año, mes, numero_datos, ajustado, fecha_real);
				}
				else if(cline.hasOption("ing_r")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos recurso...");
					String[] argumentos = cline.getOptionValues("ing_r");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    Integer recurso=(argumentos.length>3) ? Integer.parseInt(argumentos[3]) : 0;
				    boolean fecha_real=(argumentos.length>4) ? argumentos[4].compareTo("Real")==0 : false;
				    boolean ajustado=(argumentos.length>5) ? Integer.parseInt(argumentos[5])==1 : false;
					CIngreso.getPronosticosRecurso(conn, recurso, año, mes, numero_datos, ajustado, fecha_real);
				}
				else if(cline.hasOption("ing_r_all")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos recurso (Todos los recursos)...");
					String[] argumentos = cline.getOptionValues("ing_r_all");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    boolean fecha_real=(argumentos.length>3) ? argumentos[3].compareTo("Real")==0 : false;
				    boolean ajustado=(argumentos.length>4) ? Integer.parseInt(argumentos[4])==1 : false;
				    CIngreso.getPronosticosRecursoAll(conn, año, mes, numero_datos, ajustado, fecha_real);
				}
				else if(cline.hasOption("ing")){
					CLogger.writeConsole("Inicio calculos pronosticos ingresos totales...");
					String[] argumentos = cline.getOptionValues("ing");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    boolean fecha_real=(argumentos.length>3) ? argumentos[3].compareTo("Real")==0 : false;
				    boolean ajustado=(argumentos.length>4) ? Integer.parseInt(argumentos[4])==1 : false;
					CIngreso.getPronosticosRecursosTotales(conn, año, mes, numero_datos, ajustado, fecha_real);
				}
				else if(cline.hasOption("egr")){
					CLogger.writeConsole("Inicio calculos pronosticos egresos totales...");
					String[] argumentos = cline.getOptionValues("egr");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    boolean ajustado=(argumentos.length>3) ? Integer.parseInt(argumentos[3])==1 : false;
				    boolean fecha_devengado=(argumentos.length>4) ? argumentos[4].compareTo("Devengado")==0 : false;
					CEgreso.getPronosticosEgresosEntidades(conn, año, mes, numero_datos, ajustado, fecha_devengado);
				}
				else if(cline.hasOption("egr_grupo")){
					CLogger.writeConsole("Inicio calculos pronosticos egresos agrupados...");
					String[] argumentos = cline.getOptionValues("egr_grupo");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
					boolean fecha_devengado=(argumentos.length>2) ? argumentos[2].compareTo("Devengado")==0 : false;
				    Integer numero_datos=(argumentos.length>3) ? Integer.parseInt(argumentos[3]) : 12;
				    boolean ajustado=(argumentos.length>4) ? Integer.parseInt(argumentos[4])==1 : false;
				    Integer entidad=(argumentos.length>5) ? Integer.parseInt(argumentos[5]) : null;
				    Integer unidad_ejecutora=(argumentos.length>6) ? Integer.parseInt(argumentos[6]) : null;
				    Integer programa=(argumentos.length>7) ? Integer.parseInt(argumentos[7]) : null;
				    Integer subprograma=(argumentos.length>8) ? Integer.parseInt(argumentos[8]) : null;
				    Integer proyecto=(argumentos.length>9) ? Integer.parseInt(argumentos[9]) : null;
				    Integer actividad=(argumentos.length>10) ? Integer.parseInt(argumentos[10]) : null;
				    Integer obra=(argumentos.length>11) ? Integer.parseInt(argumentos[11]) : null;
				    Integer fuente=(argumentos.length>12) ? Integer.parseInt(argumentos[12]) : null;
				    Integer renglon=(argumentos.length>13) ? Integer.parseInt(argumentos[13]) : null;
					CEgreso.getPronosticosEgresos(conn, año, mes, numero_datos, ajustado, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad,
							obra, fuente, renglon, fecha_devengado);
				}
				else if(cline.hasOption("egr_anticipos")){
					CLogger.writeConsole("Inicio calculos pronosticos egresos de anticipos contables totales...");
					String[] argumentos = cline.getOptionValues("egr_anticipos");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    boolean ajustado=(argumentos.length>3) ? Integer.parseInt(argumentos[3])==1 : false;
					CEgreso.getPronosticosEgresosAnticiposContables(conn, año, mes, numero_datos, ajustado);
				}
				else if(cline.hasOption("egr_sin_reg")){
					CLogger.writeConsole("Inicio calculos pronosticos egresos totales sin regularizaciones...");
					String[] argumentos = cline.getOptionValues("egr_sin_reg");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    boolean ajustado=(argumentos.length>3) ? Integer.parseInt(argumentos[3])==1 : false;
				    boolean fecha_devengado=(argumentos.length>4) ? argumentos[4].compareTo("Devengado")==0 : false;
					CEgreso.getPronosticosEgresosSinRegularizacionesEntidades(conn, año, mes, numero_datos, ajustado, fecha_devengado);
				}
				else if(cline.hasOption("egr_grupo_sin_reg")){
					CLogger.writeConsole("Inicio calculos pronosticos egresos agrupados sin regularizaciones...");
					String[] argumentos = cline.getOptionValues("egr_grupo_sin_reg");
					Integer año=(argumentos.length>0) ? Integer.parseInt(argumentos[0]) : start.getYear();
					Integer mes=(argumentos.length>1) ? Integer.parseInt(argumentos[1]) : start.getMonthOfYear();
				    Integer numero_datos=(argumentos.length>2) ? Integer.parseInt(argumentos[2]) : 12;
				    boolean ajustado=(argumentos.length>3) ? Integer.parseInt(argumentos[3])==1 : false;
				    Integer entidad=(argumentos.length>4) ? Integer.parseInt(argumentos[4]) : null;
				    Integer unidad_ejecutora=(argumentos.length>5) ? Integer.parseInt(argumentos[5]) : null;
				    Integer programa=(argumentos.length>6) ? Integer.parseInt(argumentos[6]) : null;
				    Integer subprograma=(argumentos.length>7) ? Integer.parseInt(argumentos[7]) : null;
				    Integer proyecto=(argumentos.length>8) ? Integer.parseInt(argumentos[8]) : null;
				    Integer actividad=(argumentos.length>9) ? Integer.parseInt(argumentos[9]) : null;
				    Integer obra=(argumentos.length>10) ? Integer.parseInt(argumentos[10]) : null;
				    Integer fuente=(argumentos.length>11) ? Integer.parseInt(argumentos[11]) : null;
				    Integer renglon=(argumentos.length>12) ? Integer.parseInt(argumentos[12]) : null;
				    boolean fecha_devengado=(argumentos.length>4) ? argumentos[4].compareTo("Devengado")==0 : false;
					CEgreso.getPronosticosEgresosSinRegularizaciones(conn, año, mes, numero_datos, ajustado, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad,
							obra, fuente, renglon, fecha_devengado);
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
