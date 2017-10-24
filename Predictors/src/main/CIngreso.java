package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.rosuda.REngine.Rserve.RConnection;

import pojo.IngresoRecursoAuxiliar;
import utilities.CLogger;
import utilities.CProperties;

public class CIngreso {
	
	public static ArrayList<IngresoRecursoAuxiliar> getIngresosRecursoAuxiliar(Connection conn, Integer recurso, Integer auxiliar, Integer ejercicio, Integer mes){
		ArrayList<IngresoRecursoAuxiliar> ret = new ArrayList<IngresoRecursoAuxiliar>();
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT recurso, recurso_nombre,"
					+ "auxiliar, auxiliar_nombre, ejercicio, m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12 "
					+ "FROM minfin.mv_ingreso_recurso_auxiliar WHERE recurso=? and auxiliar=? and ejercicio<=?");
			ps.setInt(1, recurso);
			ps.setInt(2, auxiliar);
			ps.setInt(3, ejercicio);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if((rs.getInt(5)==ejercicio && i<=mes)||rs.getInt(5)<ejercicio)
						montos.add(rs.getDouble(5+i));
				}
				IngresoRecursoAuxiliar temp = new IngresoRecursoAuxiliar(rs.getInt(5), rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getString(4), montos);
				ret.add(temp);
			}
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Ingresos. "+e.getMessage());
		}
		return ret;
	}
	
	public static ArrayList<IngresoRecursoAuxiliar> getIngresosRecurso(Connection conn, Integer recurso, Integer ejercicio, Integer mes){
		ArrayList<IngresoRecursoAuxiliar> ret = new ArrayList<IngresoRecursoAuxiliar>();
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT recurso, recurso_nombre, ejercicio, "
					+ "m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12 "
					+ "from minfin.mv_ingreso_recurso "
					+ "where recurso = ? and ejercicio<=?");
			ps.setInt(1, recurso);
			ps.setInt(2, ejercicio);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if((rs.getInt(3)==ejercicio && i<=mes)||rs.getInt(3)<ejercicio)
						montos.add(rs.getDouble(3+i));
				}
				IngresoRecursoAuxiliar temp = new IngresoRecursoAuxiliar(rs.getInt(3), rs.getInt(1), rs.getString(2), 0, null, montos);
				ret.add(temp);
			}
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Ingresos. "+e.getMessage());
		}
		return ret;
	}
	
	public static ArrayList<IngresoRecursoAuxiliar> getIngresosRecursosTotales(Connection conn, Integer ejercicio, Integer mes){
		ArrayList<IngresoRecursoAuxiliar> ret = new ArrayList<IngresoRecursoAuxiliar>();
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT ejercicio, "
					+ "sum(m1) m1, "
					+ "sum(m2) m2, "
					+ "sum(m3) m3, "
					+ "sum(m4) m4, "
					+ "sum(m5) m5, "
					+ "sum(m6) m6, "
					+ "sum(m7) m7, "
					+ "sum(m8) m8, "
					+ "sum(m9) m9, "
					+ "sum(m10) m10, "
					+ "sum(m11) m11, "
					+ "sum(m12) m12 "
					+ "from minfin.mv_ingreso_recurso "
					+ "where ejercicio<=? "
					+ "group by ejercicio ");
			ps.setInt(1, ejercicio);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if((rs.getInt(1)==ejercicio && i<=mes)||rs.getInt(1)<ejercicio)
						montos.add(rs.getDouble(1+i));
				}
				IngresoRecursoAuxiliar temp = new IngresoRecursoAuxiliar(rs.getInt(1), 0, null, 0, null, montos);
				ret.add(temp);
			}
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Ingresos Totales "+e.getMessage());
		}
		return ret;
	}
	
	private static String getVectorAplanado(ArrayList<IngresoRecursoAuxiliar> datos){
		String ret="";
		if(datos!=null && datos.size()>0){
			for(IngresoRecursoAuxiliar dato:datos){
				for(int i=0; i<dato.getMontos().size(); i++)
					ret = ret + ", " + String.format("%.2f",dato.getMontos().get(i));
			}
		}
		return ret!=null && ret.length()>0 ? ret.substring(1) : "";
	} 
	
	public static ArrayList<Double> getPronosticosRecursoAuxiliar(Connection conn,Integer recurso, Integer auxiliar, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			ArrayList<IngresoRecursoAuxiliar> historicos = new ArrayList<IngresoRecursoAuxiliar>();
			historicos = getIngresosRecursoAuxiliar(conn,recurso,auxiliar, ejercicio, mes);
			//Rengine.DEBUG = 5;
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			
			engine.eval("library(forecast)");
			String vector_aplanado = "c("+getVectorAplanado(historicos)+")";
			engine.eval("datos = " + vector_aplanado);
			engine.eval("serie = ts(datos, start=c(2011,1), frequency=12)");
			if(ajustado){
				engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
				engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
			}
			else{
				engine.eval("fc = forecast(srie,"+numero_pronosticos+")");
			}	
			engine.eval("resultados=as.numeric(fc$mean)");
			double[] res=engine.eval("resultados").asDoubles();
			
			DateTime inicio = new DateTime(ejercicio,mes-1,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_ingreso_recurso_auxiliar "
					+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and recurso=? and auxiliar=? and fuente=0");
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, ejercicio);
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.setInt(7, recurso);
			ps.setInt(8, auxiliar);
			ps.executeUpdate();
			ps.close();
			ps = conn.prepareStatement("INSERT INTO minfin.mvp_ingreso_recurso_auxiliar(ejercicio, mes, recurso, auxiliar, fuente, monto) "
					+ "values(?,?,?,?,0,?)");
			
			for(double dato: res){
				ret.add(dato);
				DateTime tiempo = inicio.plusMonths(ret.size());
				ps.setInt(1, tiempo.getYear());
				ps.setInt(2, tiempo.getMonthOfYear());
				ps.setInt(3, recurso);
				ps.setInt(4, auxiliar);
				ps.setDouble(5, dato);
				ps.addBatch();
			}
			ps.executeBatch();
			ps.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de ingreso recurso - auxiliar");
		}
		return ret;
	}
	
	public static ArrayList<Double> getPronosticosRecurso(Connection conn,Integer recurso, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			ArrayList<IngresoRecursoAuxiliar> historicos = new ArrayList<IngresoRecursoAuxiliar>();
			historicos = getIngresosRecurso(conn,recurso, ejercicio, mes);
			//Rengine.DEBUG = 5;
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			
			engine.eval("library(forecast)");
			String vector_aplanado = "c("+getVectorAplanado(historicos)+")";
			engine.eval("datos = " + vector_aplanado);
			engine.eval("serie = ts(datos, start=c(2011,1), frequency=12)");
			if(ajustado){
				engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
				engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
			}
			else{
				engine.eval("fc = forecast(srie,"+numero_pronosticos+")");
			}	
			engine.eval("resultados=as.numeric(fc$mean)");
			double[] res=engine.eval("resultados").asDoubles();
			
			DateTime inicio = new DateTime(ejercicio,mes-1,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_ingreso_recurso_auxiliar "
					+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and recurso=? and auxiliar=0 and fuente=0");
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, ejercicio);
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.setInt(7, recurso);
			ps.executeUpdate();
			ps.close();
			ps = conn.prepareStatement("INSERT INTO minfin.mvp_ingreso_recurso_auxiliar(ejercicio, mes, recurso, auxiliar, fuente, monto) "
					+ "values(?,?,?,0,0,?)");
			
			for(double dato: res){
				ret.add(dato);
				DateTime tiempo = inicio.plusMonths(ret.size());
				ps.setInt(1, tiempo.getYear());
				ps.setInt(2, tiempo.getMonthOfYear());
				ps.setInt(3, recurso);
				ps.setDouble(4, dato);
				ps.addBatch();
			}
			ps.executeBatch();
			ps.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de ingreso recurso. ["+e.getMessage()+"]");
		}
		return ret;
	}
	
	public static ArrayList<Double> getPronosticosRecursosTotales(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			ArrayList<IngresoRecursoAuxiliar> historicos = new ArrayList<IngresoRecursoAuxiliar>();
			historicos = getIngresosRecursosTotales(conn, ejercicio, mes);
			//Rengine.DEBUG = 5;
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			
			
			engine.eval("library(forecast)");
			String vector_aplanado = "c("+getVectorAplanado(historicos)+")";
			engine.eval("datos = " + vector_aplanado);
			engine.eval("serie = ts(datos, start=c(2011,1), frequency=12)");
			if(ajustado){
				engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
				engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
			}
			else{
				engine.eval("fc = forecast(srie,"+numero_pronosticos+")");
			}	
			engine.eval("resultados=as.numeric(fc$mean)");
			double[] res=engine.eval("resultados").asDoubles();
			
			DateTime inicio = new DateTime(ejercicio,mes-1,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_ingreso_recurso_auxiliar "
					+ "WHERE ((ejercicio=? and mes>?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and recurso=0 and auxiliar=0 and fuente=0");
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, ejercicio);
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.executeUpdate();
			ps.close();
			ps = conn.prepareStatement("INSERT INTO minfin.mvp_ingreso_recurso_auxiliar(ejercicio, mes, recurso, auxiliar, fuente, monto) "
					+ "values(?,?,0,0,0,?)");
			for(double dato: res){
				ret.add(dato);
				DateTime tiempo = inicio.plusMonths(ret.size());
				ps.setInt(1, tiempo.getYear());
				ps.setInt(2, tiempo.getMonthOfYear());
				ps.setDouble(3, dato);
				ps.addBatch();
			}
			ps.executeBatch();
			ps.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de ingreso recursos totales. ["+e.getMessage()+"]");
		}
		return ret;
	}
}
