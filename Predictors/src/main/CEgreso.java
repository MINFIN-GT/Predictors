package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.rosuda.REngine.Rserve.RConnection;

import pojo.EgresoGasto;
import utilities.CLogger;
import utilities.CProperties;

public class CEgreso {
	
	private static String getVectorAplanado(ArrayList<EgresoGasto> datos){
		String ret="";
		if(datos!=null && datos.size()>0){
			for(EgresoGasto dato:datos){
				for(int i=0; i<dato.getMontos().size(); i++)
					ret = String.join(", ", ret,dato.getMontos().get(i).toString());
					//ret = ret + ", " + dato.getMontos().get(i);
			}
		}
		return ret!=null && ret.length()>0 ? ret.substring(1) : "";
	} 
	
	public static ArrayList<EgresoGasto> getEgresosEntidad(Connection conn, Integer ejercicio, Integer mes, Integer entidad, boolean fecha_devengado){
		ArrayList<EgresoGasto> ret = new ArrayList<EgresoGasto>();
		DateTime now = DateTime.now();
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT ejercicio, entidad, "
					+ "round(sum(m1),2) m1, "
					+ "round(sum(m2),2) m2, "
					+ "round(sum(m3),2) m3, "
					+ "round(sum(m4),2) m4, "
					+ "round(sum(m5),2) m5, "
					+ "round(sum(m6),2) m6, "
					+ "round(sum(m7),2) m7, "
					+ "round(sum(m8),2) m8, "
					+ "round(sum(m9),2) m9, "
					+ "round(sum(m10),2) m10, "
					+ "round(sum(m11),2) m11, "
					+ "round(sum(m12),2) m12 "
					+ "from minfin.mv_ejecucion_presupuestaria_mensualizada" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " "
					+ " "
					+ "where ejercicio between  ? and ? and entidad=? "
					+ "group by ejercicio ORDER BY ejercicio, entidad");
			ps.setInt(1, ejercicio-5);
			ps.setInt(2, ejercicio);
			ps.setInt(3, entidad);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if(((rs.getInt(1)==ejercicio && i<mes) || rs.getInt(1)<ejercicio) && ((rs.getInt(1)==now.getYear() && i<now.getMonthOfYear()) || rs.getInt(1)<now.getYear()))
						montos.add(rs.getDouble(2+i));
				}
				EgresoGasto temp = new EgresoGasto(rs.getInt(1), entidad, 0, 0,0,0,0,0,0,0, null, null,null,null,null,null,null,null,null, montos);
				ret.add(temp);
			}
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Egresos Totales "+e.getMessage());
		}
		return ret;
	}
	
public static ArrayList<Double> getPronosticosEgresosEntidades(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado, boolean fecha_devengado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			ArrayList<ArrayList<EgresoGasto>> historicos = new ArrayList<ArrayList<EgresoGasto>>();
			historicos = getEgresosEntidades(conn, ejercicio, mes, fecha_devengado);
			DateTime inicio = new DateTime(ejercicio,mes,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_egreso" + 
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " "  
					+ " WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and unidad_ejecutora=0 "
					+ " and programa=0 and subprograma=0 and proyecto=0 and actividad=0 and obra=0 and fuente=0 and ajustado=?");
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, inicio.getYear());
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.setInt(7, ajustado ? 1 : 0);
			ps.executeUpdate();
			ps.close();
			for(ArrayList<EgresoGasto> entidad:historicos){
				CLogger.writeConsole("Calculando pronostico para la entidad "+entidad.get(0).getEntidad());
				
				int ts_año_inicio = entidad.get(0).getEjercicio();
				
				//Rengine.DEBUG = 5;
				engine.eval("suppressPackageStartupMessages(library(forecast))");
				engine.eval("datos = c("+getVectorAplanado(entidad)+")");
				engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
				if(ajustado){
					engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
					engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
				}
				else{
					engine.eval("fc = forecast(ets(serie),"+numero_pronosticos+")");
				}	
				engine.eval("resultados=as.numeric(fc$mean)");
				double[] res_ets=engine.eval("resultados").asDoubles();
				engine.eval("error=accuracy(fc)[5]");
				double error_ets = engine.eval("error").asDouble();
				
				engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
				engine.eval("fc = forecast(auto.arima(serie),"+numero_pronosticos+")");
				engine.eval("resultados=as.numeric(fc$mean)");
				double[] res_arima=engine.eval("resultados").asDoubles();
				engine.eval("error=accuracy(fc)[5]");
				double error_arima = engine.eval("error").asDouble();
				
				ps = conn.prepareStatement("INSERT INTO minfin.mvp_egreso"+
						(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " 
						+"(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, "
						+ "proyecto, actividad, obra, fuente,renglon, monto,modelo, error_modelo, ajustado, fecha_calculo) "
						+ "values(?,?,?,0,0,0,0,0,0,0,0,?,?,?,?,?)");
				double error=0;
				int i=0;
				for(double dato: (error_ets<=error_arima ? res_ets : res_arima)){
					ret.add(dato);
					DateTime tiempo = inicio.plusMonths(i);
					ps.setInt(1, tiempo.getYear());
					ps.setInt(2, tiempo.getMonthOfYear());
					ps.setInt(3, entidad.get(0).getEntidad());
					ps.setDouble(4, dato);
					ps.setString(5, (error_ets<=error_arima ? "ETS" : "ARIMA"));
					error = error_ets<=error_arima ? error_ets : error_arima;
					if(!Double.isNaN(error) && !Double.isInfinite(error))
						ps.setDouble(6,  error);
					else
						ps.setNull(6, java.sql.Types.DECIMAL);
					ps.setInt(7, ajustado ? 1 : 0);
					ps.setTimestamp(8, new Timestamp(DateTime.now().getMillis()));
					ps.addBatch();
					i++;
				}
				
			}
			ps.executeBatch();
			ps.close();
			engine.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de egresos totales");
			e.printStackTrace(System.out);
		}
		return ret;
	}
	
	public static ArrayList<ArrayList<EgresoGasto>> getEgresos(Connection conn, Integer ejercicio, Integer mes, Integer entidad, Integer unidad_ejecutora,
			Integer programa, Integer subprograma, Integer proyecto, Integer actividad, Integer obra, Integer fuente, Integer renglon, boolean fecha_devengado){
		ArrayList<ArrayList<EgresoGasto>> ret = new ArrayList<ArrayList<EgresoGasto>>();
		DateTime now = DateTime.now();
		try{
		PreparedStatement ps = conn.prepareStatement("SELECT ejercicio, "
					+ ((entidad!=null) ? "entidad, " : "")
					+ ((unidad_ejecutora!=null) ? "unidad_ejecutora, " : "")
					+ ((programa!=null) ? "programa, " : "")
					+ ((subprograma!=null) ? "subprograma, " : "")
					+ ((proyecto!=null) ? "proyecto, " : "")
					+ ((actividad!=null) ? "actividad, " : "")
					+ ((obra!=null) ? "obra, " : "")
					+ ((fuente!=null) ? "fuente, " : "")
					+ ((renglon!=null) ? "renglon, " : "")
					+ "round(sum(m1),2) m1, "
					+ "round(sum(m2),2) m2, "
					+ "round(sum(m3),2) m3, "
					+ "round(sum(m4),2) m4, "
					+ "round(sum(m5),2) m5, "
					+ "round(sum(m6),2) m6, "
					+ "round(sum(m7),2) m7, "
					+ "round(sum(m8),2) m8, "
					+ "round(sum(m9),2) m9, "
					+ "round(sum(m10),2) m10, "
					+ "round(sum(m11),2) m11, "
					+ "round(sum(m12),2) m12 "
					+ "from minfin.mv_ejecucion_presupuestaria_mensualizada" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " 
					+ "where ejercicio<= ? "
					+ "group by "
					+ ((entidad!=null) ? "entidad, " : "")
					+ ((unidad_ejecutora!=null) ? "unidad_ejecutora, " : "")
					+ ((programa!=null) ? "programa, " : "")
					+ ((subprograma!=null) ? "subprograma, " : "")
					+ ((proyecto!=null) ? "proyecto, " : "")
					+ ((actividad!=null) ? "actividad, " : "")
					+ ((obra!=null) ? "obra, " : "")
					+ ((fuente!=null) ? "fuente, " : "")
					+ ((renglon!=null) ? "renglon, " : "")
					+ "ejercicio "
					+ "order by "
					+ ((entidad!=null) ? "entidad, " : "")
					+ ((unidad_ejecutora!=null) ? "unidad_ejecutora, " : "")
					+ ((programa!=null) ? "programa, " : "")
					+ ((subprograma!=null) ? "subprograma, " : "")
					+ ((proyecto!=null) ? "proyecto, " : "")
					+ ((actividad!=null) ? "actividad, " : "")
					+ ((obra!=null) ? "obra, " : "")
					+ ((fuente!=null) ? "fuente, " : "")
					+ ((renglon!=null) ? "renglon, " : "")
					+ " ejercicio"
					);
			ps.setInt(1, ejercicio);
			ResultSet rs = ps.executeQuery();
			Integer t_entidad=-1;
			Integer t_unidad_ejecutora=-1;
			Integer t_programa = -1;
			Integer t_subprograma = -1;
			Integer t_proyecto = -1;
			Integer t_actividad = -1;
			Integer t_obra = -1;
			Integer t_fuente = -1;
			Integer t_renglon = -1;
			ArrayList<EgresoGasto> dato=null;
			int inicio_sumas = 0;
			inicio_sumas = entidad!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = unidad_ejecutora!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = programa!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = subprograma!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = proyecto!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = actividad!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = obra!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = fuente!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = renglon!=null ?  inicio_sumas+1 : inicio_sumas;
			while(rs.next()){
				Integer r_entidad = rs.getInt("entidad");
				Integer r_unidad_ejecutora = (unidad_ejecutora!=null) ? rs.getInt("unidad_ejecutora") : -1;
				Integer r_programa = (programa!=null) ? rs.getInt("programa") : -1;
				Integer r_subprograma = (subprograma!=null) ? rs.getInt("subprograma") : -1;
				Integer r_proyecto = (proyecto!=null) ? rs.getInt("proyecto") : -1;
				Integer r_actividad = (actividad!=null) ? rs.getInt("actividad") : -1;
				Integer r_obra = (obra!=null) ? rs.getInt("obra") : -1;
				Integer r_fuente = (fuente!=null) ? rs.getInt("fuente") : -1;
				Integer r_renglon = (renglon!=null) ? rs.getInt("renglon") : -1;
				if(!t_entidad.equals(r_entidad) ||
						!t_unidad_ejecutora.equals(r_unidad_ejecutora) ||
						!t_programa.equals(r_programa) || 
						!t_subprograma.equals(r_subprograma) ||
						!t_proyecto.equals(r_proyecto) ||
						!t_actividad.equals(r_actividad) ||
						!t_obra.equals(r_obra) ||
						!t_fuente.equals(r_fuente) ||
						!t_renglon.equals(r_renglon)
						){
					if(dato!=null){
						ret.add(dato);
					}
					dato = new ArrayList<EgresoGasto>();
					t_entidad = r_entidad;
					t_unidad_ejecutora = r_unidad_ejecutora;
					t_programa = r_programa;
					t_subprograma = r_subprograma;
					t_proyecto = r_proyecto;
					t_actividad = r_actividad;
					t_obra = r_obra;
					t_fuente = r_fuente;
					t_renglon = r_renglon;
				}
					
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if(((rs.getInt(1)==ejercicio && i<mes) || rs.getInt(1)<ejercicio) && ((rs.getInt(1)==now.getYear() && i<now.getMonthOfYear()) || rs.getInt(1)<now.getYear()))
						montos.add(rs.getDouble(inicio_sumas+i+1));
				}
				EgresoGasto temp = new EgresoGasto(rs.getInt(1), entidad!=null ? rs.getInt("entidad") : 0, 
						unidad_ejecutora!=null ? rs.getInt("unidad_ejecutora") : 0, 
						programa!=null ? rs.getInt("programa"): 0,
						subprograma!=null ? rs.getInt("subprograma") : 0,
						proyecto !=null ? rs.getInt("proyecto") : 0,
						actividad!=null ? rs.getInt("actividad") : 0,
						obra!=null ? rs.getInt("obra") : 0,
						fuente!=null ? rs.getInt("fuente") : 0,
						renglon!=null ? rs.getInt("renglon") : 0,
						null, null, null, null,null, null, null, null, null, montos);
				dato.add(temp);
			}
			if(dato!=null)
				ret.add(dato);
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Egresos (Agrupados) "+e.getMessage());
		}
		return ret;
	}
	
	public static ArrayList<Double> getPronosticosEgresos(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado,
			Integer entidad, Integer unidad_ejecutora, Integer programa, Integer subprograma, Integer proyecto, Integer actividad, Integer obra,
			Integer fuente, Integer renglon, boolean fecha_devengado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			ArrayList<ArrayList<EgresoGasto>> historicos = new ArrayList<ArrayList<EgresoGasto>>();
			historicos = getEgresos(conn, ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, renglon, fecha_devengado);
			
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_egreso" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " 
					+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) "
					+ ((entidad!=null && entidad>100) ? " and entidad="+entidad : " and entidad>0 ")  
					+ ((unidad_ejecutora!=null && unidad_ejecutora>1) ? " and unidad_ejecutora="+unidad_ejecutora : "")
					+ ((programa!=null && programa>0) ? " and programa="+programa : " ")  
					+ ((subprograma!=null && subprograma>0) ? " and subprograma="+subprograma : " ")  
					+ ((proyecto!=null && proyecto>0) ? " and proyecto="+proyecto : " ")  
					+ ((actividad!=null && actividad>0) ? " and actividad="+actividad : " ")  
					+ ((obra!=null && obra>0) ? " and obra="+obra : " ")  
					+ ((fuente!=null && fuente>0) ? " and fuente="+fuente : " ")  
					+ ((renglon!=null && renglon>0) ? " and renglon="+renglon : " ")  
					+ " and ajustado=?");
			DateTime inicio = new DateTime(ejercicio,mes,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, inicio.getYear());
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.setInt(7, ajustado ? 1 : 0);
			ps.executeUpdate();
			PreparedStatement psi= conn.prepareStatement("INSERT INTO minfin.mvp_egreso"+
					(fecha_devengado==false ? "_fecha_pagado_total" : "") 
					+"(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, "
					+ "proyecto, actividad, obra, fuente,renglon, monto,modelo, error_modelo, ajustado, fecha_calculo) "
					+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			int rows = 0;
			for(ArrayList<EgresoGasto> historico : historicos){
				CLogger.writeConsole("Calculando pronostico ejercicio = "+ejercicio+" mes = "+mes
						+" entidad = "+(historico.get(0).getEntidad()!=null ? historico.get(0).getEntidad() : 0)
						+" unidad_ejecutora = "+(historico.get(0).getUnidad_ejecutora()!=null ? historico.get(0).getUnidad_ejecutora() : 0)
						+" programa = "+(historico.get(0).getPrograma()!=null ? historico.get(0).getPrograma() : 0)
						+" subprograma = "+(historico.get(0).getSubprograma()!=null ? historico.get(0).getSubprograma() : 0)
						+" proyecto = "+(historico.get(0).getProyecto()!=null ? historico.get(0).getProyecto() : 0)
						+" actividad = "+(historico.get(0).getActividad()!=null ? historico.get(0).getActividad() : 0)
						+" obra = "+(historico.get(0).getObra()!=null ? historico.get(0).getObra() : 0)
						+" fuente = "+(historico.get(0).getFuente()!=null ? historico.get(0).getFuente() : 0)
						+" renglon = "+(historico.get(0).getRenglon()!=null ? historico.get(0).getRenglon() : 0)
					);
				int ts_año_inicio = historico.get(0).getEjercicio();
				
				//Rengine.DEBUG = 5;
				
				engine.eval("suppressPackageStartupMessages(library(forecast))");
				String vector_aplanado = getVectorAplanado(historico);
				if(vector_aplanado.length()>0){
					vector_aplanado = "c("+vector_aplanado+")";
					engine.eval("datos = " + vector_aplanado);
					engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
					if(ajustado){
						engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
						engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
					}
					else{
						engine.eval("fc = forecast(ets(serie),"+numero_pronosticos+")");
					}	
					engine.eval("resultados=as.numeric(fc$mean)");
					double[] res_ets=engine.eval("resultados").asDoubles();
					engine.eval("error=accuracy(fc)[5]");
					double error_ets = engine.eval("error").asDouble();
					
					engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
					engine.eval("fc = forecast(auto.arima(serie),"+numero_pronosticos+")");
					engine.eval("resultados=as.numeric(fc$mean)");
					double[] res_arima=engine.eval("resultados").asDoubles();
					engine.eval("error=accuracy(fc)[5]");
					double error_arima = engine.eval("error").asDouble();
					
					double error=0;
					int i=0;
					for(double dato: (error_ets<=error_arima ? res_ets : res_arima)){
						ret.add(dato);
						DateTime tiempo = inicio.plusMonths(i);
						psi.setInt(1, tiempo.getYear());
						psi.setInt(2, tiempo.getMonthOfYear());
						psi.setInt(3, historico.get(0).getEntidad()!=null ? historico.get(0).getEntidad() : 0);
						psi.setInt(4, historico.get(0).getUnidad_ejecutora()!=null ? historico.get(0).getUnidad_ejecutora() : 0);
						psi.setInt(5, historico.get(0).getPrograma()!=null ? historico.get(0).getPrograma() : 0);
						psi.setInt(6, historico.get(0).getSubprograma()!=null ? historico.get(0).getSubprograma() : 0);
						psi.setInt(7, historico.get(0).getProyecto()!=null ? historico.get(0).getProyecto() : 0);
						psi.setInt(8, historico.get(0).getActividad()!=null ? historico.get(0).getActividad() : 0);
						psi.setInt(9, historico.get(0).getObra()!=null ? historico.get(0).getObra() : 0);
						psi.setInt(10, historico.get(0).getFuente()!=null ? historico.get(0).getFuente() : 0);
						psi.setInt(11, historico.get(0).getRenglon()!=null ? historico.get(0).getRenglon() : 0);
						psi.setDouble(12, dato);
						psi.setString(13, (error_ets<=error_arima ? "ETS" : "ARIMA"));
						error = error_ets<=error_arima ? error_ets : error_arima;
						if(!Double.isNaN(error) && !Double.isInfinite(error))
							psi.setDouble(14,  error);
						else
							psi.setNull(14, java.sql.Types.DECIMAL);
						psi.setInt(15, ajustado ? 1 : 0);
						psi.setTimestamp(16, new Timestamp(DateTime.now().getMillis()));
						psi.addBatch();
						i++;
						rows++;
						if(rows%10000==0)
							psi.executeBatch();
					}
				}
			}
			ps.close();
			engine.close();
			psi.executeBatch();
			psi.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de egresos");
			e.printStackTrace(System.out);
		}
		return ret;
	}
	
	public static ArrayList<ArrayList<EgresoGasto>> getEgresosContables(Connection conn, Integer ejercicio, Integer mes){
		ArrayList<ArrayList<EgresoGasto>> ret = new ArrayList<ArrayList<EgresoGasto>>();
		DateTime now = DateTime.now();
		try{
			PreparedStatement ps = conn.prepareStatement("select * from mv_anticipo_contable where (ejercicio between ? and ?) "
					+ "and clase_registro IN ('EIA','EIAP','EIC','EICO','EID','EIE','EIF','EIP','EIR','FRA','FRC','FRR', 'NDB') "
					+ "order by clase_registro, ejercicio, mes");
			ps.setInt(1, ejercicio-5);
			ps.setInt(2, ejercicio);
			ResultSet rs = ps.executeQuery();
			int ejercicio_actual=0;
			ArrayList<Double> montos=new ArrayList<Double>();
			ArrayList<EgresoGasto> atemp = null;
			String clase_registro_actual = "";
			while(rs.next()){
				if(clase_registro_actual.compareTo(rs.getString("clase_registro"))!=0) {
					EgresoGasto temp1 = new EgresoGasto(ejercicio, 0, 0, 0,0,0,0,0,0,0, rs.getString("clase_registro"), null,null,null,null,null,null,null,null, montos);
					atemp.add(temp1);
					if(atemp!=null)
						ret.add(atemp);
					atemp = new ArrayList<EgresoGasto>();
				}
				if(((rs.getInt("ejercicio")==ejercicio && rs.getInt("mes")<mes) || rs.getInt("ejercicio")<ejercicio) 
						&& ((rs.getInt("ejercicio")==now.getYear() && rs.getInt("mes")<now.getMonthOfYear()) || rs.getInt("ejercicio")<now.getYear()))
						montos.add(rs.getDouble("monto"));
				if(ejercicio_actual>0 && rs.getInt("ejercicio")!=ejercicio_actual) {
					EgresoGasto temp = new EgresoGasto(rs.getInt("ejercicio"), 0, 0, 0,0,0,0,0,0,0, rs.getString("clase_registro"), null,null,null,null,null,null,null,null, montos);
					atemp.add(temp);
					montos = new ArrayList<Double>();
				}
				else {
					ejercicio_actual = rs.getInt("ejercicio"); 
				}
			}
			EgresoGasto temp1 = new EgresoGasto(ejercicio, 0, 0, 0,0,0,0,0,0,0, rs.getString("clase_registro"), null,null,null,null,null,null,null,null, montos);
			atemp.add(temp1);
			if(atemp!=null)
				ret.add(atemp);
			rs.close();
			ps.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Egresos Contables de anticipos "+e.getMessage());
		}
		return ret;
	}
	
	public static ArrayList<Double> getPronosticosEgresosAnticiposContables(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			//String[] clase_registros =  {"EIA","EIAP","EIC","EICO","EID","EIE","EIF","EIP","EIR","FRA","FRC","FRR", "NDB"};
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			
			DateTime inicio = new DateTime(ejercicio,mes,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_anticipo_contable "
					+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and ajustado=?");
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, inicio.getYear());
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.setInt(7, ajustado ? 1 : 0);
			ps.executeUpdate();
			ps.close();
			ArrayList<ArrayList<EgresoGasto>> historicos = new ArrayList<ArrayList<EgresoGasto>>();
			historicos = getEgresosContables(conn, ejercicio, mes);
			ps = conn.prepareStatement("INSERT INTO minfin.mvp_anticipo_contable(ejercicio, mes, clase_registro, monto,modelo, error_modelo, ajustado, fecha_calculo) "
					+ "values(?,?,?,?,?,?,?,?)");
			for(ArrayList<EgresoGasto> clase : historicos){
				CLogger.writeConsole("Calculando pronostico para la clase registro "+clase.get(0).getEntidad_nombre());
				
				
				
				int ts_año_inicio = clase.get(0).getEjercicio();
				
				//Rengine.DEBUG = 5;
				engine.eval("suppressPackageStartupMessages(library(forecast))");
				String vector_aplanado = "c("+getVectorAplanado(clase)+")";
				engine.eval("datos = " + vector_aplanado);
				engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
				if(ajustado){
					engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
					engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
				}
				else{
					engine.eval("fc = forecast(ets(serie),"+numero_pronosticos+")");
				}	
				engine.eval("resultados=as.numeric(fc$mean)");
				double[] res_ets=engine.eval("resultados").asDoubles();
				engine.eval("error=accuracy(fc)[5]");
				double error_ets = engine.eval("error").asDouble();
				
				engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
				engine.eval("fc = forecast(auto.arima(serie),"+numero_pronosticos+")");
				engine.eval("resultados=as.numeric(fc$mean)");
				double[] res_arima=engine.eval("resultados").asDoubles();
				engine.eval("error=accuracy(fc)[5]");
				double error_arima = engine.eval("error").asDouble();
				
				
				double error=0;
				double dato;
				for(int k=0; k<numero_pronosticos; k++) {
					dato = (error_ets<=error_arima ? res_ets[k] : res_arima[k]);
					ret.add(dato);
					DateTime tiempo = inicio.plusMonths(k);
					ps.setInt(1, tiempo.getYear());
					ps.setInt(2, tiempo.getMonthOfYear());
					ps.setString(3, clase.get(0).getEntidad_nombre());
					ps.setDouble(4, dato);
					ps.setString(5, (error_ets<=error_arima ? "ETS" : "ARIMA"));
					error = error_ets<=error_arima ? error_ets : error_arima;
					if(!Double.isNaN(error) && !Double.isInfinite(error))
						ps.setDouble(6,  error);
					else
						ps.setNull(6, java.sql.Types.DECIMAL);
					ps.setInt(7, ajustado ? 1 : 0);
					ps.setTimestamp(8, new Timestamp(DateTime.now().getMillis()));
					ps.addBatch();
				}
			}
			engine.close();
			ps.executeBatch();
			ps.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de egresos contables anticipos");
			e.printStackTrace(System.out);
		}
		return ret;
	}
	
	public static ArrayList<EgresoGasto> getEgresosSinRegularizacionesEntidad(Connection conn, Integer ejercicio, Integer mes, Integer entidad,
			boolean fecha_devengado){
		ArrayList<EgresoGasto> ret = new ArrayList<EgresoGasto>();
		DateTime now = DateTime.now();
		try{
			PreparedStatement ps = conn.prepareStatement("select ejercicio, entidad,  " + 
					"round(sum(case when mes = 1 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m1, " + 
					"round(sum(case when mes = 2 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m2, " + 
					"round(sum(case when mes = 3 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m3, " + 
					"round(sum(case when mes = 4 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m4, " + 
					"round(sum(case when mes = 5 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m5, " + 
					"round(sum(case when mes = 6 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m6, " + 
					"round(sum(case when mes = 7 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m7, " + 
					"round(sum(case when mes = 8 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m8, " + 
					"round(sum(case when mes = 9 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m9, " + 
					"round(sum(case when mes = 10 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m10, " + 
					"round(sum(case when mes = 11 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m11, " + 
					"round(sum(case when mes = 12 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m12 " + 
					"from minfin.mv_gasto_sin_regularizaciones" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " +
					"where ejercicio between ? and ? and entidad = ? " + 
					"group by ejercicio,entidad ORDER BY ejercicio, entidad");
			ps.setInt(1, ejercicio-5);
			ps.setInt(2, ejercicio);
			ps.setInt(3, entidad);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if(((rs.getInt(1)==ejercicio && i<mes) || rs.getInt(1)<ejercicio) && ((rs.getInt(1)==now.getYear() && i<now.getMonthOfYear()) || rs.getInt(1)<now.getYear()))
						montos.add(rs.getDouble(2+i));
				}
				EgresoGasto temp = new EgresoGasto(rs.getInt(1), entidad, 0, 0,0,0,0,0,0,0, null, null,null,null,null,null,null,null,null, montos);
				ret.add(temp);
			}
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Egresos Totales "+e.getMessage());
		}
		return ret;
	}
	
	public static ArrayList<Double> getPronosticosEgresosSinRegularizacionesEntidades(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado,
			boolean fecha_devengado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			ArrayList<ArrayList<EgresoGasto>> historicos = new ArrayList<ArrayList<EgresoGasto>>();
			historicos = getEgresosSinRegularizacionesEntidades(conn, ejercicio, mes, fecha_devengado);
			DateTime inicio = new DateTime(ejercicio,mes,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_egreso_sin_regularizaciones" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " 
					+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and unidad_ejecutora=0 "
					+ "and programa=0 and subprograma=0 and proyecto=0 and actividad=0 and obra=0 and fuente=0 and ajustado=?");
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, inicio.getYear());
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.setInt(7, ajustado ? 1 : 0);
			ps.executeUpdate();
			ps.close();
			ps = conn.prepareStatement("INSERT INTO minfin.mvp_egreso_sin_regularizaciones" + 
					(fecha_devengado==false ? "_fecha_pagado_total" : "") 
					+ "(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, "
					+ "proyecto, actividad, obra, fuente,renglon, monto,modelo, error_modelo, ajustado, fecha_calculo) "
					+ "values(?,?,?,0,0,0,0,0,0,0,0,?,?,?,?,?)");
			for(ArrayList<EgresoGasto> entidad: historicos){
				CLogger.writeConsole("Calculando pronostico para la entidad "+entidad.get(0).getEntidad());
				
				int ts_año_inicio = entidad.get(0).getEjercicio();
				
				//Rengine.DEBUG = 5;
				engine.eval("suppressPackageStartupMessages(library(forecast))");
				String vector_aplanado = "c("+getVectorAplanado(entidad)+")";
				engine.eval("datos = " + vector_aplanado);
				engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
				if(ajustado){
					engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
					engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
				}
				else{
					engine.eval("fc = forecast(ets(serie),"+numero_pronosticos+")");
				}	
				engine.eval("resultados=as.numeric(fc$mean)");
				double[] res_ets=engine.eval("resultados").asDoubles();
				engine.eval("error=accuracy(fc)[5]");
				double error_ets = engine.eval("error").asDouble();
				
				engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
				engine.eval("fc = forecast(auto.arima(serie),"+numero_pronosticos+")");
				engine.eval("resultados=as.numeric(fc$mean)");
				double[] res_arima=engine.eval("resultados").asDoubles();
				engine.eval("error=accuracy(fc)[5]");
				double error_arima = engine.eval("error").asDouble();
				
				double error=0;
				int i=0;
				for(double dato: (error_ets<=error_arima ? res_ets : res_arima)){
					ret.add(dato);
					DateTime tiempo = inicio.plusMonths(i);
					ps.setInt(1, tiempo.getYear());
					ps.setInt(2, tiempo.getMonthOfYear());
					ps.setInt(3, entidad.get(0).getEntidad());
					ps.setDouble(4, dato);
					ps.setString(5, (error_ets<=error_arima ? "ETS" : "ARIMA"));
					error = error_ets<=error_arima ? error_ets : error_arima;
					if(!Double.isNaN(error) && !Double.isInfinite(error))
						ps.setDouble(6,  error);
					else
						ps.setNull(6, java.sql.Types.DECIMAL);
					ps.setInt(7, ajustado ? 1 : 0);
					ps.setTimestamp(8, new Timestamp(DateTime.now().getMillis()));
					ps.addBatch();
					i++;
				}
			}
			ps.executeBatch();
			ps.close();
			engine.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de egresos sin regularizaciones totales");
			e.printStackTrace(System.out);
		}
		return ret;
	}
	
	public static ArrayList<ArrayList<EgresoGasto>> getEgresosSinRegularizaciones(Connection conn, Integer ejercicio, Integer mes, Integer entidad, Integer unidad_ejecutora,
			Integer programa, Integer subprograma, Integer proyecto, Integer actividad, Integer obra, Integer fuente, Integer renglon, boolean fecha_devengado){
		ArrayList<ArrayList<EgresoGasto>> ret = new ArrayList<ArrayList<EgresoGasto>>();
		DateTime now = DateTime.now();
		try{
			PreparedStatement ps = conn.prepareStatement("select ejercicio,  " 
					+ ((entidad!=null) ? "entidad, " : "")
					+ ((unidad_ejecutora!=null) ? "unidad_ejecutora, " : "")
					+ ((programa!=null) ? "programa, " : "")
					+ ((subprograma!=null) ? "subprograma, " : "")
					+ ((proyecto!=null) ? "proyecto, " : "")
					+ ((actividad!=null) ? "actividad, " : "")
					+ ((obra!=null) ? "obra, " : "")
					+ ((fuente!=null) ? "fuente, " : "")
					+ ((renglon!=null) ? "renglon, " : "") +
					"round(sum(case when mes = 1 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m1, " + 
					"round(sum(case when mes = 2 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m2, " + 
					"round(sum(case when mes = 3 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m3, " + 
					"round(sum(case when mes = 4 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m4, " + 
					"round(sum(case when mes = 5 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m5, " + 
					"round(sum(case when mes = 6 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m6, " + 
					"round(sum(case when mes = 7 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m7, " + 
					"round(sum(case when mes = 8 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m8, " + 
					"round(sum(case when mes = 9 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m9, " + 
					"round(sum(case when mes = 10 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m10, " + 
					"round(sum(case when mes = 11 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m11, " + 
					"round(sum(case when mes = 12 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m12 " + 
					"from minfin.mv_gasto_sin_regularizaciones" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " +
					"where ejercicio between ? and ? " + 
					"group by "
					+ ((entidad!=null) ? "entidad, " : "")
					+ ((unidad_ejecutora!=null) ? "unidad_ejecutora, " : "")
					+ ((programa!=null) ? "programa, " : "")
					+ ((subprograma!=null) ? "subprograma, " : "")
					+ ((proyecto!=null) ? "proyecto, " : "")
					+ ((actividad!=null) ? "actividad, " : "")
					+ ((obra!=null) ? "obra, " : "")
					+ ((fuente!=null) ? "fuente, " : "")
					+ ((renglon!=null) ? "renglon, " : "")
					+ "ejercicio "
					+ "order by "
					+ ((entidad!=null) ? "entidad, " : "")
					+ ((unidad_ejecutora!=null) ? "unidad_ejecutora, " : "")
					+ ((programa!=null) ? "programa, " : "")
					+ ((subprograma!=null) ? "subprograma, " : "")
					+ ((proyecto!=null) ? "proyecto, " : "")
					+ ((actividad!=null) ? "actividad, " : "")
					+ ((obra!=null) ? "obra, " : "")
					+ ((fuente!=null) ? "fuente, " : "")
					+ ((renglon!=null) ? "renglon, " : "")
					+ " ejercicio");
			ps.setInt(1, ejercicio-5);
			ps.setInt(2, ejercicio);
			ResultSet rs = ps.executeQuery();
			Integer t_entidad=-1;
			Integer t_unidad_ejecutora=-1;
			Integer t_programa = -1;
			Integer t_subprograma = -1;
			Integer t_proyecto = -1;
			Integer t_actividad = -1;
			Integer t_obra = -1;
			Integer t_fuente = -1;
			Integer t_renglon = -1;
			ArrayList<EgresoGasto> dato=null;
			int inicio_sumas = 0;
			inicio_sumas = entidad!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = unidad_ejecutora!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = programa!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = subprograma!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = proyecto!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = actividad!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = obra!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = fuente!=null ?  inicio_sumas+1 : inicio_sumas;
			inicio_sumas = renglon!=null ?  inicio_sumas+1 : inicio_sumas;
			while(rs.next()){
				Integer r_entidad = rs.getInt("entidad");
				Integer r_unidad_ejecutora = (unidad_ejecutora!=null) ? rs.getInt("unidad_ejecutora") : -1;
				Integer r_programa = (programa!=null) ? rs.getInt("programa") : -1;
				Integer r_subprograma = (subprograma!=null) ? rs.getInt("subprograma") : -1;
				Integer r_proyecto = (proyecto!=null) ? rs.getInt("proyecto") : -1;
				Integer r_actividad = (actividad!=null) ? rs.getInt("actividad") : -1;
				Integer r_obra = (obra!=null) ? rs.getInt("obra") : -1;
				Integer r_fuente = (fuente!=null) ? rs.getInt("fuente") : -1;
				Integer r_renglon = (renglon!=null) ? rs.getInt("renglon") : -1;
				if(!t_entidad.equals(r_entidad) ||
						!t_unidad_ejecutora.equals(r_unidad_ejecutora) ||
						!t_programa.equals(r_programa) || 
						!t_subprograma.equals(r_subprograma) ||
						!t_proyecto.equals(r_proyecto) ||
						!t_actividad.equals(r_actividad) ||
						!t_obra.equals(r_obra) ||
						!t_fuente.equals(r_fuente) ||
						!t_renglon.equals(r_renglon)
						){
					if(dato!=null){
						ret.add(dato);
					}
					dato = new ArrayList<EgresoGasto>();
					t_entidad = r_entidad;
					t_unidad_ejecutora = r_unidad_ejecutora;
					t_programa = r_programa;
					t_subprograma = r_subprograma;
					t_proyecto = r_proyecto;
					t_actividad = r_actividad;
					t_obra = r_obra;
					t_fuente = r_fuente;
					t_renglon = r_renglon;
				}
					
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if(((rs.getInt(1)==ejercicio && i<mes) || rs.getInt(1)<ejercicio) && ((rs.getInt(1)==now.getYear() && i<now.getMonthOfYear()) || rs.getInt(1)<now.getYear()))
						montos.add(rs.getDouble(1+i));
				}
				EgresoGasto temp = new EgresoGasto(rs.getInt(1), entidad!=null ? rs.getInt("entidad") : 0, 
						unidad_ejecutora!=null ? rs.getInt("unidad_ejecutora") : 0, 
						programa!=null ? rs.getInt("programa"): 0,
						subprograma!=null ? rs.getInt("subprograma") : 0,
						proyecto !=null ? rs.getInt("proyecto") : 0,
						actividad!=null ? rs.getInt("actividad") : 0,
						obra!=null ? rs.getInt("obra") : 0,
						fuente!=null ? rs.getInt("fuente") : 0,
						renglon!=null ? rs.getInt("renglon") : 0,
						null, null, null, null,null, null, null, null, null, montos);
				dato.add(temp);
			}
			if(dato!=null)
				ret.add(dato);
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Egresos (Agrupados) "+e.getMessage());
		}
		return ret;
	}
	
	public static ArrayList<Double> getPronosticosEgresosSinRegularizaciones(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado,
			Integer entidad, Integer unidad_ejecutora, Integer programa, Integer subprograma, Integer proyecto, Integer actividad, Integer obra,
			Integer fuente, Integer renglon, boolean fecha_devengado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			ArrayList<ArrayList<EgresoGasto>> historicos = new ArrayList<ArrayList<EgresoGasto>>();
			historicos = getEgresosSinRegularizaciones(conn, ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, renglon, fecha_devengado);
			
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_egreso_sin_regularizaciones" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " 
					+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) "
					+ ((entidad!=null && entidad>100) ? " and entidad="+entidad : " ")  
					+ ((unidad_ejecutora!=null && unidad_ejecutora>1) ? " and unidad_ejecutora="+unidad_ejecutora : " ")
					+ ((programa!=null && programa>0) ? " and programa="+programa : " ")  
					+ ((subprograma!=null && subprograma>0) ? " and subprograma="+subprograma : " ")  
					+ ((proyecto!=null && proyecto>0) ? " and proyecto="+proyecto : " ")  
					+ ((actividad!=null && actividad>0) ? " and actividad="+actividad : " ")  
					+ ((obra!=null && obra>0) ? " and obra="+obra : " ")  
					+ ((fuente!=null && fuente>0) ? " and fuente="+fuente : " ")  
					+ ((renglon!=null && renglon>0) ? " and renglon="+renglon : " ")  
					+ " and ajustado=?");
			DateTime inicio = new DateTime(ejercicio,mes,1,0,0,0);
			DateTime fin = inicio.plusMonths(numero_pronosticos);
			ps.setInt(1, ejercicio);
			ps.setInt(2, mes);
			ps.setInt(3, inicio.getYear());
			ps.setInt(4, fin.getYear());
			ps.setInt(5, fin.getYear());
			ps.setInt(6, fin.getMonthOfYear());
			ps.setInt(7, ajustado ? 1 : 0);
			ps.executeUpdate();
			PreparedStatement psi = conn.prepareStatement("INSERT INTO minfin.mvp_egreso_sin_regularizaciones" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") 
					+ "(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, "
					+ "proyecto, actividad, obra, fuente,renglon, monto,modelo, error_modelo, ajustado, fecha_calculo) "
					+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			int rows=0;
			for(ArrayList<EgresoGasto> historico : historicos){
				CLogger.writeConsole("Calculando pronostico ejercicio = "+ejercicio+" mes = "+mes
						+" entidad = "+(historico.get(0).getEntidad()!=null ? historico.get(0).getEntidad() : 0)
						+" unidad_ejecutora = "+(historico.get(0).getUnidad_ejecutora()!=null ? historico.get(0).getUnidad_ejecutora() : 0)
						+" programa = "+(historico.get(0).getPrograma()!=null ? historico.get(0).getPrograma() : 0)
						+" subprograma = "+(historico.get(0).getSubprograma()!=null ? historico.get(0).getSubprograma() : 0)
						+" proyecto = "+(historico.get(0).getProyecto()!=null ? historico.get(0).getProyecto() : 0)
						+" actividad = "+(historico.get(0).getActividad()!=null ? historico.get(0).getActividad() : 0)
						+" obra = "+(historico.get(0).getObra()!=null ? historico.get(0).getObra() : 0)
						+" fuente = "+(historico.get(0).getFuente()!=null ? historico.get(0).getFuente() : 0)
						+" renglon = "+(historico.get(0).getRenglon()!=null ? historico.get(0).getRenglon() : 0)
					);
				int ts_año_inicio = historico.get(0).getEjercicio();
				
				//Rengine.DEBUG = 5;
				
				engine.eval("suppressPackageStartupMessages(library(forecast))");
				String vector_aplanado = getVectorAplanado(historico);
				
				if(vector_aplanado.length()>0){
					vector_aplanado = "c("+vector_aplanado+")";
					engine.eval("datos = " + vector_aplanado);
					engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
					if(ajustado){
						engine.eval("ajuste = ar(BoxCox(serie,lambda=1))");
						engine.eval("fc = forecast(ajuste,h="+numero_pronosticos+", lambda=1)");
					}
					else{
						engine.eval("fc = forecast(ets(serie),"+numero_pronosticos+")");
					}	
					engine.eval("resultados=as.numeric(fc$mean)");
					double[] res_ets=engine.eval("resultados").asDoubles();
					engine.eval("error=accuracy(fc)[5]");
					double error_ets = engine.eval("error").asDouble();
					
					engine.eval("serie = ts(datos, start=c("+ts_año_inicio+",1), frequency=12)");
					engine.eval("fc = forecast(auto.arima(serie),"+numero_pronosticos+")");
					engine.eval("resultados=as.numeric(fc$mean)");
					double[] res_arima=engine.eval("resultados").asDoubles();
					engine.eval("error=accuracy(fc)[5]");
					double error_arima = engine.eval("error").asDouble();
					
					double error=0;
					int i=0;
					for(double dato: (error_ets<=error_arima ? res_ets : res_arima)){
						ret.add(dato);
						DateTime tiempo = inicio.plusMonths(i);
						psi.setInt(1, tiempo.getYear());
						psi.setInt(2, tiempo.getMonthOfYear());
						psi.setInt(3, historico.get(0).getEntidad()!=null ? historico.get(0).getEntidad() : 0);
						psi.setInt(4, historico.get(0).getUnidad_ejecutora()!=null ? historico.get(0).getUnidad_ejecutora() : 0);
						psi.setInt(5, historico.get(0).getPrograma()!=null ? historico.get(0).getPrograma() : 0);
						psi.setInt(6, historico.get(0).getSubprograma()!=null ? historico.get(0).getSubprograma() : 0);
						psi.setInt(7, historico.get(0).getProyecto()!=null ? historico.get(0).getProyecto() : 0);
						psi.setInt(8, historico.get(0).getActividad()!=null ? historico.get(0).getActividad() : 0);
						psi.setInt(9, historico.get(0).getObra()!=null ? historico.get(0).getObra() : 0);
						psi.setInt(10, historico.get(0).getFuente()!=null ? historico.get(0).getFuente() : 0);
						psi.setInt(11, historico.get(0).getRenglon()!=null ? historico.get(0).getRenglon() : 0);
						psi.setDouble(12, dato);
						psi.setString(13, (error_ets<=error_arima ? "ETS" : "ARIMA"));
						error = error_ets<=error_arima ? error_ets : error_arima;
						if(!Double.isNaN(error) && !Double.isInfinite(error))
							psi.setDouble(14,  error);
						else
							psi.setNull(14, java.sql.Types.DECIMAL);
						psi.setInt(15, ajustado ? 1 : 0);
						psi.setTimestamp(16, new Timestamp(DateTime.now().getMillis()));
						psi.addBatch();
						i++;
						rows++;
						if(rows%10000==0)
							psi.executeBatch();
					}
					
				}
			}
			psi.executeBatch();
			psi.close();
			engine.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de egresos sin regularizaciones");
			e.printStackTrace(System.out);
		}
		return ret;
	}
	
	public static ArrayList<ArrayList<EgresoGasto>> getEgresosEntidades(Connection conn, Integer ejercicio, Integer mes, boolean fecha_devengado){
		ArrayList<ArrayList<EgresoGasto>> ret = new ArrayList<ArrayList<EgresoGasto>>();
		DateTime now = DateTime.now();
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT ejercicio, entidad, "
					+ "round(sum(m1),2) m1, "
					+ "round(sum(m2),2) m2, "
					+ "round(sum(m3),2) m3, "
					+ "round(sum(m4),2) m4, "
					+ "round(sum(m5),2) m5, "
					+ "round(sum(m6),2) m6, "
					+ "round(sum(m7),2) m7, "
					+ "round(sum(m8),2) m8, "
					+ "round(sum(m9),2) m9, "
					+ "round(sum(m10),2) m10, "
					+ "round(sum(m11),2) m11, "
					+ "round(sum(m12),2) m12 "
					+ "from minfin.mv_ejecucion_presupuestaria_mensualizada" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " "
					+ " "
					+ "where ejercicio between  ? and ? "
					+ "group by entidad, ejercicio ORDER BY entidad, ejercicio");
			ps.setInt(1, ejercicio-5);
			ps.setInt(2, ejercicio);
			ResultSet rs = ps.executeQuery();
			int entidad_actual = 0;
			ArrayList<EgresoGasto> atemp=null;
			while(rs.next()){
				if(entidad_actual!=rs.getInt(2)) {
					if(atemp!=null)
						ret.add(atemp);
					atemp = new ArrayList<EgresoGasto>();
					entidad_actual = rs.getInt(2);
				}
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if(((rs.getInt(1)==ejercicio && i<mes) || rs.getInt(1)<ejercicio) && ((rs.getInt(1)==now.getYear() && i<now.getMonthOfYear()) || rs.getInt(1)<now.getYear()))
						montos.add(rs.getDouble(2+i));
				}
				EgresoGasto temp = new EgresoGasto(rs.getInt(1), rs.getInt(2), 0, 0,0,0,0,0,0,0, null, null,null,null,null,null,null,null,null, montos);
				atemp.add(temp);
			}
			if(atemp!=null)
				ret.add(atemp);
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Egresos Totales "+e.getMessage());
		}
		return ret;
	}
	
	public static ArrayList<ArrayList<EgresoGasto>> getEgresosSinRegularizacionesEntidades(Connection conn, Integer ejercicio, Integer mes, 
			boolean fecha_devengado){
		ArrayList<ArrayList<EgresoGasto>> ret = new ArrayList<ArrayList<EgresoGasto>>();
		DateTime now = DateTime.now();
		try{
			PreparedStatement ps = conn.prepareStatement("select ejercicio, entidad,  " + 
					"round(sum(case when mes = 1 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m1, " + 
					"round(sum(case when mes = 2 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m2, " + 
					"round(sum(case when mes = 3 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m3, " + 
					"round(sum(case when mes = 4 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m4, " + 
					"round(sum(case when mes = 5 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m5, " + 
					"round(sum(case when mes = 6 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m6, " + 
					"round(sum(case when mes = 7 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m7, " + 
					"round(sum(case when mes = 8 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m8, " + 
					"round(sum(case when mes = 9 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m9, " + 
					"round(sum(case when mes = 10 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m10, " + 
					"round(sum(case when mes = 11 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m11, " + 
					"round(sum(case when mes = 12 then (ifnull(gasto,0)-ifnull(deducciones,0)) else 0 end),2) m12 " + 
					"from minfin.mv_gasto_sin_regularizaciones" +
					(fecha_devengado==false ? "_fecha_pagado_total" : "") + " " +
					"where ejercicio between ? and ? " + 
					"group by entidad,ejercicio ORDER BY entidad,ejercicio");
			ps.setInt(1, ejercicio-5);
			ps.setInt(2, ejercicio);
			ResultSet rs = ps.executeQuery();
			ArrayList<EgresoGasto> atemp = null;
			int entidad_actual = 0;
			while(rs.next()){
				if(entidad_actual!=rs.getInt(2)) {
					if(atemp!=null)
						ret.add(atemp);
					atemp = new ArrayList<EgresoGasto>();
					entidad_actual = rs.getInt(2);
				}
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if(((rs.getInt(1)==ejercicio && i<mes) || rs.getInt(1)<ejercicio) && ((rs.getInt(1)==now.getYear() && i<now.getMonthOfYear()) || rs.getInt(1)<now.getYear()))
						montos.add(rs.getDouble(2+i));
				}
				EgresoGasto temp = new EgresoGasto(rs.getInt(1), rs.getInt(2), 0, 0,0,0,0,0,0,0, null, null,null,null,null,null,null,null,null, montos);
				atemp.add(temp);
			}
			if(atemp!=null)
				ret.add(atemp);
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de Egresos Totales "+e.getMessage());
		}
		return ret;
	}
	
}

