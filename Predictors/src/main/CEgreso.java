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
					ret = ret + ", " + String.format("%.2f",dato.getMontos().get(i));
			}
		}
		return ret!=null && ret.length()>0 ? ret.substring(1) : "";
	} 
	
	private static ArrayList<Integer> getEntidadesGobiernoCentro(Connection conn, Integer ejercicio){
		ArrayList<Integer> ret = new ArrayList<Integer>();
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT * FROM cg_entidades WHERE ((entidad between 11130003 and 11130020) OR entidad = 11140021) AND ejercicio=?");
			ps.setInt(1, ejercicio);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ret.add(rs.getInt("entidad"));
			}
		}
		catch(Throwable e){
			CLogger.writeConsole("Error al consultar entidades de Gobierno Central");
		}
		return ret;
	}
	
	public static ArrayList<EgresoGasto> getEgresosEntidad(Connection conn, Integer ejercicio, Integer mes, Integer entidad){
		ArrayList<EgresoGasto> ret = new ArrayList<EgresoGasto>();
		DateTime now = DateTime.now();
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT ejercicio, entidad, "
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
					+ "from minfin.mv_ejecucion_presupuestaria_mensualizada "
					+ "where ejercicio<=? and entidad=? "
					+ "group by ejercicio ORDER BY ejercicio, entidad");
			ps.setInt(1, ejercicio);
			ps.setInt(2, entidad);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				ArrayList<Double> montos=new ArrayList<Double>();
				for(int i=1; i<=12; i++){
					if(((rs.getInt(1)==ejercicio && i<mes) || rs.getInt(1)<ejercicio) && ((rs.getInt(1)==now.getYear() && i<now.getMonthOfYear()) || rs.getInt(1)<now.getYear()))
						montos.add(rs.getDouble(1+i));
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
	
public static ArrayList<Double> getPronosticosEgresosEntidades(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			ArrayList<Integer> entidades = getEntidadesGobiernoCentro(conn, ejercicio);
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
			for(Integer entidad:entidades){
				CLogger.writeConsole("Calculando pronostico para la entidad "+entidad);
				ArrayList<EgresoGasto> historicos = new ArrayList<EgresoGasto>();
				historicos = getEgresosEntidad(conn, ejercicio, mes, entidad);
				int ts_año_inicio = historicos.get(0).getEjercicio();
				
				//Rengine.DEBUG = 5;
				engine.eval("suppressPackageStartupMessages(library(forecast))");
				String vector_aplanado = "c("+getVectorAplanado(historicos)+")";
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
				
				DateTime inicio = new DateTime(ejercicio,mes,1,0,0,0);
				DateTime fin = inicio.plusMonths(numero_pronosticos);
				PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_egreso "
						+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and entidad=? and unidad_ejecutora=0 "
						+ "and programa=0 and subprograma=0 and proyecto=0 and actividad=0 and obra=0 and fuente=0 and ajustado=?");
				ps.setInt(1, ejercicio);
				ps.setInt(2, mes);
				ps.setInt(3, inicio.getYear());
				ps.setInt(4, fin.getYear());
				ps.setInt(5, fin.getYear());
				ps.setInt(6, fin.getMonthOfYear());
				ps.setInt(7, entidad);
				ps.setInt(8, ajustado ? 1 : 0);
				ps.executeUpdate();
				ps.close();
				ps = conn.prepareStatement("INSERT INTO minfin.mvp_egreso(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, "
						+ "proyecto, actividad, obra, fuente,renglon, monto,modelo, error_modelo, ajustado, fecha_calculo) "
						+ "values(?,?,?,0,0,0,0,0,0,0,0,?,?,?,?,?)");
				double error=0;
				int i=0;
				for(double dato: (error_ets<=error_arima ? res_ets : res_arima)){
					ret.add(dato);
					DateTime tiempo = inicio.plusMonths(i);
					ps.setInt(1, tiempo.getYear());
					ps.setInt(2, tiempo.getMonthOfYear());
					ps.setInt(3, entidad);
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
				ps.executeBatch();
				ps.close();
			}
			engine.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de egresos totales");
			e.printStackTrace(System.out);
		}
		return ret;
	}
	
	public static ArrayList<ArrayList<EgresoGasto>> getEgresos(Connection conn, Integer ejercicio, Integer mes, Integer entidad, Integer unidad_ejecutora,
			Integer programa, Integer subprograma, Integer proyecto, Integer actividad, Integer obra, Integer fuente, Integer renglon){
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
					+ "from minfin.mv_ejecucion_presupuestaria_mensualizada "
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
			Integer t_entidad=null;
			Integer t_unidad_ejecutora=null;
			Integer t_programa = null;
			Integer t_subprograma = null;
			Integer t_proyecto = null;
			Integer t_actividad = null;
			Integer t_obra = null;
			Integer t_fuente = null;
			Integer t_renglon = null;
			ArrayList<EgresoGasto> dato=null;
			while(rs.next()){
				Integer r_entidad = rs.getInt("entidad");
				Integer r_unidad_ejecutora = (unidad_ejecutora!=null) ? rs.getInt("unidad_ejecutora") : null;
				Integer r_programa = (programa!=null) ? rs.getInt("programa") : null;
				Integer r_subprograma = (subprograma!=null) ? rs.getInt("subprograma") : null;
				Integer r_proyecto = (proyecto!=null) ? rs.getInt("proyecto") : null;
				Integer r_actividad = (actividad!=null) ? rs.getInt("actividad") : null;
				Integer r_obra = (obra!=null) ? rs.getInt("obra") : null;
				Integer r_fuente = (fuente!=null) ? rs.getInt("fuente") : null;
				Integer r_renglon = (renglon!=null) ? rs.getInt("renglon") : null;
				if(t_entidad!=r_entidad ||
						t_unidad_ejecutora!=r_unidad_ejecutora ||
						t_programa!=r_programa || 
						t_subprograma!=r_subprograma ||
						t_proyecto!=r_proyecto ||
						t_actividad!=r_actividad ||
						t_obra != r_obra ||
						t_fuente!=r_fuente ||
						t_renglon !=r_renglon
						){
					if(dato!=null){
						ret.add(dato);
					}
					dato = new ArrayList<EgresoGasto>();
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
	
	public static ArrayList<Double> getPronosticosEgresos(Connection conn, Integer ejercicio, Integer mes, Integer numero_pronosticos, boolean ajustado,
			Integer entidad, Integer unidad_ejecutora, Integer programa, Integer subprograma, Integer proyecto, Integer actividad, Integer obra,
			Integer fuente, Integer renglon){
		ArrayList<Double> ret = new ArrayList<Double>();
		try{
			ArrayList<ArrayList<EgresoGasto>> historicos = new ArrayList<ArrayList<EgresoGasto>>();
			historicos = getEgresos(conn, ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, proyecto, actividad, obra, fuente, renglon);
			
			RConnection engine = new RConnection(CProperties.getRserve(), CProperties.getRservePort());
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
					
					DateTime inicio = new DateTime(ejercicio,mes,1,0,0,0);
					DateTime fin = inicio.plusMonths(numero_pronosticos);
					PreparedStatement ps=conn.prepareStatement("DELETE FROM minfin.mvp_egreso "
							+ "WHERE ((ejercicio=? and mes>=?) OR (ejercicio>? and ejercicio<?) OR (ejercicio=? and mes<=?)) and entidad=? and unidad_ejecutora=? "
							+ "and programa=? and subprograma=? and proyecto=? and actividad=? and obra=? and fuente=? and renglon=? and ajustado=?");
					ps.setInt(1, ejercicio);
					ps.setInt(2, mes);
					ps.setInt(3, inicio.getYear());
					ps.setInt(4, fin.getYear());
					ps.setInt(5, fin.getYear());
					ps.setInt(6, fin.getMonthOfYear());
					ps.setInt(7, historico.get(0).getEntidad()!=null ? historico.get(0).getEntidad() : 0);
					ps.setInt(8, historico.get(0).getUnidad_ejecutora()!=null ? historico.get(0).getUnidad_ejecutora() : 0);
					ps.setInt(9, historico.get(0).getPrograma()!=null ? historico.get(0).getPrograma() : 0);
					ps.setInt(10, historico.get(0).getSubprograma()!=null ? historico.get(0).getSubprograma() : 0);
					ps.setInt(11, historico.get(0).getProyecto()!=null ? historico.get(0).getProyecto() : 0);
					ps.setInt(12, historico.get(0).getActividad()!=null ? historico.get(0).getActividad() : 0);
					ps.setInt(13, historico.get(0).getObra()!=null ? historico.get(0).getObra() : 0);
					ps.setInt(14, historico.get(0).getFuente()!=null ? historico.get(0).getFuente() : 0);
					ps.setInt(15, historico.get(0).getRenglon()!=null ? historico.get(0).getRenglon() : 0);
					ps.setInt(16, ajustado ? 1 : 0);
					ps.executeUpdate();
					ps.close();
					ps = conn.prepareStatement("INSERT INTO minfin.mvp_egreso(ejercicio, mes, entidad, unidad_ejecutora, programa, subprograma, "
							+ "proyecto, actividad, obra, fuente,renglon, monto,modelo, error_modelo, ajustado, fecha_calculo) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
					double error=0;
					int i=0;
					for(double dato: (error_ets<=error_arima ? res_ets : res_arima)){
						ret.add(dato);
						DateTime tiempo = inicio.plusMonths(i);
						ps.setInt(1, tiempo.getYear());
						ps.setInt(2, tiempo.getMonthOfYear());
						ps.setInt(3, historico.get(0).getEntidad()!=null ? historico.get(0).getEntidad() : 0);
						ps.setInt(4, historico.get(0).getUnidad_ejecutora()!=null ? historico.get(0).getUnidad_ejecutora() : 0);
						ps.setInt(5, historico.get(0).getPrograma()!=null ? historico.get(0).getPrograma() : 0);
						ps.setInt(6, historico.get(0).getSubprograma()!=null ? historico.get(0).getSubprograma() : 0);
						ps.setInt(7, historico.get(0).getProyecto()!=null ? historico.get(0).getProyecto() : 0);
						ps.setInt(8, historico.get(0).getActividad()!=null ? historico.get(0).getActividad() : 0);
						ps.setInt(9, historico.get(0).getObra()!=null ? historico.get(0).getObra() : 0);
						ps.setInt(10, historico.get(0).getFuente()!=null ? historico.get(0).getFuente() : 0);
						ps.setInt(11, historico.get(0).getRenglon()!=null ? historico.get(0).getRenglon() : 0);
						ps.setDouble(12, dato);
						ps.setString(13, (error_ets<=error_arima ? "ETS" : "ARIMA"));
						error = error_ets<=error_arima ? error_ets : error_arima;
						if(!Double.isNaN(error) && !Double.isInfinite(error))
							ps.setDouble(14,  error);
						else
							ps.setNull(14, java.sql.Types.DECIMAL);
						ps.setInt(15, ajustado ? 1 : 0);
						ps.setTimestamp(16, new Timestamp(DateTime.now().getMillis()));
						ps.addBatch();
						i++;
					}
					ps.executeBatch();
					ps.close();
				}
			}
			engine.close();
		}
		catch(Exception e){
			CLogger.writeConsole("Error al calcular los pronosticos de egresos");
			e.printStackTrace(System.out);
		}
		return ret;
	}
	
}

