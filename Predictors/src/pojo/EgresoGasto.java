package pojo;

import java.util.ArrayList;

public class EgresoGasto {
	private Integer ejercicio;
	private Integer entidad;
	private Integer unidad_ejecutora;
	private Integer programa;
	private Integer subprograma;
	private Integer proyecto;
	private Integer actividad;
	private Integer obra;
	private Integer fuente;
	private Integer renglon;
	private String entidad_nombre;
	private String unidad_ejecutora_nombre;
	private String programa_nombre;
	private String subprograma_nombre;
	private String proyecto_nombre;
	private String actividad_nombre;
	private String obra_nombre;
	private String fuente_nombre;
	private String renglon_nombre;
	private ArrayList<Double> montos;
	
	public EgresoGasto(Integer ejercicio, Integer entidad, Integer unidad_ejecutora, Integer programa, Integer subprograma,
			Integer proyecto, Integer actividad, Integer obra, Integer fuente, Integer renglon, String entidad_nombre,
			String unidad_ejecutora_nombre, String programa_nombre, String subprograma_nombre, String proyecto_nombre, 
			String actividad_nombre, String obra_nombre, String fuente_nombre, String renglon_nombre, ArrayList<Double> montos) {
		super();
		this.ejercicio = ejercicio;
		this.entidad = entidad;
		this.unidad_ejecutora = unidad_ejecutora;
		this.programa = programa;
		this.subprograma = subprograma;
		this.proyecto = proyecto;
		this.actividad = actividad;
		this.obra = obra;
		this.fuente = fuente;
		this.renglon = renglon;
		this.entidad_nombre = entidad_nombre;
		this.unidad_ejecutora_nombre = unidad_ejecutora_nombre;
		this.programa_nombre = programa_nombre;
		this.subprograma_nombre = subprograma_nombre;
		this.proyecto_nombre = proyecto_nombre;
		this.actividad_nombre = actividad_nombre;
		this.obra_nombre = obra_nombre;
		this.fuente_nombre = fuente_nombre;
		this.renglon_nombre = renglon_nombre;
		this.montos = montos;
	}

	public Integer getEjercicio() {
		return ejercicio;
	}

	public void setEjercicio(Integer ejercicio) {
		this.ejercicio = ejercicio;
	}

	public Integer getEntidad() {
		return entidad;
	}

	public void setEntidad(Integer entidad) {
		this.entidad = entidad;
	}

	public Integer getUnidad_ejecutora() {
		return unidad_ejecutora;
	}

	public void setUnidad_ejecutora(Integer unidad_ejecutora) {
		this.unidad_ejecutora = unidad_ejecutora;
	}

	public Integer getPrograma() {
		return programa;
	}

	public void setPrograma(Integer programa) {
		this.programa = programa;
	}

	public Integer getSubprograma() {
		return subprograma;
	}

	public void setSubprograma(Integer subprograma) {
		this.subprograma = subprograma;
	}

	public Integer getProyecto() {
		return proyecto;
	}

	public void setProyecto(Integer proyecto) {
		this.proyecto = proyecto;
	}

	public Integer getActividad() {
		return actividad;
	}

	public void setActividad(Integer actividad) {
		this.actividad = actividad;
	}

	public Integer getObra() {
		return obra;
	}

	public void setObra(Integer obra) {
		this.obra = obra;
	}

	public Integer getFuente() {
		return fuente;
	}

	public void setFuente(Integer fuente) {
		this.fuente = fuente;
	}

	public Integer getRenglon() {
		return renglon;
	}

	public void setRenglon(Integer renglon) {
		this.renglon = renglon;
	}

	public String getEntidad_nombre() {
		return entidad_nombre;
	}

	public void setEntidad_nombre(String entidad_nombre) {
		this.entidad_nombre = entidad_nombre;
	}

	public String getUnidad_ejecutora_nombre() {
		return unidad_ejecutora_nombre;
	}

	public void setUnidad_ejecutora_nombre(String unidad_ejecutora_nombre) {
		this.unidad_ejecutora_nombre = unidad_ejecutora_nombre;
	}

	public String getPrograma_nombre() {
		return programa_nombre;
	}

	public void setPrograma_nombre(String programa_nombre) {
		this.programa_nombre = programa_nombre;
	}

	public String getSubprograma_nombre() {
		return subprograma_nombre;
	}

	public void setSubprograma_nombre(String subprograma_nombre) {
		this.subprograma_nombre = subprograma_nombre;
	}

	public String getProyecto_nombre() {
		return proyecto_nombre;
	}

	public void setProyecto_nombre(String proyecto_nombre) {
		this.proyecto_nombre = proyecto_nombre;
	}

	public String getActividad_nombre() {
		return actividad_nombre;
	}

	public void setActividad_nombre(String actividad_nombre) {
		this.actividad_nombre = actividad_nombre;
	}

	public String getObra_nombre() {
		return obra_nombre;
	}

	public void setObra_nombre(String obra_nombre) {
		this.obra_nombre = obra_nombre;
	}

	public String getFuente_nombre() {
		return fuente_nombre;
	}

	public void setFuente_nombre(String fuente_nombre) {
		this.fuente_nombre = fuente_nombre;
	}

	public String getRenglon_nombre() {
		return renglon_nombre;
	}

	public void setRenglon_nombre(String renglon_nombre) {
		this.renglon_nombre = renglon_nombre;
	}

	public ArrayList<Double> getMontos() {
		return montos;
	}

	public void setMontos(ArrayList<Double> montos) {
		this.montos = montos;
	}
	
	
}
