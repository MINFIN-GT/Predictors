package pojo;

import java.util.ArrayList;

public class IngresoRecursoAuxiliar {
	private Integer ejercicio;
	private Integer recurso;
	private String recurso_nombre;
	private Integer auxiliar;
	private String auxiliar_nombre;
	private ArrayList<Double> montos;
	
	public IngresoRecursoAuxiliar(Integer ejercicio, Integer recurso, String recurso_nombre,
			Integer auxiliar, String auxiliar_nombre, ArrayList<Double> montos) {
		super();
		this.ejercicio = ejercicio;
		this.recurso = recurso;
		this.recurso_nombre = recurso_nombre;
		this.auxiliar = auxiliar;
		this.auxiliar_nombre = auxiliar_nombre;
		this.montos = montos;
	}
	
	public Integer getEjercicio() {
		return ejercicio;
	}
	public void setEjercicio(Integer ejercicio) {
		this.ejercicio = ejercicio;
	}
	public Integer getRecurso() {
		return recurso;
	}
	public void setRecurso(Integer recurso) {
		this.recurso = recurso;
	}
	public String getRecurso_nombre() {
		return recurso_nombre;
	}
	public void setRecurso_nombre(String recurso_nombre) {
		this.recurso_nombre = recurso_nombre;
	}
	public Integer getAuxiliar() {
		return auxiliar;
	}
	public void setAuxiliar(Integer auxiliar) {
		this.auxiliar = auxiliar;
	}
	public String getAuxiliar_nombre() {
		return auxiliar_nombre;
	}
	public void setAuxiliar_nombre(String auxiliar_nombre) {
		this.auxiliar_nombre = auxiliar_nombre;
	}
	public ArrayList<Double> getMontos() {
		return montos;
	}
	public void setMonto(ArrayList<Double> montos) {
		this.montos = montos;
	}  
}
