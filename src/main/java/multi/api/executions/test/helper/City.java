package multi.api.executions.test.helper;

import java.io.Serializable;
import java.util.List;

public class City implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String city;
	private List<Double> loc;
	private Long pop;
	private String state;
	private String _id;
	
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public List<Double> getLoc() {
		return loc;
	}

	public void setLoc(List<Double> location) {
		this.loc = location;
	}

	public Long getPop() {
		return pop;
	}

	public void setPop(Long population) {
		this.pop = population;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

		
	public City(){
		
	}
	
	public City(String s, List<Double> l, Long p, String s2, String z){
		this.city = s;
		this.loc = l;
		this.pop = p;
		this.state = s2;
		this.set_id(z);
	}

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}
	
}

