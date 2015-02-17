/**
 * 
 */
package edu.buffalo.cse562.table;

public class Column {
	private String name;
	private String dataType;
	
	public Column(String name, String dataType){
		this.name = name;
		this.dataType = dataType;
	}
	
	public String getName(){
		return this.name;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public String getDataType(){
		return this.dataType;
	}
	
	public void setDataType( String dataType){
		this.dataType = dataType;
	}
}
