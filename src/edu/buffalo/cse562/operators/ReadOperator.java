package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import net.sf.jsqlparser.expression.*;
import edu.buffalo.cse562.Main;

public class ReadOperator implements Operator {
	
	File file = null;
	BufferedReader br = null;
	String tableName;
	
	ReadOperator(File f, String tableName){
		this.file = f;
		this.tableName = tableName;
		reset();
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try{
			br = new BufferedReader(new FileReader(file));
		}
		catch(IOException e){
			System.out.println("IO Exception in ReadOperator.reset()");
		}
		
	}

	@Override
	/**Read One tuple at a time from the File.
	 * @return Object array for the row. 
	 */
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		if(br == null)
			return null;
		String line = "";
		
		try{
			line=br.readLine();
			if(line == null)
				return null;
			String cols[] = line.split("\\|");
			Object[] tuple = new Object[cols.length];
			for(int i=0; i<cols.length;i++){
				switch(Main.tables.get(this.tableName).get(i).getDataType()){
					case "int": tuple[i] = new LongValue(cols[i]); break;
					case "decimal": tuple[i] = new DoubleValue(cols[i]); break;
					case "date": tuple[i] = new DateValue(cols[i]); break;
					case "char": tuple[i] = new StringValue("'"+cols[i]+"'"); break;
					case "string": tuple[i] = new StringValue("'"+cols[i]+"'"); break;
					case "varchar": tuple[i] = new StringValue("'"+cols[i]+"'"); break;			
				}
					
				System.out.println(tuple[i]+"qwerty");
			}
			return tuple;
		}catch(IOException e){
			System.out.println("IOException in ReadOperator.readOneTuple()");
		}
		return null;
	}

}