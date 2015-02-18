package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ReadOperator implements Operator {
	
	File file = null;
	BufferedReader br = null;
	
	ReadOperator(File f){
		this.file = f; 
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
				tuple[i] = new Integer(Integer.parseInt(cols[i]));
			}
			return tuple;
		}catch(IOException e){
			System.out.println("IOException in ReadOperator.readOneTuple()");
		}
		return null;
	}

}
