package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.utility.Utility;

public class ReadOperator implements Operator {

	File file = null;
	BufferedReader br = null;
	String tableName;

	public ReadOperator(File f, String tableName){
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
			String[] cols = line.split("\\|");
			Object[] tuple = new Object[cols.length];
			ArrayList<String> dataType = Utility.tableSchema.get(tableName.toUpperCase());
			for(int i=0; i<cols.length;i++){
				switch(dataType.get(i)){
				case "int":
				case "INT":
					tuple[i] = new LongValue(cols[i]); 
					break;
				case "decimal":
				case "DECIMAL":
				case "DOUBLE":
					tuple[i] = new DoubleValue(cols[i]); 
					break;
				case "date": 
				case "DATE":
					tuple[i] = new DateValue(" "+cols[i]+" "); 
					break;
				case "char": 
				case "CHAR":
					tuple[i] = new StringValue(" "+cols[i]+" "); 
					break;
				case "string": 
				case "STRING":
					tuple[i] = new StringValue(" "+cols[i]+" "); 
					break;
				case "varchar":
				case "VARCHAR":
					tuple[i] = new StringValue(" "+cols[i]+" "); 
					break;
				default:
				{
					if(dataType.get(i).contains("CHAR") || dataType.get(i).contains("char")){
						tuple[i] = new StringValue(" "+cols[i]+" ");
					}
				}
				}
			}
			return tuple;
		}catch(IOException e){
			System.out.println("IOException in ReadOperator.readOneTuple()");
		}
		return null;
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return tableName;
	}

}
