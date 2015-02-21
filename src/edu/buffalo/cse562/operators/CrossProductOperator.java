package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.Join;
import edu.buffalo.cse562.utility.Utility;

public class CrossProductOperator implements Operator{

	ArrayList<ReadOperator> readOps;
	ArrayList<String> tableNames;

	public CrossProductOperator(Operator oper, ArrayList<Join> joins,
			String tableName) {
		readOps = new ArrayList<ReadOperator>();
		tableNames = new ArrayList<String>();

		readOps.add((ReadOperator)oper);
		tableNames.add(tableName);
		for(Join table: joins){
			tableNames.add(table.toString());
			String dataFileName = table.toString().toLowerCase() + ".dat";
			dataFileName = Utility.dataDir.toString() + File.separator + dataFileName;
			try{
				readOps.add(new ReadOperator(new File(dataFileName), table.toString()));
			}catch(NullPointerException e){
				System.out.println("Null pointer exception in JoinOperator()");
			}
		}

	}

	@Override
	public void reset() {

	}

	@Override
	public Object[] readOneTuple() {
		HashMap<String, Integer> newSchema = new HashMap<String, Integer>();
		HashMap<String, Integer> tempSchema = new HashMap<String, Integer>();
		Object[] temp1 = readOps.get(0).readOneTuple();
		Object[] temp2 = readOps.get(1).readOneTuple();
		Object[] temp3 = null;
		Object[] temp4 = null;
		int size1 = Utility.tables.get(tableNames.get(0)).size();
		int size2 = Utility.tables.get(tableNames.get(1)).size();
		int size3 = 0;
		int size4 = 0;
		
		updateSchema(newSchema, tempSchema, 0, 0);
		updateSchema(newSchema, tempSchema, size1, 1);
		
		String newTableName = tableNames.get(0)+","+tableNames.get(1);
		
		if(readOps.size() == 3){
			temp3 = readOps.get(2).readOneTuple();
			size3 = Utility.tables.get(tableNames.get(2)).size();
			updateSchema(newSchema, tempSchema, size2+size1, 2);
			newTableName += ","+tableNames.get(2);
		}
		if(readOps.size() == 4){
			temp4 = readOps.get(3).readOneTuple();
			size4 = Utility.tables.get(tableNames.get(3)).size();
			updateSchema(newSchema, tempSchema, size3+size2+size1, 3);
			newTableName += ","+tableNames.get(3);
		}
	
		for(String col: newSchema.keySet()){
			System.out.println(col+" "+newSchema.get(col));
		}
		
		Utility.tables.put(newTableName, newSchema);
		
		int size = size1+size2+size3+size4;
		Object[] toReturn = new Object[size];
		
		while(temp1 != null){
			for(int i = 0; i < size1; i++){
				toReturn[i] = temp1[i];
			}
			while(temp2 != null){
				for(int i = size1, j=0; i < size1+size2; i++){
					toReturn[i] = temp2[j];
					j++;
				}
				
				while(temp3 != null){
					for(int i = size1+size2, j=0; i < size1+size2+size3; i++){
						toReturn[i] = temp3[j];
						j++;
					}
					System.out.println(toReturn[0]+" "+toReturn[1]+" "+toReturn[2]+" "+toReturn[3]+" "+toReturn[4]+" "+toReturn[5]);
					while(temp4 != null){
						temp4 = readOps.get(3).readOneTuple();
					}//end of 4
					temp3 = readOps.get(2).readOneTuple();
				}//end of 3
				readOps.get(2).reset();
				temp3 = readOps.get(2).readOneTuple();
				temp2 = readOps.get(1).readOneTuple();
			}//end of 2
			readOps.get(1).reset();
			temp2 = readOps.get(1).readOneTuple();
			temp1 = readOps.get(0).readOneTuple();
		}// end of 1
		return null;
	}

	void updateSchema(HashMap<String, Integer> newSchema, HashMap<String, Integer> tempSchema,
			int size, int index){
		tempSchema = Utility.tables.get(tableNames.get(index));
		for(String col: tempSchema.keySet()){
			newSchema.put(col, tempSchema.get(col)+size);
		}
	}
	

}
