package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.utility.Utility;

public class CrossProductOperator implements Operator{

	ArrayList<ReadOperator> readOps;
	ArrayList<Table> joinTables;
	static ArrayList<Object[]> allTuples;
	static int counter;
	Table table;

	public CrossProductOperator(Operator oper, ArrayList<Table> joinTables,
			Table table) {
		counter = 0;
		readOps = new ArrayList<ReadOperator>();
		this.joinTables = new ArrayList<Table>();
		allTuples = new ArrayList<Object[]>();
		readOps.add((ReadOperator)oper);
		this.joinTables.add(table);
		this.joinTables.addAll(joinTables);
		for(Table t: joinTables){
			String dataFileName = t.getName().toString() + ".dat";
			dataFileName = Utility.dataDir.toString() + File.separator + dataFileName;
			try{
				readOps.add(new ReadOperator(new File(dataFileName), t));
			}catch(NullPointerException e){
				System.out.println("Null pointer exception in JoinOperator()");
			}
		}
		initializeAllTuple();
	}

	@Override
	public void reset() {
		counter = 0;
	}

	@Override
	public Object[] readOneTuple() {
		Object[] temp = null;
		if(counter < allTuples.size()){
			temp = allTuples.get(counter);
			counter++;
		}
		return temp;
	}

	void updateSchema(HashMap<String, Integer> newSchema, HashMap<String, Integer> tempSchema,
			int size, int index, ArrayList<String> dataType){
		
		tempSchema = Utility.tables.get(joinTables.get(index).getAlias());
		dataType.addAll(Utility.tableSchema.get(joinTables.get(index).getName()));
		for(String col: tempSchema.keySet()){
			newSchema.put(col, tempSchema.get(col)+size);
		}
	}

	void initializeAllTuple(){
		HashMap<String, Integer> newSchema = new HashMap<String, Integer>();
		HashMap<String, Integer> tempSchema = new HashMap<String, Integer>();
		Object[] temp1 = readOps.get(0).readOneTuple();
		Object[] temp2 = readOps.get(1).readOneTuple();
		Object[] temp3 = null;
		Object[] temp4 = null;
		int size1 = Utility.tables.get(joinTables.get(0).getAlias()).size();
		int size2 = Utility.tables.get(joinTables.get(1).getAlias()).size();
		int size3 = 0;
		int size4 = 0;
		ArrayList<String> dataType = new ArrayList<String>();

		updateSchema(newSchema, tempSchema, 0, 0, dataType);
		updateSchema(newSchema, tempSchema, size1, 1, dataType);

		String newTableName = joinTables.get(0).getAlias()+","+joinTables.get(1).getAlias();

		if(readOps.size() == 3){
			temp3 = readOps.get(2).readOneTuple();
			size3 = Utility.tables.get(joinTables.get(2)).size();
			updateSchema(newSchema, tempSchema, size2+size1, 2, dataType);
			newTableName += ","+joinTables.get(2);
		}
		if(readOps.size() == 4){
			temp4 = readOps.get(3).readOneTuple();
			size4 = Utility.tables.get(joinTables.get(3)).size();
			updateSchema(newSchema, tempSchema, size3+size2+size1, 3, dataType);
			newTableName += ","+joinTables.get(3);
		}

		this.table = new Table(newTableName, null);
		this.table.setAlias(newTableName);
		Utility.tables.put(newTableName, newSchema);
		Utility.tableSchema.put(newTableName, dataType);
		int size = size1+size2+size3+size4;
		while(temp1 != null){
			Object[] toReturn1 = new Object[size1];
			for(int i = 0; i < size1; i++){
				toReturn1[i] = temp1[i];
			}
			while(temp2 != null){
				Object[] toReturn2 = new Object[size2];
				for(int j=0; j < size2; j++){
					toReturn2[j] = temp2[j];
				}
				if(readOps.size() == 2){
					Object[] x = createTuple(toReturn1, toReturn2, null, null, size);
					allTuples.add(x);
				}
				while(temp3 != null){
					Object[] toReturn3 = new Object[size3];
					for(int j=0; j < size3; j++){
						toReturn3[j] = temp3[j];
					}
					if(readOps.size() == 3){
						Object[] x = createTuple(toReturn1, toReturn2, toReturn3, null, size);
						allTuples.add(x);
					}
					while(temp4 != null){
						Object[] toReturn4 = new Object[size4];
						for(int j=0; j < size4; j++){
							toReturn4[j] = temp4[j];
						}
						Object[] x = createTuple(toReturn1, toReturn2, toReturn3, toReturn4, size);
						allTuples.add(x);
						temp4 = readOps.get(3).readOneTuple();
					}//end of 4
					if(readOps.size() == 4){
						readOps.get(3).reset();
						temp4 = readOps.get(3).readOneTuple();
					}
					temp3 = readOps.get(2).readOneTuple();
				}//end of 3
				if(readOps.size() == 3)
				{
					readOps.get(2).reset();
					temp3 = readOps.get(2).readOneTuple();
				}
				temp2 = readOps.get(1).readOneTuple();
			}//end of 2
			readOps.get(1).reset();
			temp2 = readOps.get(1).readOneTuple();
			temp1 = readOps.get(0).readOneTuple();
		}// end of 1
	}


	public Object[] createTuple(Object[] toReturn1, Object[] toReturn2, Object[] toReturn3, Object[] toReturn4, int size){
		Object[] toReturn = new Object[size];
		int index = 0;
		for(int i=0; i<toReturn1.length; i++){
			toReturn[index] = toReturn1[i];
			index++;
		}

		for(int i=0; i<toReturn2.length; i++){
			toReturn[index] = toReturn2[i];
			index++;
		}

		if(toReturn3 != null){
			for(int i=0; i<toReturn3.length; i++){
				toReturn[index] = toReturn3[i];
				index++;
			}
		}

		if(toReturn4 != null){
			for(int i=0; i<toReturn4.length; i++){
				toReturn[index] = toReturn4[i];
				index++;
			}
		}

		return toReturn;
	}

	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return this.table;
	}
}
