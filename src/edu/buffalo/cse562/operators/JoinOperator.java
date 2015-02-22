package edu.buffalo.cse562.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.Set;

import net.sf.jsqlparser.statement.select.Join;
import edu.buffalo.cse562.utility.Utility;

public class JoinOperator implements Operator {

	ArrayList<ReadOperator> readOps;
	ArrayList<Object[]> allTuples;
	ArrayList<Set<String>> commonColumns;
	ArrayList<String> tableNames;

	public JoinOperator(Operator oper, ArrayList<Join> joins, String t) {
		// TODO Auto-generated constructor stub
		allTuples = null;
		commonColumns = null;
		readOps = new ArrayList<ReadOperator>();
		tableNames = new ArrayList<String>();

		readOps.add((ReadOperator)oper);
		tableNames.add(t);
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
		// TODO Auto-generated method stub
	}

	@Override
	public Object[] readOneTuple() {
		Object[] temp1 = null;
		Object[] temp2 = null;
//		commonColumns = getCommonColumns();
		//		for(int i = 0; i < readOps.size(); i++){
		//			temp1 = readOps.get(0).readOneTuple();
		//			temp2 = readOps.get(1).readOneTuple();
		//			while(temp2 != null){
		//				allTuples = joinTuples(temp1, temp2);
		//				if(allTuples != null)
		//					System.out.println(allTuples.get(0)[0]+" "+allTuples.get(0)[1]+" "+allTuples.get(1)[0]+" "+allTuples.get(1)[1]);
		//				temp2 = readOps.get(1).readOneTuple();
		//			}
		//		}
		return null;
	}

	private Object[] joinTuples(Object[] temp1, Object[] temp2) {
		//		ArrayList<Object[]> tempTuple = new ArrayList<Object[]>();
		//		for(String column: commonColumns){
		//			Object x = temp1[Utility.tables.get(tableNames.get(0)).get(column)];
		//			Object y = temp2[Utility.tables.get(tableNames.get(1)).get(column)];
		//			if(x.equals(y)){
		//				tempTuple.add(temp1);
		//				tempTuple.add(temp2);
		//				return tempTuple;
		//			}
		//		}
		//		return null;
		Object[] tempTuple = new Object[temp1.length + temp2.length];
		int index = 0;
//		for(String column: commonColumns){
//			Object x = temp1[Utility.tables.get(tableNames.get(0)).get(column)];
//			Object y = temp2[Utility.tables.get(tableNames.get(1)).get(column)];
//			if(x.equals(y)){
//				for(int j = 0; j < temp1.length; j++){
//					tempTuple[index] = temp1[j];
//					index++;
//				}
//				for(int j = 0; j < temp2.length; j++){
//					tempTuple[index] = temp2[j];
//					index++;
//				}
//				return tempTuple;
//			}
//		}
		return null;
	}//end of function

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Function to get common columns of two tuples. 
	 * Example Table X(A,B), Table Y(B,C) would return B as the common column
	 * @return Set<String> - a set containing common columns in both tables
	 */
//	private Set<String> getCommonColumns(){
//		commonColumns = Utility.tables.get(tableNames.get(0)).keySet();
//		for(int i = 1; i < tableNames.size(); i++){
//			commonColumns.retainAll(Utility.tables.
//					get(tableNames.get(1)).keySet());
//		}
//		return commonColumns;
//	}
}
