package edu.buffalo.cse562.parsers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.utility.Utility;

public class CreateTableParser {

	/**Create Table Schema and store in a hash map as (TableName, Columns) 
	 * 
	 * @param statement
	 */
	public static void parseStatement(Statement statement){
		String tableName = ((CreateTable)statement).getTable().getName();
		HashMap<String, Integer> cols = new HashMap<String, Integer>();
		Utility.tableSchema = new HashMap<String,ArrayList<String>>();
		ArrayList<String> dataType = new ArrayList<String>();
		
		if(Utility.tables != null && !Utility.tables.containsKey(tableName)){
			@SuppressWarnings("unchecked")
			List<ColumnDefinition> list = ((CreateTable)statement).getColumnDefinitions();
			for(int i=0; i<list.size();i++){
				ColumnDefinition temp = list.get(i);
				cols.put(temp.getColumnName(), i);
				dataType.add(temp.getColDataType().toString());
			}
			Utility.tables.put(tableName, cols);
			Utility.tableSchema.put(tableName, dataType);
		}//end if
	}
}
