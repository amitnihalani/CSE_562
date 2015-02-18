package edu.buffalo.cse562.parsers;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.table.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class CreateTableParser {

	/**
	 * 
	 * @param statement
	 */
	public static void parseStatement(Statement statement){
		String tableName = ((CreateTable)statement).getTable().getName();

		if(Main.tables != null && !Main.tables.containsKey(tableName)){
			@SuppressWarnings("unchecked")
			List<ColumnDefinition> list = ((CreateTable)statement).getColumnDefinitions();
			ArrayList<Column> columns = new ArrayList<Column>();
			for(int i=0; i<list.size();i++){			 
				String name = list.get(i).getColumnName();
				ColDataType dataType = list.get(i).getColDataType();
				Column c = new Column(name, dataType.toString());
				columns.add(c);				
			}
			Main.tables.put(tableName, columns);
		}
	}
}
