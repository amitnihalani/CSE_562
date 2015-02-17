package edu.buffalo.cse562.parsers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.table.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class CreateTableParser {

	public static void parseStatement(Statement statement){
		//CreateTableBody body = ((CreateTable) statement).getSelectBody();
		//((CreateTable)statement).getColumnDefinitions();
		String tableName = ((CreateTable)statement).getTable().getName();
		
		if(Main.tables != null && !Main.tables.containsKey(tableName)){
			List<ColumnDefinition> l = ((CreateTable)statement).getColumnDefinitions();
			ArrayList<Column> columns = new ArrayList<Column>();
			try{

				for(int i=0; i<l.size();i++){			 
					String name = l.get(i).getColumnName();
					ColDataType dataType = l.get(i).getColDataType();
					Column c = new Column(name, dataType.toString());
					columns.add(c);				
				}
				Main.tables.put(tableName, columns);
	
			} catch(Exception ex){
				System.out.println("Table already exists:" + ex);
			}
		}
			
		
	}
}
