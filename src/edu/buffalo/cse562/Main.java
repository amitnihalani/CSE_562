package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import edu.buffalo.cse562.parsers.CreateTableParser;
import edu.buffalo.cse562.parsers.SelectParser;
import edu.buffalo.cse562.utility.Utility;

public class Main {

	public static void main(String args[]){
		initialize(args);
		Utility.tables = new HashMap<String, HashMap<String, Integer>>();
		for(File sql: Utility.sqlFiles){
			FileReader fr = getFileReader(sql);
			parseWithJsql(fr);			
		}
	}// end of main

	/**
	 * Load the Data Directory and SQL Files
	 * @param args - the arguments that are passed to the program.
	 */
	private static void initialize(String args[]) {
		Utility.tableSchema = new HashMap<String,ArrayList<String>>();
		Utility.dataDir = new File(args[1]);
		Utility.sqlFiles = new ArrayList<File>();

		for(int i = 2; i < args.length; i++){ 
			try{
				File sql = new File(args[i]);
				Utility.sqlFiles.add(sql);
			}
			catch(NullPointerException e){
				System.out.println("Null pointer exception encountered in initialize()");
			}
		}
	}

	/**
	 * Takes a FileReader object and parses it using JsqlParser
	 * @param inputFile The FileReader object to parse
	 * @return void
	 */
	private static void parseWithJsql(FileReader inputFile) {
		try{
			CCJSqlParser parser = new CCJSqlParser(inputFile);
			Statement statement = null;
			while((statement  = parser.Statement()) != null){
				if(statement instanceof Select){
					SelectParser.parseStatement(statement);
				}
				else if(statement instanceof CreateTable){
					CreateTableParser.parseStatement(statement);
				}
			}
		}catch(ParseException e){
			System.out.println("Invalid statement to parse or null. Encountered in parseWithJsql()");
		}
	}

	/**
	 * Takes String arguments from the console and returns a FileReader object
	 * @param args The String arguments
	 * @return inputFile The FileReader object
	 * @exception FileNotFoundException e
	 */
	private static FileReader getFileReader(File file){
		FileReader inputFile = null;
		try{
			inputFile = new FileReader(file);
		}catch(FileNotFoundException e){
			System.out.println("The file could not be found. Encountered in getFileReader()");
			System.exit(0);
		}
		return inputFile;
	}
}

