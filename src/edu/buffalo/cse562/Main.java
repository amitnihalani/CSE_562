package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import edu.buffalo.cse562.parsers.SelectParser;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class Main {

	static File dataDir = null;
	static ArrayList<File> sqlFiles;

	public static void main(String args[]){
		initialize(args);

		for(File sql: sqlFiles){
			FileReader fr = getFileReader(sql);
			parseWithJsql(fr);			
		}
	}

	/**
	 * Load the Data Directory and SQL Files
	 * @param args - the arguments that are passed to the program.
	 */
	private static void initialize(String args[]) {
		dataDir = new File(args[1]);
		sqlFiles = new ArrayList<File>();

		for(int i = 2; i < args.length; i++){ 
			try{
				File sql = new File(args[i]);
				sqlFiles.add(sql);
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
					System.out.println(statement.toString());
					SelectParser.parseStatement(statement);
				}
				else if(statement instanceof CreateTable){
				}
			}
		}catch(ParseException e){
			System.out.println("Invalid statement to parse or null. Encountered in parseWithJsql()");
		}
	}
	
//	/**
//	 * Takes a statement object for CreateTable and reads the corresponding table from the data directory. 
//	 * Table is stored as 'table.dat'
//	 * @param statement - a CreateTable statement
//	 */
//	private static void loadTable(Statement statement){
//		String dataFileName = statement.toString().split(" ")[2].toLowerCase() + ".dat";
//		dataFileName = dataDir.toString() + File.separator + dataFileName;
//		File dataFile = new File(dataFileName);
//		FileReader fr = getFileReader(dataFile);
//		BufferedReader br = new BufferedReader(fr);
//		
//		String temp = "";
//		try {
//			while((temp = br.readLine())!=null){
//				
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			System.out.println("IO Exception encountered in loadTable()");
//		}
//	}

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
