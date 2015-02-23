package edu.buffalo.cse562.utility;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.expression.Expression;

public class Utility {

	public static File dataDir = null;
	public static ArrayList<File> sqlFiles;
	public static HashMap<String, HashMap<String, Integer>> tables = null;
	public static HashMap<String, ArrayList<String>> tableSchema = null;
	public static HashMap<String, Expression> alias=null;
	
}
