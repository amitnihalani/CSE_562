package edu.buffalo.cse562.operators;

import java.io.File;

public class OperatorTest {

	public static void execute(File file){
		Operator oper = new ReadOperator(file);
		dump(oper);
	}
	
	public static void dump(Operator input){
		Object[] row = input.readOneTuple();
		while(row != null){
			for(Object col: row){
				System.out.print(col.toString()+" | ");
			}
			System.out.println();
			row = input.readOneTuple();
		}
	}
}
