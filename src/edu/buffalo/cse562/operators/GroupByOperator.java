/**
 * 
 */
package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.buffalo.cse562.evaluate.Evaluator;
import edu.buffalo.cse562.utility.Utility;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;


public class GroupByOperator implements Operator {

	public  HashMap<String, ArrayList<Object[]>> groupByTuples;
	public  HashMap<String, ArrayList<Object>> computedGroupedByValues;
	public  HashMap<String, Integer> sch = new HashMap<String, Integer>();

	Operator op;
	String tableName;
	HashMap<String, Integer> schema;
	ArrayList<SelectExpressionItem> list;
	ArrayList<Expression> grpByColumns = new ArrayList<Expression>();
	Expression having;
	String groupByKey = null;
	ArrayList<Function> functions;
	ArrayList<Object[]> answer;
	static int counter;
	static int schIndex = 0;

	public GroupByOperator(Operator oper, String tableName, ArrayList<SelectExpressionItem> list, ArrayList<Function> functions, ArrayList<Expression> grpByColumns, Expression having){
		this.groupByTuples = new HashMap<String, ArrayList<Object[]>>();
		this.computedGroupedByValues = new HashMap<String, ArrayList<Object>>();
		this.op = oper;
		this.tableName = tableName;
		this.schema = Utility.tables.get(tableName);
		this.list = list;
		this.grpByColumns = grpByColumns;
		this.having = having;
		this.functions = functions;
		this.answer = new ArrayList<Object[]>();
		counter = 0;
		schIndex = 0;
		sch = new HashMap<String, Integer>();
		while(schIndex < grpByColumns.size()){
			if(Utility.alias.containsKey(grpByColumns.get(schIndex).toString())){
				sch.put(Utility.alias.get(grpByColumns.get(schIndex).toString()).toString(), schIndex);
			}else{
				sch.put(grpByColumns.get(schIndex).toString(), schIndex);
			}
			schIndex++;
		}
		

		initialize();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.operators.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		counter = 0;

	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.operators.Operator#readOneTuple()
	 */
	@Override
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		Object[] temp = null;
		if(counter < answer.size()){
			temp = answer.get(counter);
			counter++;
			if(having !=null)
			{
				Evaluator eval=new Evaluator(Utility.tables.get(tableName), temp);
				try {
					BooleanValue b= (BooleanValue)eval.eval(having);
					if(b.getValue())
						return temp;
					else
					{
						temp=readOneTuple();
						if(temp== null)
						{
							return null;
						}
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		return temp;
	} //readOneTuple

	void initialize(){
		Object[] tuple=  null;
		tuple=op.readOneTuple();
		try {
			while(tuple != null){
				Evaluator eval= new Evaluator(schema,tuple);		
				groupByKey = eval.eval(grpByColumns.get(0)).toString();
				for(int i = 1; i<grpByColumns.size(); i++){
					groupByKey += ","+eval.eval(grpByColumns.get(i)).toString();
				}
				if(!groupByTuples.containsKey(groupByKey)){
					ArrayList<Object[]> tupleArray = new ArrayList<Object[]>();
					tupleArray.add(tuple);
					groupByTuples.put(groupByKey, tupleArray);
				}else	
					groupByTuples.get(groupByKey).add(tuple);
				tuple = op.readOneTuple();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		computeFunctionValues();
	}

	public void computeFunctionValues(){
		for(String key : groupByTuples.keySet())
		{
			LeafValue l=null;
			ArrayList<Object> computedAggValues = new ArrayList<Object>();;
			//System.out.print(key + "  :-");
			ArrayList<Object[]> singleKeyTuples =groupByTuples.get(key);

			for(int i=0; i<functions.size();i++){
				String fname = functions.get(i).getName();
				if(!sch.containsKey(functions.get(i).toString())){
					sch.put(functions.get(i).toString(), schIndex);
					schIndex++;
				}
				if(fname.equalsIgnoreCase("SUM")){
					l = computeSum(singleKeyTuples, (Expression)functions.get(i).getParameters().getExpressions().get(0));						
					computedAggValues.add(l);
				}else if(fname.equalsIgnoreCase("AVG")){
					l = computeAvg(singleKeyTuples, (Expression)functions.get(i).getParameters().getExpressions().get(0));						
					computedAggValues.add(l);
				}else if(fname.equalsIgnoreCase("MIN")){
					l = computeMin(singleKeyTuples, (Expression)functions.get(i).getParameters().getExpressions().get(0));						
					computedAggValues.add(l);					
				}else if(fname.equalsIgnoreCase("MAX")){
					l = computeMax(singleKeyTuples, (Expression)functions.get(i).getParameters().getExpressions().get(0));						
					computedAggValues.add(l);					
				}else if(fname.equalsIgnoreCase("COUNT")){
					l = computeCount(singleKeyTuples);						
					computedAggValues.add(l);					
				}
			}

			if(computedGroupedByValues.get(key) == null){
				computedGroupedByValues.put(key, computedAggValues);
			}else{
				computedGroupedByValues.get(key).add(computedAggValues);
			}

		}


		
		ArrayList<Object> temp = null;
		for(Object key : computedGroupedByValues.keySet()){
			temp = new ArrayList<Object>();
			String[] k = key.toString().split(",");
			for(int i = 0; i<k.length; i++){
				temp.add(getValue(k[i],i));
			}
			ArrayList<Object> obj=computedGroupedByValues.get(key);
			for(Object o : obj){
				temp.add(o);
			}
			answer.add(temp.toArray());
		}
		this.tableName = tableName+"-GroupBy";
		HashMap<String, Integer> schema = new HashMap<String, Integer>();
		for(SelectExpressionItem e: list){
			schema.put(tableName+"."+e.getExpression().toString(), sch.get(e.getExpression().toString()));
		}
		Utility.tables.put(this.tableName, schema);
		
	}


	private Object getValue(String s, int i) {
		// TODO Auto-generated method stub
		String col = grpByColumns.get(i).toString();
		int index=0;
		if(Utility.alias.containsKey(col))
		{
			index=Utility.tables.get(tableName).get(tableName+"."+Utility.alias.get(col).toString());
		}
		else{
			if(col.contains("."))
				index = Utility.tables.get(tableName).get(col);
			else{
				if(tableName.contains(",")){
					for(String key: schema.keySet()){
						String x = key.substring(key.indexOf(".") + 1, key.length());
						if(x.equals(col)){
							index = schema.get(key);
						}
					}
				}else{
					index = Utility.tables.get(tableName).get(tableName+"."+col);
				}
			}
				
		}
		ArrayList<String> dataType = Utility.tableSchema.get(tableName);
		switch(dataType.get(index)){
		case "int": 
			return new LongValue(s); 
		case "decimal": 
			return new DoubleValue(s); 
		case "date": 
			return new DateValue(s); 
		case "char": 
			return new StringValue(" "+s+" "); 
		case "string": 
			return new StringValue(" "+s+" "); 
		case "varchar": 
			return new StringValue(" "+s+" "); 
		}
		return null;
	}

	public LeafValue computeSum(ArrayList<Object[]> tupleList, Expression expression)
	{
		//		Object[] tuple=null;
		//		tuple=input.readOneTuple();
		Double d=0.0;
		Long l=0L;
		int flag=0;
		for(Object[] tuple : tupleList){
			if(tuple == null)
				return null;

			try {
				Evaluator eval=new Evaluator(schema, tuple);
				LeafValue leaf=eval.eval(expression);

				if(leaf instanceof DoubleValue)
				{
					d+=((DoubleValue) leaf).getValue();
					flag=0;
				}
				else if(leaf instanceof LongValue)
				{
					l+=((LongValue) leaf).getValue();
					flag=1;
				}

			} catch (SQLException e) {
				System.out.println("SQLException in AggregateOperator.computeSum()");
			}
		}

		if(flag==1)
			return new LongValue(l.toString());
		return new DoubleValue(d.toString());
	}

	public LeafValue computeAvg(ArrayList<Object[]> tupleList, Expression expression)
	{
		Double d=0.0;
		Long l=0L;
		int count=0;
		int flag=0;
		for(Object[] tuple : tupleList){
			if(tuple == null)
				return null;

			try {				
				count++;
				Evaluator eval=new Evaluator(schema, tuple);
				LeafValue leaf=eval.eval(expression);

				if(leaf instanceof DoubleValue)
				{
					d+=((DoubleValue) leaf).getValue();
					flag=0;
				}
				else if(leaf instanceof LongValue)
				{
					l+=((LongValue) leaf).getValue();
					flag=1;
				}									
			} catch (SQLException e) {
				System.out.println("SQLException in AggregateOperator.computeSum()");
			}
		}

		if(flag==1){
			Double avg = l.doubleValue() / count;
			return new DoubleValue(avg.toString());
		}

		Double avg = d / count;
		return new DoubleValue(avg.toString());
	}

	public LeafValue computeMin(ArrayList<Object[]> tupleList, Expression expression)
	{	
		Double dmin= Double.MAX_VALUE;
		Long lmin=Long.MAX_VALUE;
		int flag=0;
		

		for(Object[] tuple : tupleList){
			if(tuple == null)
				return null;

			try {				
//				
				Evaluator eval=new Evaluator(schema, tuple);
				LeafValue leaf=eval.eval(expression);

				if(leaf instanceof DoubleValue && ((DoubleValue) leaf).getValue()<dmin)
				{
					dmin=((DoubleValue) leaf).getValue();
					flag=0;
				}
				else if(leaf instanceof LongValue && ((LongValue) leaf).getValue()<lmin)
				{
					lmin=((LongValue) leaf).getValue();
					flag=1;
				}					
			} catch (SQLException e) {
				System.out.println("SQLException in AggregateOperator.computeSum()");
			}
		}

		if(flag==1)
			return new LongValue(lmin.toString());

		return new DoubleValue(dmin.toString());
	}

	public LeafValue computeMax(ArrayList<Object[]> tupleList, Expression expression)
	{	
		Double dmax=Double.MIN_VALUE;
		Long lmax=Long.MIN_VALUE;
		int flag=0;
		for(Object[] tuple : tupleList){
			if(tuple == null)
				return null;

			try {				

				Evaluator eval=new Evaluator(schema, tuple);
				LeafValue leaf=eval.eval(expression);

				if(leaf instanceof DoubleValue && ((DoubleValue) leaf).getValue()>dmax)
				{
					dmax=((DoubleValue) leaf).getValue();
					flag=0;
				}
				else if(leaf instanceof LongValue && ((LongValue) leaf).getValue()>lmax)
				{
					lmax=((LongValue) leaf).getValue();
					flag=1;
				}								
			} catch (SQLException e) {
				System.out.println("SQLException in AggregateOperator.computeSum()");
			}
		}

		if(flag==1)
			return new LongValue(lmax.toString());
		return new DoubleValue(dmax.toString());
	}

	public LeafValue computeCount(ArrayList<Object[]> tupleList)
	{
		Integer count=0;

		for(Object[] tuple : tupleList){
			if(tuple == null)
				return null;

			count++;			
		}
		return new LongValue(count.toString());
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return this.tableName;
	}

}
