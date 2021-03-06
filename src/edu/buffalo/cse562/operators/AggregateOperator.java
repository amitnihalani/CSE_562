package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.evaluate.Evaluator;

public class AggregateOperator implements Operator {

	Operator input;
	HashMap<String, Integer> schema;
	String fname;
	ArrayList<Function> functions;
	Table table;

	public AggregateOperator(Operator oper, HashMap<String, Integer> hashMap,
			ArrayList<Function> functions, Table table) {
		// TODO Auto-generated constructor stub
		this.input = oper;
		this.schema = hashMap;
		this.functions = functions;
		this.table = table;
	}

	@Override
	public void reset() {
		input.reset();
	}

	@Override
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		LeafValue l=null;
		Object[] obj = new Object[functions.size()];
		
		for(int i=0; i<functions.size(); i++){
			input.reset();
			String fname = functions.get(i).getName();
			if(fname.equalsIgnoreCase("SUM")){
				l = computeSum((Expression)functions.get(i).getParameters().getExpressions().get(0));
				if(l == null)
					return null;
				obj[i] = l;
			}
			else if(fname.equalsIgnoreCase("AVG")){
				l = computeAvg((Expression)functions.get(i).getParameters().getExpressions().get(0));
				if(l == null)
					return null;
				obj[i] = l;
			}
			else if(fname.equals("MIN")){
				l=computeMin((Expression)functions.get(i).getParameters().getExpressions().get(0));
				if(l == null)
					return null;
				obj[i] = l;
			}
			else if (fname.equals("MAX")){
				l=computeMax((Expression)functions.get(i).getParameters().getExpressions().get(0));
				if(l == null)
					return null;
				obj[i] = l;
			}
			else if(fname.equals("COUNT")){
			l=computeCount();
				if(l == null)
					return null;
				obj[i] = l;
			}
		}
		return obj;
	}

	public LeafValue computeSum(Expression expression)
	{
		Object[] tuple=null;
		tuple=input.readOneTuple();
		Double d=0.0;
		Long l=0L;
		int flag=0;
		if(tuple == null)
			return null;
		try {
			do{
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
				tuple=input.readOneTuple();

			}while(tuple!=null);
		} catch (SQLException e) {
			System.out.println("SQLException in AggregateOperator.computeSum()");
		}

		if(flag==1)
			return new LongValue(l.toString());
		return new DoubleValue(d.toString());
	}

	public LeafValue computeAvg(Expression expression)
	{
		Object[] tuple=null;
		tuple=input.readOneTuple();
		Double d=0.0;
		Long l=0L;
		int count=0;
		int flag=0;
		if(tuple == null)
			return null;

		try {
			do{

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
				tuple=input.readOneTuple();

			}while(tuple!=null);
		} catch (SQLException e) {
			System.out.println("SQLException in AggregateOperator.computeSum()");
		}

		if(flag==1){
			Double avg = l.doubleValue() / count;
			return new DoubleValue(avg.toString());
		}
		
		Double avg = d / count;
		return new DoubleValue(avg.toString());
	}

	public LeafValue computeMin(Expression expression)
	{
		Object[] tuple=null;
		tuple=input.readOneTuple();
		Double dmin=0.0;
		Long lmin=0L;

		int flag=0;
		if(tuple == null)
			return null;

		try {

			Evaluator eval1=new Evaluator(schema, tuple);
			LeafValue leaf1=eval1.eval(expression);
			if(leaf1 instanceof DoubleValue)
			{
				dmin=((DoubleValue) leaf1).getValue();
				flag=0;

			}
			else if(leaf1 instanceof LongValue)
			{
				lmin=((LongValue) leaf1).getValue();
				flag=1;
			}
			do{

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
				tuple=input.readOneTuple();

			}while(tuple!=null);
		} catch (SQLException e) {
			System.out.println("SQLException in AggregateOperator.computeSum()");
		}

		if(flag==1)
			return new LongValue(lmin.toString());
		return new DoubleValue(dmin.toString());
	}
	public LeafValue computeMax(Expression expression)
	{
		Object[] tuple=null;
		tuple=input.readOneTuple();
		Double dmax=0.0;
		Long lmax=0L;

		int flag=0;
		if(tuple == null)
			return null;

		try {

			Evaluator eval1=new Evaluator(schema, tuple);
			LeafValue leaf1=eval1.eval(expression);
			if(leaf1 instanceof DoubleValue)
			{
				dmax=((DoubleValue) leaf1).getValue();
				flag=0;

			}
			else if(leaf1 instanceof LongValue)
			{
				lmax=((LongValue) leaf1).getValue();
				flag=1;
			}
			do{

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
				tuple=input.readOneTuple();

			}while(tuple!=null);
		} catch (SQLException e) {
			System.out.println("SQLException in AggregateOperator.computeSum()");
		}

		if(flag==1)
			return new LongValue(lmax.toString());
		return new DoubleValue(dmax.toString());
	}

	public LeafValue computeCount()
	{
		Object[] tuple=null;
		tuple=input.readOneTuple();
		Integer count=0;
		if(tuple == null)
			return null;

		do{ 	
			count++;
			tuple=input.readOneTuple();

		}while(tuple!=null);


		return new LongValue(count.toString());
	}

	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return table;
	}
}
