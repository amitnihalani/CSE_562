package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.HashMap;

import edu.buffalo.cse562.evaluate.Evaluator;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;

public class AggregateOperator implements Operator {

	Operator input;
	HashMap<String, Integer> schema;
	Expression column;
	String fname;
	public AggregateOperator(Operator input, HashMap<String, Integer> schema, Expression column, String fname) {
		this.input=input;
		this.schema=schema;
		this.column=column;
		this.fname=fname;
	}

	public AggregateOperator(Operator input, HashMap<String, Integer> schema,
			String fname) {
		this.input=input;
		this.schema=schema;
		this.fname=fname;
	}

	@Override
	public void reset() {
		input.reset();

	}

	@Override
	public Object[] readOneTuple() {
		// TODO Auto-generated method stub
		LeafValue l=null;
		if(fname.equalsIgnoreCase("SUM"))
			l=computeSum();
		else if(fname.equalsIgnoreCase("AVG"))
			l=computeAvg();
		else if(fname.equals("MIN"))
			l=computeMin();
		else if (fname.equals("MAX"))
			l=computeMax();
		else if(fname.equals("COUNT"))
			l=computeCount();
		Object[] obj=null;
		if(l != null)
		{
			obj=new Object[1];
			obj[0]=l;
		}
		
		return obj;
	}

	public LeafValue computeSum()
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
				LeafValue leaf=eval.eval(column);

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
			return new LongValue(l);
		return new DoubleValue(d);
	}

	public LeafValue computeAvg()
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
				LeafValue leaf=eval.eval(column);

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
			return new DoubleValue(l.doubleValue()/count);
		return new DoubleValue(d/count);
	}

	public LeafValue computeMin()
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
			LeafValue leaf1=eval1.eval(column);
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
				LeafValue leaf=eval.eval(column);

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
			return new LongValue(lmin);
		return new DoubleValue(dmin);
	}
	public LeafValue computeMax()
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
			LeafValue leaf1=eval1.eval(column);
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
				LeafValue leaf=eval.eval(column);

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
			return new LongValue(lmax);
		return new DoubleValue(dmax);
	}

	public LeafValue computeCount()
	{
		Object[] tuple=null;
		tuple=input.readOneTuple();
		int count=0;
		if(tuple == null)
			return null;

		do{ 	
			count++;
			tuple=input.readOneTuple();

		}while(tuple!=null);


		return new LongValue(count);

	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return null;
	}
}
