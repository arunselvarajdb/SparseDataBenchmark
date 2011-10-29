import java.io.*;
import java.util.Random;
import java.util.Scanner;
public class DataGenerator {
	int noOfRecords;
	int NumOfSprsCol;
	String fileName;
	int noOfColumns;
	ColumnObject[] colObj;
	
	DataGenerator(int noOfRecords, String fileName, int noOfColumns, int NumOfSprsCol){ 
		
	
		this.noOfRecords = noOfRecords;
		this.fileName = fileName;
		this.noOfColumns=noOfColumns;
		this.NumOfSprsCol=NumOfSprsCol;
		colObj = new ColumnObject[noOfColumns];
	}
	
	void buildObject(){
		String pipeTemp = null;
		String attribute[];
		String ColumnName = null;
		String ColumnType = null;
		int ColumnLength = 0;
		String isNull = null;
		int group = 0;
		boolean sparse = false;
		String value=null;
		try{
			
			 FileInputStream fstream = new FileInputStream(fileName);
			  // Get the object of DataInputStream
			  DataInputStream in = new DataInputStream(fstream);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));
			  String strLine;
			  int i=0;
				  
			  while ((strLine = br.readLine()) != null)   
			  	{

				//System.out.println (strLine);
				String splitPipe[] = strLine.split("\\|");
				for(int n=0;n <splitPipe.length; n++)
				{
					pipeTemp=splitPipe[n];
					attribute= pipeTemp.split(":");
					//System.out.println (attribute[0]);
					//System.out.println (attribute[1]);
					if(attribute[0].equals("name"))
						ColumnName = attribute[1];
					else if(attribute[0].equals("type"))
						ColumnType= attribute[1];
					else if(attribute[0].equals("length"))
						ColumnLength = Integer.parseInt(attribute[1]);
					else if(attribute[0].equals("nullable"))
						isNull = attribute[1];
					else if(attribute[0].equals("group"))
						group = Integer.parseInt(attribute[1]);
					else if(attribute[0].equals("sparse"))
						sparse = attribute[1].equals("true")?true:false;	
				}
				
				colObj[i]= new ColumnObject(ColumnName,ColumnType,ColumnLength,isNull,group,sparse,value);
				
			i++;
				
			}

			  
			  //Close the input stream
			  in.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	void generateData()
	{
		
		OuputWriter pipe_out = new OuputWriter("/home/arun/pipe_auto");
		OuputWriter json_out = new OuputWriter("/home/arun/json_auto");
		pipe_out.open();
		json_out.open();
		for(int i=0; i<noOfRecords; i++)
		
		{
			int SparseColumns=NumOfSprsCol;
			int Spacing= noOfColumns/SparseColumns;
			int randomColSparse;
			int END=0;
			

			
			ColumnObject[] TempcolObj = new ColumnObject[noOfColumns];
	
			for (int k=0; k<noOfColumns; k++)
			{
			TempcolObj[k]= new ColumnObject(colObj[k].ColumnName,colObj[k].ColumnType,colObj[k].ColumnLength,colObj[k].isNull,colObj[k].group,colObj[k].sparse,colObj[k].value);
			}
			//TempcolObj=colObj;
			
			//assigning value to first column - unique key
			TempcolObj[0].value=Integer.toString(i+1);
			//System.out.println(noOfColumns);
			for(int j=1;j<noOfColumns && END < noOfColumns;j++)
			{
				//randomColSparse=rand()
			    int START = (j-1)*(int)Spacing + 1;
			    END = (j)*(int)Spacing;
			    
			    
			    if(END > noOfColumns)
			    	END=noOfColumns;
			    
			    Random random = new Random();
			    System.out.println("Spacing "+ Spacing);
			    System.out.println("Start " + START + " End " + END );
			    randomColSparse=RandomInteger(START, END, random);
			    //System.out.println(randomColSparse);
			    //System.out.println(j);
			    //System.out.println("Sparse column: "+ randomColSparse);
			    if (END < noOfColumns)
			    {
			    if(TempcolObj[randomColSparse].ColumnType.equals("int"))
		    	{
		    	int temp=((j+1)%(TempcolObj[j].group+1)); // need to include counter
		    	TempcolObj[randomColSparse].value=Integer.toString(temp);
			    }
		    	else if(TempcolObj[randomColSparse].ColumnType.equals("varchar"))
		    	{
		    		String temp = "string"; // need to parameterized
		    		TempcolObj[randomColSparse].value=temp + Integer.toString(j+1);
		    	
		    		
			    }
		    	else if(TempcolObj[randomColSparse].ColumnType.equals("float"))
		    	{
		    		Float temp = (float) 1.456; // need to parameterized
		    		TempcolObj[randomColSparse].value=Float.toString(temp*(j+1));
		    	
		    		
			    }
		    	else if(TempcolObj[randomColSparse].ColumnType.equals("date"))
		    	{
		    		String temp = "2011-10-01"; // need to parameterized
		    		TempcolObj[randomColSparse].value=temp;
		    		
			    }
		    	else
		    		System.out.println("Data type provided is not supported");
			      
			}
			
		}
			
			
			String temp = PipeDelimitedGeneration(TempcolObj);
			System.out.println(temp);
			pipe_out.write(temp);
			temp=JsonFormatGeneration(TempcolObj);
			System.out.println(temp);
			json_out.write(temp);
			/*
			for (int k=0; k<noOfColumns; k++)
			{
				
				//TempcolObj[k].ColumnName="asd";
				System.out.println(TempcolObj[k].ColumnName);
				System.out.println(TempcolObj[k].ColumnType);
				System.out.println(TempcolObj[k].ColumnLength);
				System.out.println(TempcolObj[k].isNull);
				System.out.println(TempcolObj[k].group);
				System.out.println(TempcolObj[k].sparse);
				System.out.println(TempcolObj[k].value);
			
			}
			*/
		}
		pipe_out.close();
		json_out.close();
		

		}

  
	public String PipeDelimitedGeneration(ColumnObject[] TempcolObj)
	{
		
		StringBuilder tempDelim = new StringBuilder();
		
			tempDelim.append(TempcolObj[0].value);
			for(int i=1;i<noOfColumns;i++)
			{
				tempDelim.append("|");
				if(TempcolObj[i].value!=null)
					tempDelim.append(TempcolObj[i].value);
				else
					tempDelim.append("");
			}
			tempDelim.append("\n");
		System.out.println(tempDelim.toString());
		return tempDelim.toString();	
			
	}
	
	
	public String JsonFormatGeneration(ColumnObject[] TempcolObj)
	{
	
		StringBuilder tempDelim = new StringBuilder();
		
		tempDelim.append("{");
		tempDelim.append((char)34);
		tempDelim.append(TempcolObj[0].ColumnName);
		tempDelim.append((char)34);
		tempDelim.append(":");
		tempDelim.append(TempcolObj[0].value);
		for(int i=1;i<noOfColumns;i++)
		{		
			if (TempcolObj[i].value!=null)
			{
				tempDelim.append(",");
				tempDelim.append((char)34);
				tempDelim.append(TempcolObj[i].ColumnName);
				tempDelim.append((char)34);
				tempDelim.append(":");
			if(TempcolObj[i].ColumnType.equals("varchar"))
			{
				tempDelim.append((char)34);
				tempDelim.append(TempcolObj[i].value);
				tempDelim.append((char)34);
			}
			else if(TempcolObj[i].ColumnType.equals("date"))
			{
				tempDelim.append("date(");
				tempDelim.append((char)34);
				tempDelim.append(TempcolObj[i].value);
				tempDelim.append((char)34);
				tempDelim.append(")");
			}
			else if(TempcolObj[i].ColumnType.equals("float"))
			{
				tempDelim.append("float(");
				tempDelim.append((char)34);
				tempDelim.append(TempcolObj[i].value);
				tempDelim.append((char)34);
				tempDelim.append(")");
			}
			else
			{
				tempDelim.append(TempcolObj[i].value);
			}
		}
		}
		tempDelim.append("}\n");
	System.out.println(tempDelim.toString());
	return tempDelim.toString();
		
	}
	
	  private static int RandomInteger(int aStart, int aEnd, Random aRandom){
	    if ( aStart > aEnd ) {
	      throw new IllegalArgumentException("Start cannot exceed End.");
	    }
	    long range = (long)aEnd - (long)aStart + 1;
	    long fraction = (long)(range * aRandom.nextDouble());
	    int randomNumber =  (int)(fraction + aStart);    
	  	return randomNumber;
	  }
	
}
