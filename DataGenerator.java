import java.io.*;
import java.util.Random;
import java.util.Scanner;
public class DataGenerator {
	int noOfRecords;
	int NumOfSprsCol;
	String fileName;
	int noOfColumns;
	String pipeSQLDelimitFile;
	String pipeDB2DelimitFile;
	String JsonDelimitFile;
	ColumnObject[] colObj;
	int prime, generator;
	
	DataGenerator(int noOfRecords, String fileName, int noOfColumns, int NumOfSprsCol, String pipeSQLDelimitFile, String pipeDB2DelimitFile, String JsonDelimitFile){ 
		
	
		this.noOfRecords = noOfRecords;
		this.fileName = fileName;
		this.noOfColumns=noOfColumns;
		this.NumOfSprsCol=NumOfSprsCol;
		colObj = new ColumnObject[noOfColumns];
		this.pipeDB2DelimitFile=pipeDB2DelimitFile;
		this.pipeSQLDelimitFile=pipeSQLDelimitFile;
		this.JsonDelimitFile=JsonDelimitFile;
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
		int seed;
		
		OuputWriter pipe_sql_out = new OuputWriter(pipeSQLDelimitFile);
		OuputWriter pipe_db2_out = new OuputWriter(pipeDB2DelimitFile);
		OuputWriter json_out = new OuputWriter(JsonDelimitFile);
		pipe_sql_out.open();
		pipe_db2_out.open();
		json_out.open();
		
		if
		(noOfColumns <= 1000)
		{ 
			generator = 279; 
			prime = 1009;
		
		}
		else if (noOfColumns <= 10000)
		{ 
			generator = 2969; 
			prime = 10007;
		}
		else if (noOfColumns <= 100000) 
		{ 
			generator = 21395;
			prime = 100003;
		}
		else if (noOfColumns <= 1000000)
		{ generator = 2107;
		prime = 100000;
		}
		else if (noOfColumns <= 10000000)
		{ generator = 211;
		prime = 10000;
		}
		else if (noOfColumns <= 100000000)
		{ generator = 21;
		prime = 100000;
		}
		else { System.out.println("too many rows requested\n"); }
		
		seed=generator;

		for(int i=0; i<noOfRecords; i++)
		
		{
			int SparseColumns=NumOfSprsCol;
			int Spacing= (int)Math.round((double) noOfColumns/ (double)SparseColumns);
			//float double_Spacing= noOfColumns/ (float)SparseColumns;
			System.out.println(Spacing);
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
			for(int j=0;j<NumOfSprsCol;j++)
			{
				System.out.println(seed);
				seed = rand(seed,noOfColumns-1);
				System.out.println(seed);
				
			    Random random = new Random();
			    //System.out.println("Spacing "+ Spacing);
			    //System.out.println("Start " + START + " End " + END );
			    randomColSparse=seed;
			    //System.out.println(randomColSparse);
			    //System.out.println(j);
			    //System.out.println("Sparse column: "+ randomColSparse);
			    if (END < noOfColumns)
			    {
			    if(TempcolObj[randomColSparse].ColumnType.equals("int"))
		    	{
		    	int temp=(Integer.parseInt(TempcolObj[0].value) % 4) + randomColSparse ; // need to include counter
		    	TempcolObj[randomColSparse].value=Integer.toString(temp);
			    }
		    	else if(TempcolObj[randomColSparse].ColumnType.equals("varchar"))
		    	{
		    		String temp = "string"; // need to parameterized
		    		TempcolObj[randomColSparse].value=temp + (Integer.parseInt(TempcolObj[0].value) % 4) + randomColSparse;
		    		
			    }
		    	else if(TempcolObj[randomColSparse].ColumnType.equals("float"))
		    	{
		    		Float temp = (float)(Integer.parseInt(TempcolObj[0].value)) % (float)4 + ((float)1/(float)randomColSparse); // need to parameterized
		    		TempcolObj[randomColSparse].value=Float.toString(temp);
		    	
		    		
			    }
		    	else if(TempcolObj[randomColSparse].ColumnType.equals("date"))
		    	{
		    		int monthValue=Integer.parseInt(TempcolObj[0].value) % 4 + 1;
		    		int dateValue=Integer.parseInt(TempcolObj[0].value) % 27 + 1;
		    		String temp = "2011-"+Integer.toString(monthValue)+"-"+Integer.toString(dateValue);// need to parameterized
		    		TempcolObj[randomColSparse].value=temp;
		    		
			    }
		    	else
		    		System.out.println("Data type provided is not supported");
			      
			}
			
		}
			
			
			String temp = PipeDB2DelimitedGeneration(TempcolObj);
			//System.out.println(temp);
			pipe_db2_out.write(temp);
			
			
			temp = PipeSQLServerDelimitedGeneration(TempcolObj);
			pipe_sql_out.write(temp);
			
			temp=JsonFormatGeneration(TempcolObj);
			//System.out.println(temp);
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
		pipe_db2_out.close();
		pipe_sql_out.close();
		json_out.close();
		

		}

  
	public String PipeSQLServerDelimitedGeneration(ColumnObject[] TempcolObj)
	{
		
		StringBuilder tempDelim = new StringBuilder();
		int count=0;
		
			tempDelim.append(TempcolObj[0].value);
			for(int i=1;i<noOfColumns;i++)
			{
				tempDelim.append("|");
				if(TempcolObj[i].value!=null)
				{
					tempDelim.append(TempcolObj[i].value);
					count=count+1;
				}
				else
				{
					tempDelim.append("");
					
				}
					
			}
			tempDelim.append(";");
		//System.out.println(tempDelim.toString());
		System.out.println("number of columns without null " + count);
		return tempDelim.toString();	
			
	}
	
	
	public String PipeDB2DelimitedGeneration(ColumnObject[] TempcolObj)
	{
		
		StringBuilder tempDelim = new StringBuilder();
		int count=0;
		
			tempDelim.append(TempcolObj[0].value);
			for(int i=1;i<noOfColumns;i++)
			{
				tempDelim.append("|");
				if(TempcolObj[i].value!=null)
				{
					tempDelim.append(TempcolObj[i].value);
					count=count+1;
				}
				else
				{
					tempDelim.append("");
					
				}
					
			}
			tempDelim.append("\n");
		//System.out.println(tempDelim.toString());
		System.out.println("number of columns without null " + count);
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
	//System.out.println(tempDelim.toString());
	return tempDelim.toString();
		
	}
	
	
	/* generate a unique random number between 1 and limit*/
	private int rand (int seed, int limit)
	{
		do 
		{ 
			seed = (generator * seed) % prime; 
		} while (seed > limit);
	return (seed);
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
