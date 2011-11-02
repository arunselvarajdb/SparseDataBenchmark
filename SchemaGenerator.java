import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;


public class SchemaGenerator {
	int noOfColumns;
	int sparsity;
	int noOfNonSparseColumns;
	String fileName;
	
	SchemaGenerator(int noOfColumns, int sparsity, int  noOfNonSparseColumns, String fileName){
		this.noOfColumns = noOfColumns;
		this.sparsity = sparsity;
		this.noOfNonSparseColumns = noOfNonSparseColumns;
		this.fileName = fileName;
	}

	void generateSchema(){
		OuputWriter out = new OuputWriter(fileName);
		out.open();
		StringBuilder sb = new StringBuilder();
		sb.append("name:C0|");
		sb.append("type:int|");
		sb.append("length:4|");
		sb.append("nullable:no|");
		sb.append("group:0|");
		sb.append("sparse:false|");
		sb.append("\n");
		out.write(sb.toString());
		sb.setLength(0);
		int temp = 0;
		
		for(int i=1;i<noOfColumns;i++){
			
			//Column name 
			sb.append("name:C"+i+"|");
			
			//Datatype
			if((i-1)%4==0)
			{
				sb.append("type:int|");
				sb.append("length:4|");
				sb.append("nullable:yes|");
			}
			else if((i-1)%4==1)
			{
			
				sb.append("type:int|");
				sb.append("length:4|");
				sb.append("nullable:yes|");
			}
			else if((i-1)%4==2)
			{
				sb.append("type:int|");
				sb.append("length:4|");
				sb.append("nullable:yes|");
			}
			else if((i-1)%4==3)
			{
				sb.append("type:int|");
				sb.append("length:4|");
				sb.append("nullable:yes|");
			}
			//Group to determine mod function
			temp = (i%4)+1;
			sb.append("group:"+temp+"|");
			
			//Set Sparse property of column
			//if(i<=noOfNonSparseColumns)
				sb.append("sparse:false|");
			//else
				//sb.append("sparse:true|");
			
			sb.append("\n");
			out.write(sb.toString());
			sb.setLength(0);
			
			
		}
		out.close();
	}


void AsterixCreateTable(String SchemaFileName, String AsterixFileName)
{
	String pipeTemp=null;
	String attribute[];
	String ColumnName=null;
	String ColumnType=null;
	int ColumnLength;
	String isNull=null;
	
	try{
		
		 FileInputStream fstream = new FileInputStream(SchemaFileName);
		  // Get the object of DataInputStream
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  
		  OuputWriter out = new OuputWriter(AsterixFileName);
		  out.open();
		  
		  StringBuilder temp = new StringBuilder();
		  
		  temp.append("create type SparseType as open {\n");
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
				

					
			}
			if (ColumnType.equals("int"))
				temp.append(ColumnName + ": " + "int32");
			else if (ColumnType.equals("varchar"))
				temp.append(ColumnName + ": " + "string");
			else if (ColumnType.equals("date"))
				temp.append(ColumnName + ": " + "date");
			else if (ColumnType.equals("float"))
				temp.append(ColumnName + ": " + "float");
			
			if (isNull.equals("yes"))
				temp.append(" ?");
			
			if (i != noOfColumns-1)
				temp.append(",\n");
				
			i++;
			
			
			
		}
		  temp.append("\n}");
		  
		  out.write(temp.toString());
		  
		  //Close the input stream
		  in.close();
		  out.close();
		
	}catch(Exception e){
		e.printStackTrace();
	}

}
	
void Db2CreateTable(String SchemaFileName, String DB2FileName)
{
	String pipeTemp=null;
	String attribute[];
	String ColumnName=null;
	String ColumnType=null;
	int ColumnLength = 0;
	String isNull=null;
	
	try{
		
		 FileInputStream fstream = new FileInputStream(SchemaFileName);
		  // Get the object of DataInputStream
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  
		  OuputWriter out = new OuputWriter(DB2FileName);
		  out.open();
		  
		  StringBuilder temp = new StringBuilder();
		  
		  temp.append("CREATE TABLE SPARSEDATA (\n");
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
				

					
			}
			if (ColumnType.equals("int"))
				temp.append(ColumnName + " INTEGER");
			else if (ColumnType.equals("varchar"))
			{
				temp.append(ColumnName + " VARCHAR(");
				temp.append(ColumnLength + ")");
			}
			else if (ColumnType.equals("date"))
				temp.append(ColumnName + " DATE");
			else if (ColumnType.equals("float"))
				temp.append(ColumnName + " REAL");
			
			if (isNull.equals("no"))
				temp.append(" NOT NULL");
			
			if (i != noOfColumns-1)
				temp.append(",\n");
				
			i++;
			
			
			
		}
		  temp.append("\n)");
		  
		  out.write(temp.toString());
		  
		  //Close the input stream
		  in.close();
		  out.close();
		
	}catch(Exception e){
		e.printStackTrace();
	}


}


void SQLServerCreateTable(String SchemaFileName, String SQLServerFileName)
{

	String pipeTemp=null;
	String attribute[];
	String ColumnName=null;
	String ColumnType=null;
	int ColumnLength = 0;
	String isNull=null;
	
	try{
		
		 FileInputStream fstream = new FileInputStream(SchemaFileName);
		  // Get the object of DataInputStream
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  
		  OuputWriter out = new OuputWriter(SQLServerFileName);
		  out.open();
		  
		  StringBuilder temp = new StringBuilder();
		  
		  temp.append("CREATE TABLE SPARSEDATA (\n");
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
				

					
			}
			if (ColumnType.equals("int"))
				temp.append(ColumnName + " INT");
			else if (ColumnType.equals("varchar"))
			{
				temp.append(ColumnName + " VARCHAR(");
				temp.append(ColumnLength + ")");
			}
			else if (ColumnType.equals("date"))
				temp.append(ColumnName + " DATE");
			else if (ColumnType.equals("float"))
				temp.append(ColumnName + " float");
			
			if (isNull.equals("no"))
				temp.append(" NOT NULL");
			else
			{
				//temp.append(" SPARSE");
			}
			if (i != noOfColumns-1)
				temp.append(",\n");
			
			
			i++;
			
			
			
		}
		  temp.append("\n)");
		  
		  out.write(temp.toString());
		  
		  //Close the input stream
		  in.close();
		  out.close();
		
	}catch(Exception e){
		e.printStackTrace();
	}
	
	
}
	

void SQLServerSparseCreateTable(String SchemaFileName, String SQLServerFileName)
{

	String pipeTemp=null;
	String attribute[];
	String ColumnName=null;
	String ColumnType=null;
	int ColumnLength = 0;
	String isNull=null;
	
	try{
		
		 FileInputStream fstream = new FileInputStream(SchemaFileName);
		  // Get the object of DataInputStream
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  
		  OuputWriter out = new OuputWriter(SQLServerFileName);
		  out.open();
		  
		  StringBuilder temp = new StringBuilder();
		  
		  temp.append("CREATE TABLE SPARSEDATA (\n");
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
				

					
			}
			if (ColumnType.equals("int"))
				temp.append(ColumnName + " INT");
			else if (ColumnType.equals("varchar"))
			{
				temp.append(ColumnName + " VARCHAR(");
				temp.append(ColumnLength + ")");
			}
			else if (ColumnType.equals("date"))
				temp.append(ColumnName + " DATE");
			else if (ColumnType.equals("float"))
				temp.append(ColumnName + " float");
			
			if (isNull.equals("no"))
				temp.append(" NOT NULL");
			else
			{
				temp.append(" SPARSE");
			}
			if (i != noOfColumns-1)
				temp.append(",\n");
			
			
			i++;
			
			
			
		}
		  temp.append("\n)");
		  
		  out.write(temp.toString());
		  
		  //Close the input stream
		  in.close();
		  out.close();
		
	}catch(Exception e){
		e.printStackTrace();
	}
	
	
}	
	
	
}
