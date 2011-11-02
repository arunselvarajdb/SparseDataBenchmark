
import java.io.*;
public class mn {


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int noOfColumns=1000;
		int sparsity=80;
		//int noOfSparseColumns=400;
		int noOfRecords=1000;
		//for (int i=100;i<1000;i=i+100)
		//{
		int noOfSparseColumns=100;
		//String SchemafileName="C:\\Users\\arun\\Desktop\\data\\" + "1\\"+"output.txt";
		String SchemafileName="/home/arun/data/output.txt";
		String AsterixFileName="/home/arun/data/asterix.create";
		String DB2FileName="/home/arun/data/DB2.create";
		String SQLServerFileName="/home/arun/data/SQLServer.create";
		String SQLServerSparseFileName="/home/arun/data/SQLServerSparse.create";
		
		String pipeSQLDelimitFile="/home/arun/data/pipesql.dat";
		String pipeDB2DelimitFile="/home/arun/data/pipedb2.dat";
		String JsonDelimitFile="/home/arun/data/json.dat";
		
		
		
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		SchemaGenerator sg = new SchemaGenerator(noOfColumns,sparsity,noOfSparseColumns,SchemafileName);
		sg.generateSchema();
		
		DataGenerator dg = new DataGenerator(noOfRecords,SchemafileName,noOfColumns,noOfSparseColumns,pipeSQLDelimitFile, pipeDB2DelimitFile, JsonDelimitFile);
		
		dg.buildObject();
		
		dg.generateData();
		
		sg.AsterixCreateTable(SchemafileName, AsterixFileName);
		sg.Db2CreateTable(SchemafileName, DB2FileName);
		sg.SQLServerCreateTable(SchemafileName,SQLServerFileName);
		sg.SQLServerSparseCreateTable(SchemafileName,SQLServerSparseFileName);
		
		
		System.out.println((int)Math.ceil(3.3));
		}
	}
//}

