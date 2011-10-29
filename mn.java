import java.io.*;
public class mn {


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int noOfColumns;
		int sparsity;
		int noOfNonSparseColumns;
		String fileName;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		SchemaGenerator sg = new SchemaGenerator(1000,80,15,"/home/arun/output.txt");
		sg.generateSchema();
		
		DataGenerator dg = new DataGenerator(10000,"/home/arun/output.txt",1000,15);
		
		dg.buildObject();
		
		dg.generateData();
		
		sg.AsterixCreateTable("/home/arun/output.txt", "/home/arun/asterix.create");
		
		System.out.println(Math.ceil((float)10/(float)9));
		
		//dg.PipeDelimitedGeneration("/home/arun.txt");
		//dg.JsonFormatGeneration("/home/arun.txt");
		
	}
}
