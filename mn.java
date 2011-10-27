import java.io.*;
public class mn {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int noOfColumns;
		int sparsity;
		int noOfNonSparseColumns;
		String fileName;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		SchemaGenerator sg = new SchemaGenerator(13,100,2,"/Users/badhrijagan06/Desktop/output.txt");
		sg.generateSchema();
		
	}
}
