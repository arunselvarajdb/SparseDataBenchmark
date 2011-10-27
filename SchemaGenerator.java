
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
		sb.append("group:0|");
		sb.append("sparse:false|");
		out.write(sb.toString());
		sb.setLength(0);
		int temp = 0;
		
		for(int i=1;i<noOfColumns;i++){
			
			//Column name 
			sb.append("name:C"+i+"|");
			
			//Datatype
			if((i-1)%4==0)
				sb.append("type:int|");
			else if((i-1)%4==1)
				sb.append("type:float|");
			else if((i-1)%4==2)
				sb.append("type:date|");
			else if((i-1)%4==3)
				sb.append("type:varchar|");
			
			//Group to determine mod function
			temp = (i%4)+1;
			sb.append("group:"+temp+"|");
			
			//Set Sparse property of column
			if(i<=noOfNonSparseColumns)
				sb.append("sparse:false|");
			else
				sb.append("sparse:true|");
				
			out.write(sb.toString());
			sb.setLength(0);
			
			
		}
		out.close();
	}
	
	
}
