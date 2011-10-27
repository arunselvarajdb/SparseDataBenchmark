import java.io.*;
import java.util.Scanner;
public class DataGenerator {
	int noOfRecords;
	String fileName;
	int noOfColumns;
	
	DataGenerator(int noOfRecords, String fileName, int noOfColumns){
		this.noOfRecords = noOfRecords;
		this.fileName = fileName;
	}
	
	void buildObject(){
		String temp;
		String [] attribute;
		ColumnObject[] colObj = new ColumnObject[noOfColumns];
		try{
			FileReader fin = new FileReader(fileName);
			Scanner src = new Scanner(fin);
			src.useDelimiter("| *");
			
			for(int i=1;i<=noOfColumns;i++){
				for(int j=0;j<4;j++){
					if(src.hasNext()){
						temp = src.next();
						attribute = temp.split(":");
						if(attribute[0].equals("name"))
							colObj[i-1].ColumnName = attribute[1];
						else if(attribute[0].equals("type"))
							colObj[i-1].ColumnType = attribute[1];
						else if(attribute[0].equals("group"))
							colObj[i-1].group = Integer.parseInt(attribute[1]);
						else if(attribute[0].equals("sparse"))
							colObj[i-1].sparse = attribute[1].equals("true")?true:false;						
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
