import java.io.BufferedWriter;
import java.io.FileWriter;


public class OuputWriter {
	String fileName;
	FileWriter fstream;
	BufferedWriter out;
	
	
	OuputWriter(){
		
	}
	
	OuputWriter(String input){
		fileName = input;
	}
	
	void open(){
		try{
			fstream = new FileWriter(fileName);
			out =  new BufferedWriter(fstream);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	void write(String input){
		try{
			out.write(input);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	void close(){
		try{
			out.close();
		}catch(Exception e){
			
		}
	}
}
