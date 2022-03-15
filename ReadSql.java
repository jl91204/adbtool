package adb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.nio.ByteBuffer;




import org.apache.commons.codec.binary.Hex;


public class ReadSql {

	private static String fileName = "c:/jdbc/SetM.sql";
	
	//static String fileName=ADBTool.fileName;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Path path = Paths.get(fileName);
		
		try {
		     if (isContainBOM(path)) {

		          byte[] bytes = Files.readAllBytes(path);

		          ByteBuffer bb = ByteBuffer.wrap(bytes);

		          System.out.println("Found BOM!");

		          byte[] bom = new byte[3];
		          // get the first 3 bytes
		          bb.get(bom, 0, bom.length);

		          // remaining
		          byte[] contentAfterFirst3Bytes = new byte[bytes.length - 3];
		          bb.get(contentAfterFirst3Bytes, 0, contentAfterFirst3Bytes.length);

		          System.out.println("Remove the first 3 bytes, and overwrite the file!");

		          // override the same path
		          Files.write(path, contentAfterFirst3Bytes);

		      } else {
		          System.out.println("This file doesn't contains UTF-8 BOM!");
		      }

			//readFromFile();
		} catch (Exception e) {
		}
	}
	public static void removeAllComents(String fileName) {
		String allLine="";
		String line="";
		//try (BufferedReader br = new BufferedReader(new FileReader(fileName) )){
		  try (FileInputStream fis = new FileInputStream(fileName);
			       InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
			       BufferedReader br = new BufferedReader(isr)
			  ) {
			while ((line = br.readLine()) != null) {
				allLine=allLine+line+"\n";
			}
			//Pattern commentPattern = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
			Pattern commentPattern = 
					Pattern.compile("(--[^\\r\\n]*)|(/\\*[\\w\\W]*?(?=\\*/)\\*/)");
			allLine = commentPattern.matcher(allLine).replaceAll("");
			//System.out.println(allLine);
			br.close();
	}catch (Exception ex) {
			System.out.println(ex);
		}
  
		//remove the contents
		try{PrintWriter writer = new PrintWriter(fileName);
		writer.print("");
		writer.close();
		}catch(Exception ex) {System.out.println(ex);}
		
		//write the new contents
		/*try{PrintWriter writer = new PrintWriter(fileName);
		writer.print(allLine);
		writer.close();
		}catch(Exception ex) {System.out.println(ex);}
		*/
		
		try (OutputStreamWriter writer =
	             new OutputStreamWriter(new FileOutputStream(fileName), StandardCharsets.UTF_8)){
	    // do stuff
			writer.append(allLine);
		}catch(Exception ex) {System.out.println(ex);}
	
	
	}
	
	public static ArrayList<String> readFromFile() throws Exception {
		
		//remove -- and /**/ multiple lines and single line comments from the script file
		removeAllComents(fileName);
		
		ArrayList<String> list = new ArrayList<>();
		String sqlStatement = "";
		int lineNo = 0;
		int statementNo=0;
		
		try (BufferedReader br = new BufferedReader(new FileReader(fileName) )){

			//br returns as stream and convert it into a List
		
			String line;
			while ((line = br.readLine()) != null) {
				//System.out.println(line);
				lineNo++;
				line = line.trim();
				if(line.isEmpty()) {
					//System.out.println("empty line...."+line);
				
					//line = "";
					continue;
				}
				if (line.startsWith("--")) {	//duplicate code... should remove this block code
					System.out.println("comment line with -- "+line);
					//line = "";
					continue;
				}
				
				
				if (!line.substring(line.length()-1).contains(";")) {
				 // if (!line.contains(";")) {
					sqlStatement = sqlStatement + line.trim();
					//System.out.println(sqlStatement);
				
				} else {
					line = line.substring(0, line.length() - 1);
					sqlStatement = sqlStatement + line;
					//System.out.println(sqlStatement);
					list.add(sqlStatement);
					statementNo++;
					sqlStatement = "";
					//line="";
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		/*for(String line:list)
		{
			System.out.println(line);
		}*/
		System.out.println("the no of Statement has been read " + lineNo);
		System.out.println("the no of Statement will be added into array " + statementNo);
		System.out.println("the size of the List of Statements " + list.size());
		
		return list;
	}
	
	 static boolean isContainBOM(Path path) throws IOException {

	      if(Files.notExists(path)){
	          throw new IllegalArgumentException("Path: " + path + " does not exists!");
	      }

	      boolean result = false;

	      byte[] bom = new byte[3];
	      try(InputStream is = new FileInputStream(path.toFile())){

	          // read first 3 bytes of a file.
	          is.read(bom);

	          // BOM encoded as ef bb bf
	          String content = new String(Hex.encodeHex(bom));
	          if ("efbbbf".equalsIgnoreCase(content)) {
	              result = true;
	          }

	      }

	      return result;
	  }
	
	   static void removeBom(Path path) throws IOException {

	      if (isContainBOM(path)) {

	          byte[] bytes = Files.readAllBytes(path);

	          ByteBuffer bb = ByteBuffer.wrap(bytes);

	          System.out.println("Found BOM!");

	          byte[] bom = new byte[3];
	          // get the first 3 bytes
	          bb.get(bom, 0, bom.length);

	          // remaining
	          byte[] contentAfterFirst3Bytes = new byte[bytes.length - 3];
	          bb.get(contentAfterFirst3Bytes, 0, contentAfterFirst3Bytes.length);

	          System.out.println("Remove the first 3 bytes, and overwrite the file!");

	          // override the same path
	          Files.write(path, contentAfterFirst3Bytes);

	      } else {
	          System.out.println("This file doesn't contains UTF-8 BOM!");
	      }

	  }

	
}
