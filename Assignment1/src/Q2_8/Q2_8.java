package Q2_8;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Q2_8 {

    public static void main(String[] args) {

        String txtFile = "/media/sf_SHARED/isd-history.txt";
        // load text file, change path for getting to the text file
        BufferedReader br = null;
        String line = "";
        //String cvsSplitBy = ";";

        try {

            br = new BufferedReader(new FileReader(txtFile));
            // read txt file
            int count = 0;
            // initialize counter for number of lines
            while ((line = br.readLine()) != null) {
            // read each line while it is not empty
            	count += 1;
            	// increment counter by 1 when 1 line is read
            	if (count > 22){
            		// we process the text only beyond line 22
            		if (line.length() > 0){
            			System.out.println("STATION: " + line.substring(13, 42) + "FIPS: " + line.substring(43, 45) + " ALTITUDE: " + line.substring(74, 81));
            			// with substring, isolate station name, FIPS and altitude
            		}
            	}
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}