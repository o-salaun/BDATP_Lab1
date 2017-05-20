package Q2_7;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Q2_7 {

    public static void main(String[] args) {

        String csvFile = "/media/sf_SHARED/arbres.csv";
        // modify the path to the file arbres.csv
        
        BufferedReader br = null;
        String line = "";
        String symbol_split = ";";

        try {

            br = new BufferedReader(new FileReader(csvFile));
            // load csv file
            while ((line = br.readLine()) != null) {
            	// load each line as long as it is not empty

                String[] tree = line.split(symbol_split);
                // fill an array of strings with each element separated by separator ";" in csv file

                System.out.println("[YEAR=" + tree[5] + " , HEIGHT=" + tree[6] + "]");
                // print year and height of each tree

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