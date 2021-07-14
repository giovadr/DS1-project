import java.io.*;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

public class Check {
    public static void main(String[] args) {
        String fileName = args[0];
        String line;
        int localSum, totalSum = 0;

        try {
            BufferedReader reader =
                    new BufferedReader(new FileReader(fileName));

            while((line = reader.readLine()) != null) {
                String[] l = line.split(" ");
                if(l.length < 4) continue;
                //System.out.println(Arrays.toString(l));
                if (l[2].equals("SUM")) {
                    localSum = Integer.parseInt(l[3]);

                    totalSum += localSum;
                }
            }

            System.out.println("Total sum: " + totalSum);

            reader.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
