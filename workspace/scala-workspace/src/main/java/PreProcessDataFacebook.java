import java.io.*;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

public class PreProcessDataFacebook {

    static String outLine = "";

    public static void main(String[] args) throws IOException {
        TreeMap<Integer, Set<String>> finalMap = new TreeMap<>();
        Set<String> hashSet;

        File file = new File("DataSet/facebook_combined.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        while ((st = br.readLine()) != null) {
            String[] stringArray = st.split(" ");
            hashSet = finalMap.get(Integer.valueOf(stringArray[0]));
            if (hashSet == null)
                hashSet = new TreeSet<>();
            for (int i = 1; i < stringArray.length; i++) {
                if (!"".equalsIgnoreCase(stringArray[i].trim()))
                    hashSet.add(stringArray[i].trim());
            }
            finalMap.put(Integer.valueOf(stringArray[0]), hashSet);
        }
        System.out.println(finalMap);

        FileWriter fileWriter = new FileWriter("DataSet/processed_fb_combined.txt");
        AtomicReference<String> outputLine = new AtomicReference<>("");

        finalMap.forEach((k, v) -> {
            outLine = k.toString();
            for (String value : v) {
                outLine = outLine + " " + value;
            }
            try {
                fileWriter.write(outLine);

            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                fileWriter.write("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        fileWriter.close();

    }
}
