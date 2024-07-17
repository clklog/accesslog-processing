package com.zcunsoft.accesslog.processing.utils;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

public final class UtilHelper {


    /**
     * Load file content.
     *
     * @param filePath the file path
     * @return the list
     */
    public static List<String> loadFileAllLine(String filePath) {
        InputStreamReader fr = null;

        File f = new File(filePath);

        List<String> lineList = null;
        try {
            fr = new InputStreamReader(new FileInputStream(f), "GB2312");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (fr != null) {
            BufferedReader br = new BufferedReader(fr);
            String line = "";

            try {
                lineList = new ArrayList<String>();
                while ((line = br.readLine()) != null) {
                    lineList.add(line);
                }

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                br.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                fr.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return lineList;

    }
}
