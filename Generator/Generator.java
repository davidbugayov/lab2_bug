package hw2;

import java.io.*;
import java.util.Random;

public class Generator {

    /**
     * Count of line in input data
     * */
    private int count = 100;

    /**
     * Filename input data
     * */
    private String filename = "metrics.log";


    /**
     * Function generate log file
     * */
    public void start()
    {
        String result = "";

        Random rand = new Random();

        int unixtime = getunixtime();

        while(count!=0)
        {
            result+=rand.nextInt(3)+","+(unixtime+count*100)+","+rand.nextInt(100)+"\n";
            count--;
        }
        //System.out.println(result);

        writelog(result);
    }



    /**
     * Function write in file
     * @param result - content file
     * */
    private void writelog(String result)
    {
        try
        {
            FileWriter writer = new FileWriter(this.filename, false);
            writer.write(result);
            writer.flush();
        }
        catch(IOException ex){

            System.out.println(ex.getMessage());
        }
    }

    /**
     * Function return current unixtime
     * @return unixtime
     * */
    private int getunixtime()
    {
        return (int) (System.currentTimeMillis() / 1000L);
    }
}
