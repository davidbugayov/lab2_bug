package hw2;

import java.io.File;


/**
 * MainClass
 * */
public class mainClass {

    public static void main(String[] args)
    {
        System.out.println(args.length);
        if(args.length!=1)
        {
            System.out.println("Run 'jarname.jar' <path_to_source_log>");
            System.exit(1);
        }

        File f = new File(args[0]);
        String pathlogsource = "/home/cloudera/Pictures/hw2david/src/main/java/hw2/metrics.log";

        if(!(f.exists() && !f.isDirectory())) {
            System.out.println("Source File Not Exists. Used test file for example");
        }
        else
        {
            pathlogsource = args[0];
        }

        System.out.println("Kafka producer started");
        kafka kf = new kafka(pathlogsource);
        kf.run();
        System.out.println("Kafka producer ended");

        System.out.println("Ignite started");
        ignite ig = new ignite();
        ig.consumer();
        System.out.println("Ignite compute started");
        ig.compute();
        System.out.println("Ignite ended");
    }
}
