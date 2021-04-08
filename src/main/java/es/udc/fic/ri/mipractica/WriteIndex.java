package es.udc.fic.ri.mipractica;

public class WriteIndex {

    static String indexPath;
    static String output = "WriteIndex.txt";

    private static void parseArguments(String[] args) {

        String usage = "java -jar WriteIndex-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-outputFile FILE]";

        if (args.length < 1)
            System.out.println(usage);

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-output".equals(args[i])) {
                output = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            }
        }
    }

    public static void main(String[] args) {

        parseArguments(args);
    }
}
