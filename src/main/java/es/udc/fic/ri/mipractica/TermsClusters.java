package es.udc.fic.ri.mipractica;


public class TermsClusters {

    static String indexPath;
    static String field;
    static String term;
    static int top;
    static String rep;
    static int k;

    private static void parseArguments(String[] args) {

        String usage = "java -jar TermsClusters-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-field FIELD_NAME] [-term TERM_NAME]"
                + " [-top N] [-rep <bin | tf | tfxidf] [-k NUM_CLUSTERS]";

        if (args.length < 1)
            System.out.println(usage);

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-field".equals(args[i])) {
                field = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-term".equals(args[i])) {
                term = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-top".equals(args[i])) {
                top = Integer.parseInt(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-rep".equals(args[i])) {
                rep = args[i + 1];
                System.out.println(args[i]);
                i++;
            } else if ("-k".equals(args[i])) {
                k = Integer.parseInt(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            }
        }
    }

    public static void main(String[] args) {

        parseArguments(args);
    }
}
