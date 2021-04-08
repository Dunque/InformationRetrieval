package es.udc.fic.ri.mipractica;

import org.apache.lucene.index.IndexWriterConfig;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BestTerms {

    static String indexPath;
    static int docID;
    static String field;
    static int top;
    static String order;
    static String output = "BestTerms.txt";

    private static void parseArguments(String[] args) {

        String usage = "java -jar BestTerms-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-docID ID] [-field FIELD_NAME]"
                + " [-top N] [-order <tf | df | tfxidf] [-outputFile FILE]";

        if (args.length < 1)
            System.out.println(usage);

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-docID".equals(args[i])) {
                docID = Integer.parseInt(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-field".equals(args[i])) {
                field = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-top".equals(args[i])) {
                top = Integer.parseInt(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-order".equals(args[i])) {
                order = args[i + 1];
                System.out.println(args[i]);
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

//        Encontré esto en stackoverlfow no sé ni lo que hace
//        QueryParser parser = new QueryParser("Body", new EnglishAnalyzer());
//        Query query = parser.parse(topic);
//        TopDocs hits = iSearcher.search(query, 1000);
//        for (int i=0; i<hits.scoreDocs.length; i++){
//            Terms termVector = iSearcher.getIndexReader().getTermVector(hits.scoreDocs[i].doc, "Body");
//            Document doc = iSearcher.doc(hits.scoreDocs[i].doc);
//            documentsList.put(doc, termVector);
//        }
    }
}
