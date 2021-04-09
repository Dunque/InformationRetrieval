package es.udc.fic.ri.mipractica;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.store.FSDirectory;

public class StatsField {

    static String indexPath;
    static String field;

    private static void parseArguments(String[] args) {

        String usage = "java -jar StatsField-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-field FIELD_NAME]";

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
            }
        }
    }

    public static void main(String[] args) {

        parseArguments(args);
        
        try {
			IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
			CollectionStatistics collectionStats = new CollectionStatistics(
		            field.toString(),
		            reader.maxDoc(),
		            reader.getDocCount(field),
		            reader.getSumTotalTermFreq(field),
		            reader.getSumDocFreq(field)
		            );
			System.out.println(collectionStats.sumTotalTermFreq());
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
