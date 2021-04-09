package es.udc.fic.ri.mipractica;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
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
            	if(args.length>i+1 && !args[i+1].startsWith("-")) {
            		field = args[i + 1];
                    System.out.println(args[i] + args[i + 1]);
                    i++;
            	}else {
            		System.out.println(usage);
            		System.exit(-1);
            	}
            }
        }
    }
    
    static void printStatistics(IndexReader reader, String field) throws IOException{
    	CollectionStatistics collectionStats = new CollectionStatistics(
	            field,
	            reader.maxDoc(),
	            reader.getDocCount(field),
	            reader.getSumTotalTermFreq(field),
	            reader.getSumDocFreq(field)
	            );
		System.out.println(collectionStats.toString());
    }

    public static void main(String[] args) {

        parseArguments(args);
        IndexReader reader=null;
        
        try {
			reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
			if(field==null) {
				for(IndexableField ifield : reader.document(0).getFields()) 
					printStatistics(reader,ifield.name());
			}else {
				printStatistics(reader,field);
			}
			reader.close();
			
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
