package es.udc.fi.ri.mipractica;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.store.FSDirectory;

public class StatsField {

    static String indexPath = "index";
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
    
    static String gatherStatistics(IndexReader reader, String field) throws IOException{
    	if(reader.getDocCount(field)>0) {
    		CollectionStatistics collectionStats = new CollectionStatistics(
    	            field,
    	            reader.maxDoc(),
    	            reader.getDocCount(field),
    	            reader.getSumTotalTermFreq(field),
    	            reader.getSumDocFreq(field)
    	            );
    		return collectionStats.toString();
    	}
    	return null;
    }
    
    static void printStatistics(IndexReader reader, Set<String> st) throws IOException{
    	for(String statistic : st) {
    		System.out.println(statistic);
    	}
    }

    public static void main(String[] args) {

        parseArguments(args);
        IndexReader reader=null;
        Set<String> st = new HashSet<String>();
        
        try {
			reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
			if(field==null) {
				for(int i = 0; i<reader.maxDoc();i++)
					for(IndexableField ifield : reader.document(i).getFields()) {
						String statistic=gatherStatistics(reader,ifield.name());
						if(statistic!=null)
							st.add(statistic);
						//printStatistics(reader,ifield.name());
					}
						
			}else {
				String statistic=gatherStatistics(reader,field);
				if(statistic!=null)
					st.add(statistic);
				//printStatistics(reader,field);
			}
			if(!st.isEmpty())
				printStatistics(reader,st);
			reader.close();
			
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
