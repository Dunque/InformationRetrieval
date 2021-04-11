package es.udc.fic.ri.mipractica;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

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
        
        IndexReader reader = null;

        try {
        	reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        PostingsEnum termPosting ;
        
        try {
        	List<String> terms = new ArrayList<String>();
        	Map<String,Integer> frequencies = new HashMap<>();
        	for(int i = 0; i<reader.maxDoc();i++) {
        		Terms vector = reader.getTermVector(i, field);
            	TermsEnum termsEnum = null;
        		termsEnum = vector.iterator();
        		BytesRef text = null;
        		while ((text = termsEnum.next()) != null) {
        			String term = text.utf8ToString();
        			int freq = (int) termsEnum.totalTermFreq();
        			frequencies.put(term, freq);
        			System.out.println(term);
        			terms.add(term);
        		}
        	}
        	if(!terms.contains(term)) {
    			System.out.println("B O I");
    		}else {
    			RealVector vector = new ArrayRealVector(terms.size());
    			int i = 0;
    			for(String termv : terms) {
    				int value = frequencies.containsKey(termv) ? frequencies.get(termv) : 0;
    				vector.setEntry(i++, value);
    				System.out.println("AA"+termv+" "+value+i);
    			}
    			RealVector rv = (RealVector) vector.mapDivide(vector.getL1Norm());
    			RealVector vector2 = new ArrayRealVector(terms.size());
    			int j = 0;
    			for(String termv : terms) {
    				int value = frequencies.get(term);
    				vector2.setEntry(j++, value);
    				System.out.println("BB"+term+" "+value+j);
    			}
    			//int value = frequencies.containsKey(term) ? frequencies.get(term) : 0;
    			//vector2.setEntry(0, value);
    			RealVector rv2 = (RealVector) vector2.mapDivide(vector2.getL1Norm());
    			
    			System.out.println(rv.dotProduct(rv2)/rv.getNorm()*rv2.getNorm());
    			System.out.println(rv.toString());
    			System.out.println(rv2.toString());
    			System.out.println(vector.toString());
    			System.out.println(vector2.toString());
    			
    			RealVector vector3 = new ArrayRealVector(1);
    			vector3.setEntry(0, frequencies.get(term));
    			for(String termv : terms) {
    				RealVector vector4 = new ArrayRealVector(1);
    				vector4.setEntry(0, frequencies.get(termv));
    				RealVector rv3 = (RealVector) vector3.mapDivide(vector3.getL1Norm());
    				RealVector rv4 = (RealVector) vector4.mapDivide(vector4.getL1Norm());
    				System.out.println(rv3.dotProduct(rv4)/rv3.getNorm()*rv4.getNorm());
    				System.out.println(termv+" "+frequencies.get(termv)+rv4.toString());
    			}
    			
    		}
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

        try {
			if ((termPosting = MultiTerms.getTermPostingsEnum(reader, field, new BytesRef(term))) != null) {
				int docId;

			    while ((docId = termPosting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
			    	
			    }
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
