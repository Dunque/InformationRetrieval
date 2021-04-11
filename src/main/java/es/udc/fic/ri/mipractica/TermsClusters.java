package es.udc.fic.ri.mipractica;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    
    static class Vector{
    	private Map<Character,Integer> counter;
    	private Set<Character> charSet;
    	private double length;
    	private String word;
    	public Vector(Map<Character,Integer> counter, Set<Character> charSet, double length, String word){
    		this.counter=counter;
    		this.charSet=charSet;
    		this.length=length;
    		this.word=word;
    	}
    	
    	public Set<Character> getCharSet(){
    		return this.charSet;
    	}
    	
    	public Map<Character,Integer>  getCounter(){
    		return this.counter;
    	}
    	
    	public double  getLength(){
    		return this.length;
    	}
    	
    	double cosine_sim(Vector v2) {
    		Set<Character> commonchars= new HashSet<>(this.charSet);
    		commonchars.retainAll(v2.getCharSet());
    		
    		int product_sum=0;
    		for(Character character : commonchars) {
    			product_sum +=this.counter.get(character)*v2.getCounter().get(character);
    		}
    		
    		double length = this.length * v2.getLength();
    		
    		double similarity = 0;
    		if(length!=0) {
    			similarity=(product_sum/length);
    		}
    		return similarity;
    	}
    }
    
    private static Vector word2vec(String word) {
    	Map<Character,Integer> counter = new HashMap<>();
    	for(int i = 0;i<word.length();i++) {
    		if(counter.get(word.charAt(i))!=null) {
    			int count = counter.get(word.charAt(i))+1;
        		//counter.put(word.charAt(i), count);
        		counter.replace(word.charAt(i), count);
        			
    		}else {
    			counter.put(word.charAt(i), 1);
    		}
    	}
    	
    	Set<Character> charSet = counter.keySet();
    	
    	double length=0;
    	for(int i : counter.values()) {
    		length=length+(i*i);
    	}
    	length = Math.sqrt(length);
    	
    	return new Vector(counter,charSet,length,word);
    }

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
        Vector v1 = word2vec(term);
        
        IndexReader reader = null;

        try {
        	reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        PostingsEnum termPosting ;
        
        try {
        	List<String> terms = new ArrayList<String>();
        	Map<String,Double> frequencies = new HashMap<>();
        	for(int i = 0; i<reader.numDocs();i++) {
        		Terms vector = reader.getTermVector(i, field);
        		if(vector!=null) {
        			TermsEnum termsEnum = null;
            		termsEnum = vector.iterator();
            		BytesRef text = null;
            		while ((text = termsEnum.next()) != null) {
            			String term = text.utf8ToString();
            			Vector v2 = word2vec(term);
            			frequencies.put(term,v1.cosine_sim(v2));
            		}
        		}
        	}
        	
        	List<Map.Entry<String, Double> > list =
 	               new LinkedList<Map.Entry<String, Double> >(frequencies.entrySet());
 		Collections.sort(list, new Comparator<Map.Entry<String, Double> >() {
             public int compare(Map.Entry<String, Double> o1, 
                                Map.Entry<String, Double> o2)
             {
                 return (o2.getValue()).compareTo(o1.getValue());
             }
         });
           
         // put data from sorted list to hashmap 
         HashMap<String, Double> temp = new LinkedHashMap<String, Double>();
         for (Map.Entry<String, Double> aa : list) {
             temp.put(aa.getKey(), aa.getValue());
         }
         int n = temp.size();
         List<String> result = new ArrayList<>(n);
         for(String s : temp.keySet()) {
        	 result.add(s);
         }
         List<Double> result_sim = new ArrayList<>(n);
         for(Double d : temp.values()) {
        	 result_sim.add(d);
         }
         
         if(top>result.size()) {
        	 top=result.size();
         }else if(top<0) {
        	 top=0;
         }
         System.out.println(top+ " terms more similar with "+ term);
         for(int i = 0;i<top;i++) {
        	 System.out.println("Term: "+result.get(i)+" Similarity: "+String.format("%.4f",result_sim.get(i))+"%");
         }
        	
    			
    		
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
