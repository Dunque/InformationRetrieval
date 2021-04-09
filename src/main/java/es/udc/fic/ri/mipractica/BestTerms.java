package es.udc.fic.ri.mipractica;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

    private static class TermValues {
        public String term;
        public double tf;
        public double df;
        public double idf;
        public double tfxidf;

        public TermValues(String term, double tf, double df, double idf) {
            this.term = term;
            this.tf = tf;
            this.df = df;
            this.idf = idf;
            this.tfxidf = tf * idf;
        }
    }

    private static class TFXIDFComparator implements Comparator<TermValues> {

        public int compare(TermValues a, TermValues b) {
            if (a.tfxidf > b.tfxidf)
                return -1;
            else if (b.tfxidf > a.tfxidf)
                return 1;
            else
                return 0;
        }
    }

    private static class TFComparator implements Comparator<TermValues> {

        public int compare(TermValues a, TermValues b) {
            if (a.tf > b.tf)
                return -1;
            else if (b.tf > a.tf)
                return 1;
            else
                return 0;
        }
    }

    public static ArrayList<TermValues> calculateTfidf(IndexReader reader, String field, int docID) throws IOException {

        Terms termVector;

        if ((termVector = reader.getTermVector(docID, field)) == null) {
            System.out.println("Document has no term vector");
            return null;
        }

        TermsEnum iterator = termVector.iterator();
        TFIDFSimilarity classicSimilarity = new ClassicSimilarity();
        BytesRef tmpTerm;
        int docsCount = reader.numDocs();
        PostingsEnum docs = null;
        ArrayList<TermValues> termValuesArray = new ArrayList<>();

        while ((tmpTerm = iterator.next()) != null) {

            Term term = new Term(field, tmpTerm);
            long indexDf = reader.docFreq(term);
            double idf = classicSimilarity.idf(docsCount, indexDf);
            docs = iterator.postings(docs, PostingsEnum.NONE);

            while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {

                double tf = classicSimilarity.tf(docs.freq());
                termValuesArray.add(new TermValues(term.text(), tf, idf));
            }
        }
        return termValuesArray;
    }

    private static IndexSearcher createSearcher(String path) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(path));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        return searcher;
    }

    private static TopDocs searchById(Integer id, IndexSearcher searcher) throws Exception {
        QueryParser qp = new QueryParser("id", new StandardAnalyzer());
        Query idQuery = qp.parse(id.toString());
        TopDocs hits = searcher.search(idQuery, 10);
        return hits;
    }

    public static void main(String[] args) throws Exception {

        parseArguments(args);

        IndexSearcher searcher = createSearcher(indexPath);
        TopDocs foundDocs = searchById(docID, searcher);
        Document doc;

        for (ScoreDoc sd : foundDocs.scoreDocs) {
            doc = searcher.doc(sd.doc);
        }

        DirectoryReader reader = null;

        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }


        ArrayList<TermValues> termValuesArray = calculateTfidf(reader, field, docId);

        if (rep == "tfxidf")
            Collections.sort(termValuesArray, new TFXIDFComparator());
        else if (rep == "tf")
            Collections.sort(termValuesArray, new TFComparator());
        else if (rep == "bin")
            Collections.sort(termValuesArray, new TFComparator());

        for (int i = 0; i < top; i++)
            System.out.println(termValuesArray.get(i).term);
    }
}
