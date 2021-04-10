package es.udc.fic.ri.mipractica;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class SimilarTerms {

    public static class CosineDocumentSimilarity {

        public static final String CONTENT = "Content";
        private final Set<String> terms = new HashSet<>();
        private final RealVector v1;
        private final RealVector v2;

        CosineDocumentSimilarity(String s1, String s2, String spath) throws IOException {
            Directory directory = createIndex(s1, s2, spath);
            IndexReader reader = DirectoryReader.open(directory);
            Map<String, Integer> f1 = getTermFrequencies(reader, 0);
            Map<String, Integer> f2 = getTermFrequencies(reader, 1);
            reader.close();
            v1 = toRealVector(f1);
            v2 = toRealVector(f2);
        }

        Directory createIndex(String s1, String s2, String spath) throws IOException {

            MMapDirectory directory = new MMapDirectory(Paths.get(spath));

            /*
             * File-based Directory implementation that uses mmap for reading, and
             * FSDirectory.FSIndexOutput for writing.
             *
             * RAMDirectory uses inefficient synchronization and is discouraged in lucene
             * 8.x in favor of MMapDirectory and it will be removed in future versions of
             * Lucene.
             */

            Analyzer analyzer = new SimpleAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            IndexWriter writer = new IndexWriter(directory, iwc);
            addDocument(writer, s1);
            addDocument(writer, s2);
            writer.close();
            return directory;
        }

        /* Indexed, tokenized, stored. */
        public final FieldType TYPE_STORED = new FieldType();

        final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

        {
            TYPE_STORED.setIndexOptions(options);
            TYPE_STORED.setTokenized(true);
            TYPE_STORED.setStored(true);
            TYPE_STORED.setStoreTermVectors(true);
            TYPE_STORED.setStoreTermVectorPositions(true);
            TYPE_STORED.freeze();
        }

        void addDocument(IndexWriter writer, String content) throws IOException {
            Document doc = new Document();
            Field field = new Field(CONTENT, content, TYPE_STORED);
            doc.add(field);
            writer.addDocument(doc);
        }

        double getCosineSimilarity() {
            return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
        }

        public double getCosineSimilarity(String s1, String s2, String spath) throws IOException {
            return new CosineDocumentSimilarity(s1, s2, spath).getCosineSimilarity();
        }

        Map<String, Integer> getTermFrequencies(IndexReader reader, int docId) throws IOException {
            Terms vector = reader.getTermVector(docId, CONTENT);
            // IndexReader.getTermVector(int docID, String field):
            // Retrieve term vector for this document and field, or null if term
            // vectors were not indexed.
            // The returned Fields instance acts like a single-document inverted
            // index (the docID will be 0).

            // Por esta razon al iterar sobre los terminos la totalTermFreq que es
            // la frecuencia
            // de un termino en la coleccion, en este caso es la frecuencia del
            // termino en docID,
            // es decir, el tf del termino en el documento docID

            TermsEnum termsEnum = null;
            termsEnum = vector.iterator();
            Map<String, Integer> frequencies = new HashMap<>();
            BytesRef text = null;
            while ((text = termsEnum.next()) != null) {
                String term = text.utf8ToString();
                int freq = (int) termsEnum.totalTermFreq();
                frequencies.put(term, freq);
                terms.add(term);
            }
            return frequencies;
        }

        RealVector toRealVector(Map<String, Integer> map) {
            RealVector vector = new ArrayRealVector(terms.size());
            int i = 0;
            for (String term : terms) {
                int value = map.containsKey(term) ? map.get(term) : 0;
                vector.setEntry(i++, value);
            }
            return (RealVector) vector.mapDivide(vector.getL1Norm());
        }
    }


    static String indexPath = "index";
    static String field;
    static String term;
    static int top;
    static String rep;

    private static void parseArguments(String[] args) {

        String usage = "java -jar SimilarTerms-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-field FIELD_NAME] [-term TERM_NAME]"
                + " [-top N] [-rep <bin | tf | tfxidf]";

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
            }
        }
    }

    static class TermValues {
        public String term;
        public double tf;
        public double idf;
        public double tfxidf;

        public TermValues(String term, double tf, double idf) {
            this.term = term;
            this.tf = tf;
            this.idf = idf;
            this.tfxidf = tf * idf;
        }
    }

    static class TFXIDFComparator implements Comparator<TermValues> {

        public int compare(TermValues a, TermValues b) {
            if (a.tfxidf > b.tfxidf)
                return -1;
            else if (b.tfxidf > a.tfxidf)
                return 1;
            else
                return 0;
        }
    }

    static class TFComparator implements Comparator<TermValues> {

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
        TFIDFSimilarity cosineSimilarity = new ClassicSimilarity();
        BytesRef tempTerm;
        int docsCount = reader.numDocs();
        PostingsEnum docs = null;
        ArrayList<TermValues> termValuesArray = new ArrayList<>();

        while ((tempTerm = iterator.next()) != null) {

            Term term = new Term(field, tempTerm);
            long indexDf = reader.docFreq(term);
            double idf = cosineSimilarity.idf(docsCount, indexDf);
            docs = iterator.postings(docs, PostingsEnum.NONE);

            while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {

                double tf = cosineSimilarity.tf(docs.freq());
                termValuesArray.add(new TermValues(term.text(), tf, idf));
            }
        }
        return termValuesArray;
    }

    public static void main(String[] args) throws IOException {

        parseArguments(args);

        DirectoryReader reader = null;

        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        PostingsEnum termPosting ;

        if ((termPosting = MultiTerms.getTermPostingsEnum(reader, field, new BytesRef(term))) != null) {

            int docId;

            while ((docId = termPosting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {

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

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
