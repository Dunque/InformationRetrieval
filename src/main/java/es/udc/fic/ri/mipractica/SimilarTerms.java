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

        private final Set<String> terms = new HashSet<>();

        CosineDocumentSimilarity() {
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

        double getCosineSimilarity(RealVector v1, RealVector v2) {
            return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
        }

        Map<String, Integer> getTermFrequencies(IndexReader reader, int docId, String field) throws IOException {
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

            Terms termVector;

            if ((termVector = reader.getTermVector(docId, field)) == null) {
                System.out.println("Document has no term vector");
                return null;
            }

            System.out.println(termVector);

            TermsEnum termsEnum = termVector.iterator();

            termsEnum = termVector.iterator();
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
            return vector.mapDivide(vector.getL1Norm());
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
            System.exit(-1);
        }

        List<Integer> docsList = new ArrayList<>();

        //Obtenemos la lista invertida del termino de referencia
        PostingsEnum referenceTermPosting = MultiTerms.getTermPostingsEnum(reader, field, new BytesRef(term));
        //Comprobamos que existe
        if (referenceTermPosting != null) {
            int docid;
            //Parseamos la lista entera de documentos en los que aparece el termino
            while ((docid = referenceTermPosting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
                //DocId contiene el id de un documento en el que aparece el termino solicitado
                docsList.add(docid);
            }
        }

        CosineDocumentSimilarity cds = new CosineDocumentSimilarity();

        for (int i = 0; i < docsList.size() - 1; i++) {

            Map<String, Integer> tm1 = cds.getTermFrequencies(reader, docsList.get(i), field);
            Map<String, Integer> tm2 = cds.getTermFrequencies(reader, docsList.get(i + 1), field);

            RealVector v1 = cds.toRealVector(tm1);
            RealVector v2 = cds.toRealVector(tm2);

            double sim = cds.getCosineSimilarity(v1, v2);

            System.out.println("Similarity between vectors " + docsList.get(i) + " and " + docsList.get(i + 1) + " : " + sim);

        }

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
