package es.udc.fic.ri.mipractica;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Paths;
import java.util.List;

public class WriteIndex {

    static String indexPath = "index";
    static String output = "WriteIndex.txt";

    private static void parseArguments(String[] args) {

        String usage = "java -jar WriteIndex-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-outputFile FILE]";

        if (args.length < 1)
            System.out.println(usage);

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-output".equals(args[i])) {
                output = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            }
        }
    }

    public static void main(String[] args) throws IOException {

        parseArguments(args);

        Directory dir = null;
        DirectoryReader indexReader = null;
        Document doc = null;

        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        List<IndexableField> fields = null;

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);
        } catch (Exception e) {
            System.out.println("Couldn't read path: " + indexPath);
            e.printStackTrace();
        }

        for (int i = 0; i < indexReader.numDocs(); i++) {

            try {
                doc = indexReader.document(i);
            } catch (CorruptIndexException e1) {
                System.out.println("Graceful message: exception " + e1);
                e1.printStackTrace();
            } catch (IOException e1) {
                System.out.println("Graceful message: exception " + e1);
                e1.printStackTrace();
            }

            System.out.println("Documento " + i);

            fields = doc.getFields();
            // Note doc.getFields() gets the stored fields
            bw.write("Fields found\n");
            for (IndexableField field : fields) {
                String fieldName = field.name();
                bw.write(fieldName + "\n");

            }

        }

        try {
            indexReader.close();
            dir.close();
            bw.close();
        } catch (IOException e) {
            System.out.println("Graceful message: exception " + e);
            e.printStackTrace();
        }
    }
}
