package es.udc.fic.ri.mipractica;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing. Run
 * it with no command-line arguments for usage information.
 */
public class IndexFiles {

    static final String DEFAULT_PATH = "./config.properties";

    static String indexPath = "index"; //default index path is a folder named index located in the root dir
    static boolean create = true; //Create true == Update false
    static boolean onlyFiles = false;
    static List<String> fileTypes = new ArrayList<String>();
    static List<Path> docsPath = new ArrayList<Path>();
    static OpenMode openmode = OpenMode.CREATE_OR_APPEND;
    static boolean partialIndex = false;
    static List<Path> partialIndexes = new ArrayList<Path>();
    static int numThreads = Runtime.getRuntime().availableProcessors();

    static int topLines = 0;
    static int bottomLines = 0;
    //final ExecutorService executor = Executors.newFixedThreadPool(numCores);

    private IndexFiles() {
    }

    private static void parseArguments(String[] args) {

        String usage = "java -jar IndexFiles-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-update] [-onlyFiles]"
                + " [-openmode <APPEND | CREATE | APPEND_OR_CREATE>]"
                + " [-partialIndexes] [-numThreads NUM_THREADS]\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                + " in INDEX_PATH that can be searched with SearchFiles";

        if (args.length < 1)
            System.out.println(usage);

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-update".equals(args[i])) {
                create = false;
                System.out.println(args[i]);
            } else if ("-onlyFiles".equals(args[i])) {
                onlyFiles = true;
                System.out.println(args[i]);
            } else if ("-openmode".equals(args[i])) {
                openmode = IndexWriterConfig.OpenMode.valueOf(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-partialIndexes".equals(args[i])) {
                partialIndex = true;
                System.out.println(args[i]);
            } else if ("-numThreads".equals(args[i])) {
                numThreads = Integer.parseInt(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            }
        }
    }

    /**
     * Index all text files under a directory.
     */
    public static void main(String[] args) {

        parseArguments(args);
        readConfigFile(DEFAULT_PATH);

        for (Path path : docsPath) {
            if (!Files.isReadable(path)) {
                System.out.println("Document directory '" + path.toAbsolutePath()
                        + "' does not exist or is not readable, please check the path");
                System.exit(1);
            }
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer. But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);

            IndexWriter writer = new IndexWriter(dir, iwc);

            final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            List<Path> docsPathAux = new ArrayList<Path>(docsPath);
            ArrayList<Path>[] al = new ArrayList[numThreads];
            for (int i = 0; i < numThreads; i++) {
                al[i] = new ArrayList<Path>();
            }
    		
    		while(!docsPathAux.isEmpty()) {
    			for(int i = 0;i<numThreads;i++) {
    				if(docsPathAux.size()>0 && docsPathAux.get(0)!=null) {
    					al[i].add(docsPathAux.get(0));
    					docsPathAux.remove(0);
    				}
    			}
    		}
    		
    		
    		for(int i = 0; i<numThreads;i++) {
    			final Runnable worker = new WorkerThread(al[i],writer);
    			executor.execute(worker);
    		}
    		
    		//for (Path path : docsPath) {
    		//	final Runnable worker = new WorkerThread(path,writer);
    		//	executor.execute(worker);
    		//}


            //writer.close();

            executor.shutdown();
    		/* Wait up to 1 hour to finish all the previously submitted jobs */
    		try {
    			executor.awaitTermination(1, TimeUnit.HOURS);
    		} catch (final InterruptedException e) {
    			e.printStackTrace();
    			System.exit(-2);
    		}
    		
    		try {
				writer.commit();
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + " with message: " + e.getMessage());
        }
    }
    public static class WorkerThread extends Thread {

        private final List<Path> folders;
        private IndexWriter writer;

        public WorkerThread(final List<Path> folders, IndexWriter writer) {
            this.folders = folders;
            this.writer = writer;
        }

        /**
         * This is the work that the current thread will do when processed by the pool.
         * In this case, it will only print some information.
         */
        @Override
        public void run() {
            for (Path path : folders) {
                String ThreadName = Thread.currentThread().getName();

                System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
                        Thread.currentThread().getName(), path));

                //-----------------------------------------------------------------
                try {
                    System.out.println(ThreadName + ": Indexing to directory '" + path + "'...");

                    indexDocs(writer, path, ThreadName);


                } catch (IOException e) {
                    System.out.println(ThreadName + ": caught a " + e.getClass() + "\n with message: " + e.getMessage());
                }
                //-----------------------------------------------------------------
            }


        }

    }

    private static void readConfigFile(String path) {

        FileInputStream inputStream;
        Properties prop = new Properties();

        try {
            inputStream = new FileInputStream(path);
            prop.load(inputStream);
        } catch (IOException ex) {
            System.out.println("Error reading config file: " + ex);
            System.exit(-1);
        }

        //Read and store docs paths
        String docsList = prop.getProperty("docs");
        if (docsList != null) {
            String[] docsSplit = docsList.split(" ");
            for (int i = 0; i < docsSplit.length; i++) {
                Path doc = Paths.get(docsSplit[i]);
                docsPath.add(doc);
            }
        } else {
            System.out.println("Error in the config file, there are no docs paths");
            System.exit(-1);
        }

        //Read and store partial indexes paths
        String partIndexList = prop.getProperty("partialIndexes");
        if (partIndexList != null) {
            String[] partIndexSplit = partIndexList.split(" ");
            for (int i = 0; i < partIndexSplit.length; i++) {
                Path partIndex = Paths.get(partIndexSplit[i]);
                partialIndexes.add(partIndex);
            }
        } else {
            //System.out.println("Error in the config file, there are no partial index paths");
            //System.exit(-1);
        }

        //Reading the allowed file types
        String fileTypesList = prop.getProperty("onlyFiles");
        if (fileTypesList != null) {
            String[] fileTypesSplit = fileTypesList.split(" ");
            fileTypes.addAll(Arrays.asList(fileTypesSplit));
        } else
            System.out.println("Warning, no file types specified in config file");

        //Reading topLines property
        String onlyTopLines = prop.getProperty("onlyTopLines");
        if (onlyTopLines != null) {
            try {
                topLines = Integer.parseInt(onlyTopLines);
            } catch (Exception e) {
                System.out.println("Error reading onlyTopLines property " + e);
            }
        }

        //Reading bottomLines property
        String onlyBottomLines = prop.getProperty("onlyBottomLines");
        if (onlyBottomLines != null) {
            try {
                bottomLines = Integer.parseInt(onlyBottomLines);
            } catch (Exception e) {
                System.out.println("Error reading onlyBottomLines property " + e);
            }
        }
    }

    private static String getExtension(File file) {
        String fileName = file.getName();
        //Si el archivo tiene un . tomamos los caracteres de despues del punto
        if (fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0) {
            return fileName.substring(fileName.lastIndexOf("."));
        } else {
            return "";
        }
    }

    static void indexDocs(final IndexWriter writer, Path path, String threadName) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (onlyFiles) {
                        // We check what type of file is this
                        String fileType = getExtension(file.toFile());

                        // If the file extension is included add it
                        if (fileTypes.contains(fileType)) {
                            try {
                                indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                            } catch (IOException ignore) {
                                // don't index files that can't be read.
                            }
                        }
                    } else {
                        try {
                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                        } catch (IOException ignore) {
                            // don't index files that can't be read.
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    /**
     * Indexes a single document
     */
    static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            // make a new, empty document
            Document doc = new Document();

            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);
            doc.add(new LongPoint("modified", lastModified));
            doc.add(new TextField("contents",
                    new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
            doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
            doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
            doc.add(new DoublePoint("sizeKb", (double) (new File(file.toString()).length() / 1024)));

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                // New index, so we just add the document (no old document can be there):
                System.out.println(Thread.currentThread().getName() + " adding " + file);
                writer.addDocument(doc);
            } else {
                // Existing index (an old copy of this document may have been indexed) so
                // we use updateDocument instead to replace the old one matching the exact
                // path, if present:
                System.out.println(Thread.currentThread().getName() + " updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }

//    static void indexMulti(IndexWriter writer){
//        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
//
//        List<Path> docsPathAux = new ArrayList<Path>(docsPath);
//        ArrayList<Path>[] al = new ArrayList[numThreads];
//        for (int i = 0; i < numThreads; i++) {
//            al[i] = new ArrayList<Path>();
//        }
//
//        while (!docsPathAux.isEmpty()) {
//            for (int i = 0; i < numThreads; i++) {
//                if (docsPathAux.size() > 0 && docsPathAux.get(0) != null) {
//                    Path partialIndexPath = partialIndexes.get(j);
//                    FSDirectory partialIndexDir = FSDirectory.open(partialIndexPath);
//                    System.out.println("Sending " + p + " to be indexed in " + partialIndexPath);
//                    final Runnable worker = new WorkerThread(al[i], partialIndexDir);
//                    executor.execute(worker);
//                    j++;
//                    al[i].add(docsPathAux.get(0));
//                    docsPathAux.remove(0);
//                }
//            }
//        }
//
//        for (int i = 0; i < numThreads; i++) {
//            final Runnable worker = new WorkerThread(al[i], writer);
//            executor.execute(worker);
//        }
//
//        //Para todos los PATHS indicados en IndexFiles.config
//        try{
//            for (Path p:docsPathAux){
//                Path partialIndexPath = partialIndexes.get(j);
//                FSDirectory partialIndexDir = FSDirectory.open(partialIndexPath);
//                System.out.println("Sending " + p + " to be indexed in " + partialIndexPath);
//                final Runnable worker = new WorkerThread(p, partialIndexDir);
//                executor.execute(worker);
//                j++;
//            }
//        }
//        catch (Exception e){
//            error("Error during multithread Indexing "+e);
//        }
//        //Cerramos el pool de ejecutores
//        executor.shutdown();
//
//        //Le damos 1h para terminar la tarea
//        try {
//            executor.awaitTermination(1, TimeUnit.HOURS);
//        }
//        catch (final InterruptedException e) {
//            error("Timeout during index creation: "+e);
//            System.exit(-2);
//        }
//        finally {
//            //Fusionamos los indices temporales
//            debug("Merging indexes into "+indexPath);
//            try {
//                for (FSDirectory tmp : directory_list) {
//                    mainWriter.addIndexes(tmp);
//                }
//            } catch (IOException e) {
//                error("Error during indexes merge: "+e);
//            }
//        }
//    }
}
