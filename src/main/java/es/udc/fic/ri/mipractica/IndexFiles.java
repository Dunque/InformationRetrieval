package es.udc.fic.ri.mipractica;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
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
import org.apache.lucene.store.MMapDirectory;

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
    static ExecutorService executor = Executors.newFixedThreadPool(numThreads);

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

//        if (!partialIndex)
//            for (Path p : docsPath)
//                indexDocs(mainWriter, p);

        final Path docDir = null;

        List<MMapDirectory> dirList = new ArrayList<MMapDirectory>();

        Date start = new Date();

        MMapDirectory mmapdir = null;
        int i = 0;

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir)) {
            mmapdir = new MMapDirectory(Paths.get("/tmp/LuceneIndex"));
            dirList.add(mmapdir);

            final Runnable mainWorker = new WorkerThread(docDir, mmapdir);
            executor.execute(mainWorker);
            for (final Path path : directoryStream) {
                if (Files.isDirectory(path)) {
                    mmapdir = new MMapDirectory(Paths.get("/tmp/LuceneIndex" + i++));
                    dirList.add(mmapdir);

                    final Runnable worker = new WorkerThread(path, mmapdir);
                    /*
                     * Send the thread to the ThreadPool. It will be processed eventually.
                     */
                    executor.execute(worker);
                }
            }
        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        /*
         * Close the ThreadPool; no more jobs will be accepted, but all the previously
         * submitted jobs will be processed.
         */
        executor.shutdown();
        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        } finally {
            //Merge directories
            System.out.println("Merging indexes into " + indexPath);
            IndexWriterConfig iconfig = new IndexWriterConfig(new StandardAnalyzer());

            if (create)
                iconfig.setOpenMode(OpenMode.CREATE);
            else
                iconfig.setOpenMode(OpenMode.CREATE_OR_APPEND);

            IndexWriter ifusedwriter = null;

            try {
                Directory dir = FSDirectory.open(Paths.get(indexPath));
                ifusedwriter = new IndexWriter(dir, iconfig);
                for (MMapDirectory tmp : dirList) {
                    ifusedwriter.addIndexes(tmp);
                }
                ifusedwriter.commit();
                ifusedwriter.close();
            } catch (IOException e) {
                System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
            }

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        }
    }

	public static class WorkerThread implements Runnable {

		private final Path folder;
		private final MMapDirectory dir;

		public WorkerThread(final Path folder, final MMapDirectory dir) {
			this.folder = folder;
            this.dir = dir;
        }

		/**
		 * This is the work that the current thread will do when processed by the pool.
		 * In this case, it will only print some information.
		 */
		@Override
		public void run() {
			String ThreadName = Thread.currentThread().getName();

			System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
					Thread.currentThread().getName(), folder));
			
			try {
				System.out.println(ThreadName+": Indexing to directory '" + dir + "'...");

				//Directory dir = FSDirectory.open(Paths.get(indexPath+"/"+ThreadName));

				Analyzer analyzer = new StandardAnalyzer();
				IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

				if (create) {
					iwc.setOpenMode(OpenMode.CREATE);
				} else {
					// Add new documents to an existing index:
					iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				}

				IndexWriter writer = new IndexWriter(dir, iwc);
				//Do indexDoc to every file of folder (como indexDocs)
				indexDocs(writer,folder,ThreadName);

				writer.close();

			} catch (IOException e) {
				System.out.println(ThreadName+": caught a " + e.getClass() + "\n with message: " + e.getMessage());
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
            System.out.println("Error in the config file, there are no partial index paths");
            System.exit(-1);
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

    /**
     * Indexes the given file using the given writer, or if a directory is given,
     * recurses over files and directories found under the given directory.
     * <p>
     * NOTE: This method indexes one document per input file. This is slow. For good
     * throughput, put multiple documents into your input file(s). An example of
     * this is in the benchmark module, which can create "line doc" files, one
     * document per line, using the <a href=
     * "../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be
     *               stored
     * @param path   The file to index, or the directory to recurse into to find
     *               files to index
     * @throws IOException If there is a low-level I/O error
     */
    static void indexDocs(final IndexWriter writer, Path path, String threadName) throws IOException {

        Files.walkFileTree(path,new HashSet<>(),1, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!Files.isDirectory(file)) {
                    try {
                        indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), threadName);
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                        System.out.println("File " + file + " couldn't be indexed");
                    }
                }
				return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Indexes a single document
     */
    static void indexDoc(IndexWriter writer, Path file, long lastModified, String threadName) throws IOException {
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

            //faltan los de  lines y bottom lines

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                // New index, so we just add the document (no old document can be there):
                System.out.println(threadName + ": adding " + file);
                writer.addDocument(doc);
            } else {
                // Existing index (an old copy of this document may have been indexed) so
                // we use updateDocument instead to replace the old one matching the exact
                // path, if present:
                System.out.println(threadName + ": updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }
}


