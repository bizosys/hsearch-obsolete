package com.bizosys.hsearch.benchmark;

import java.io.File;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.HDocument;

public class LuceneIndexManager {
	private static LuceneIndexManager instance = null;
	public static LuceneIndexManager getInstance() throws Exception {
		if ( null != instance) return instance;
		synchronized (LuceneIndexManager.class) {
			if ( null != instance) return instance;
			instance = new LuceneIndexManager();
		}
		return instance;
	}
	
	Directory directory = null;
	IndexWriter iwriter = null;
	public LuceneIndexManager() throws Exception {
	    File indexDir = new File("/tmp");
	    this.directory = FSDirectory.open(indexDir);

	    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);

	    // To store an index on disk, use this instead:
	    this.iwriter = new IndexWriter(directory, analyzer, true,
	    		new IndexWriter.MaxFieldLength(25000));
	    this.iwriter.setMaxMergeDocs(10000);
	}
	
	public void insert(HDocument hdoc) throws Exception {
		
	    Document doc = new Document();
	    for (com.bizosys.hsearch.common.Field fld: hdoc.fields) {
	    	ByteField bf = fld.getByteField();
	    	Store store =  (fld.isStore()) ? Field.Store.YES: Field.Store.NO;
	    	Index index =  (fld.isAnalyze()) ? Field.Index.ANALYZED: Field.Index.NOT_ANALYZED;
	    	doc.add(new Field(bf.name, bf.getValue().toString(), store, index));
		}
	    
	    doc.add(new Field("id", hdoc.getTenantDocumentKey(), Field.Store.YES, Field.Index.ANALYZED));
	    if ( null != hdoc.docType) doc.add(new Field("type", hdoc.docType , Field.Store.YES, Field.Index.ANALYZED));
	    if ( null != hdoc.url) doc.add(new Field("url", hdoc.url , Field.Store.YES, Field.Index.ANALYZED));
	    if ( null != hdoc.title) doc.add(new Field("title", hdoc.title , Field.Store.YES, Field.Index.ANALYZED));
	    if ( null != hdoc.preview) doc.add(new Field("preview", hdoc.preview , Field.Store.YES, Field.Index.ANALYZED));
	    if ( null != hdoc.cacheText) doc.add(new Field("cache", hdoc.cacheText , Field.Store.YES, Field.Index.ANALYZED));
	    
	    iwriter.addDocument(doc);
	}
	
	public void insert(List<HDocument> hdocs) throws Exception {
		for (HDocument hdoc : hdocs) {
			insert(hdoc);
		}
	}
	
	public void search(String queryText) throws Exception {
		System.out.println("Searching:" + queryText);
		long start = System.currentTimeMillis();
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);

		// Now search the index:
	    IndexSearcher isearcher = new IndexSearcher(directory, true); // read-only=true
	    // Parse a simple query that searches for "text":
	    QueryParser parser = new QueryParser(Version.LUCENE_30,"cache", analyzer);
	    Query query = parser.parse(queryText);
	    ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
	    
		// Iterate through the results:
	    for (int i = 0; i < 100; i++) {
	      Document hitDoc = isearcher.doc(hits[i].doc);
	      hitDoc.get("id");
	      hitDoc.get("url");
	      hitDoc.get("preview");
	      //System.out.println(hitDoc.get("cache"));
	    }
		long end = System.currentTimeMillis();
		System.out.println(hits.length + " > Time Taken = " + (end - start));

	    isearcher.close();
	}

	public void close() throws Exception {
	    this.iwriter.close();
		directory.close(); 
	}
}
