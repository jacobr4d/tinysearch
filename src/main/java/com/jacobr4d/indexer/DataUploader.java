package com.jacobr4d.indexer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jacobr4d.indexer.index.Index;
import com.jacobr4d.indexer.index.InverseDocumentFrequency;
import com.jacobr4d.indexer.index.InvertedHit;
import com.jacobr4d.indexer.index.PageRank;
import com.jacobr4d.indexer.index.TermFrequency;

public class DataUploader {
	private static final Logger logger = LogManager.getLogger(DataUploader.class);
	
	Index index;
	
	public DataUploader() {
		index = new Index("output/index");
	}
	
	public void close() {
		index.close();
	}
	
	public void uploadData() throws IOException {
		
		logger.info("populating tfs and inverted index");
		Stream<String> tfs = Files.lines(Paths.get("output/tfs"));
		tfs.forEach((tf) -> {
			String[] words = tf.split("\\s+");
			
			/* put tfs */
			TermFrequency o = new TermFrequency();
			o.wordUrl = words[0] + " " + words[1];
			o.frequency = words[2];
			index.putTermFrequency(o);
			
			/* put inverted hits */
			InvertedHit invertedHit = new InvertedHit();
			invertedHit.word = words[0];
			invertedHit.url = words[1];
			index.putInvertedHit(invertedHit);
		});
		tfs.close();
		
		logger.info("populating idfs");
		Stream<String> idfs = Files.lines(Paths.get("output/idfs"));
		idfs.forEach((idf) -> {
			String[] words = idf.split("\\s+");
			InverseDocumentFrequency o = new InverseDocumentFrequency();
			o.word = words[0];
			o.inverseDocumentFrequency = words[1];
			index.putInverseDocumentFrequency(o);
		});
		idfs.close();
		
		logger.info("populating prs");
		Stream<String> prs = Files.lines(Paths.get("output/prs"));
		prs.forEach((pr) -> {
			String[] words = pr.split("\\s+");
			PageRank o = new PageRank();
			o.url = words[0];
			o.pageRank = words[1];
			index.putPageRank(o);
		});
		prs.close();
		
		
		/* PRINT STATS */
		logger.info("HITS (by word<space>url) " + index.invertedHitIndex.count());
		logger.info("TFS (by word<space>url) " + index.termFrequencyIndex.count());
		logger.info("IDFS (by word) " + index.inverseDocumentFrequencyIndex.count());
		logger.info("PRS (by url) " + index.pageRankIndex.count());
	}
	
	public static void main(String[] args) throws IOException {
		DataUploader uploader = new DataUploader();
		uploader.uploadData();
		uploader.close();
	}

}
