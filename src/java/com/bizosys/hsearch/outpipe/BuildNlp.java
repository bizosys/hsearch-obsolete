/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.outpipe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.carrot2.clustering.lingo.LingoClusteringAlgorithm;
import org.carrot2.core.Cluster;
import org.carrot2.core.Controller;
import org.carrot2.core.ControllerFactory;
import org.carrot2.core.Document;
import org.carrot2.core.ProcessingResult;

import com.bizosys.hsearch.facet.FacetField;
import com.bizosys.hsearch.facet.IFacetField;
import com.bizosys.hsearch.index.IndexLog;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Cluster NLP information
 * @author karan
 *
 */
public class BuildNlp implements PipeOut{
	
	private static final boolean DEBUG_ENABLED = IndexLog.l.isDebugEnabled();
	private static final String NLP = "nlp";
	int maxDocuments = 50;
	
	public BuildNlp() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		
        HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryResult res = query.result;
		if ( null == res) return;
		if ( null == ctx.clusters) return;
		if (! ctx.clusters.contains(NLP) ) return;
		
		int teasersT = ( null == res.teasers) ? 0 : res.teasers.length;
		if ( teasersT < 1 ) return;
		
		Object[] teasers = res.teasers;

        ArrayList<Document> documents = new ArrayList<Document>(teasersT);
        
        for (int i=0; i<teasersT; i++) {
        	DocTeaserWeight teaser = (DocTeaserWeight) teasers[i];
    	    if (null == teaser.id ) continue;
    	    if ( teaser.id.length() == 0 ) continue;
    	    
    	    if (null == teaser.title ) continue;
    	    if ( teaser.title.length() == 0 ) continue;
    	    
    	    String summary = ( null == teaser.cacheText) ? teaser.preview : teaser.cacheText;
    	    if (null == summary ) continue;
    	    if ( summary.length() == 0 ) summary = "empty";
    	    
    	    documents.add(new Document(teaser.title, summary, teaser.id));
        }
        
        if ( DEBUG_ENABLED) {
        	IndexLog.l.debug("Total Carrot2 Documents Created :" + teasersT);
        }

        /* A controller to manage the processing pipeline. */
        final Controller controller = ControllerFactory.createSimple();
        

        if ( DEBUG_ENABLED) {
        	IndexLog.l.debug("Carrot2 Controller Created.");
        }

        /*
         * Perform clustering by topic using the Lingo algorithm. Lingo can 
         * take advantage of the original query, so we provide it along with the documents.
         */
        final ProcessingResult byTopicClusters = controller.process(documents, "data mining",
            LingoClusteringAlgorithm.class);
        
        
        if ( DEBUG_ENABLED) {
        	IndexLog.l.debug("Carrot2 Processing is over.");
        }

        final List<Cluster> clustersByTopic = byTopicClusters.getClusters();
        
        int clustersByTopicT = ( null == clustersByTopic) ? 
        	0 : clustersByTopic.size();
        if ( 0 == clustersByTopicT) return;
        
        StringBuilder sb = new StringBuilder(512);
		if ( null == res.facets) res.facets = 
			new HashMap<String, IFacetField[]>(ctx.clusters.size());
			
		IFacetField[] nlpFacets = new FacetField[clustersByTopicT];
		int counter = 0;
        for (Cluster cluster : clustersByTopic) {
        	cluster.setOtherTopics(false);
        	for (Document doc : cluster.getAllDocuments()) {
        		sb.append(doc.getId()).append('|');
			}
            FacetField ff = new FacetField(cluster.getLabel(), 
            	cluster.getAllDocuments().size(),sb.toString());

            nlpFacets[counter++] = ff;
            sb.delete(0, sb.capacity());
        }
        res.facets.put(NLP, nlpFacets);
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}
	
	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.maxDocuments = conf.getInt("cluster.cutoff.size", 50);
	}

	public PipeOut getInstance() {
		return this;
	}

	public String getName() {
		return "BuildNlp";
	}		
}
