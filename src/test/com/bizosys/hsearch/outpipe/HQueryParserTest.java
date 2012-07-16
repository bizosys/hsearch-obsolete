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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.TestAll;
import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.lang.Stemmer;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.util.GeoId;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class HQueryParserTest extends TestCase {
	
	public static void main(String[] args) throws Exception {
		HQueryParserTest t = new HQueryParserTest();
		
		//TestFerrari.testRandom(t);
		TestAll.run(new TestCase[]{t});

		/**
		t.setUp();
		t.testNoType("\"abinash Karan\"");
		t.tearDown();
		*/
	}
	
	boolean isMultiClient = false;
	AccountInfo acc = null;
	String ANONYMOUS = "anonymous";

	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);

		this.ANONYMOUS = Thread.currentThread().getName();
		this.acc = Account.getAccount(ANONYMOUS);
		if ( null == acc) {
			acc = new AccountInfo(ANONYMOUS);
			acc.name = ANONYMOUS;
			acc.maxbuckets = 1;
			Account.storeAccount(acc);
			Account.getCurrentBucket(acc);
		} else {
			Account.getCurrentBucket(acc);
		}
		
		List<String> stopwords = new ArrayList<String>();
		stopwords.add("but");
		StopwordManager.getInstance().setLocalStopwords(stopwords);		
	}	

	@Override
	protected void tearDown() throws Exception {
		ServiceFactory.getInstance().stop();
	}
	
	public void testNoType(String query) throws Exception  {
		Pattern pattern = Pattern.compile("[^a-zA-Z0-9 ]");
		Matcher queryMatcher = pattern.matcher(query);
		query = queryMatcher.replaceAll("").trim();
		query = query.replace(" ", "") ;

		QueryContext ctx = new QueryContext(acc, query);
		QueryPlanner qp = parseQuery(ctx);
		
		QueryTerm queryTerm = qp.mustTerms.get(0);
		assertEquals(query.toLowerCase(), queryTerm.wordOrig);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
	}

	public void testSpecificType(String type, String value) throws Exception  {
		
		Pattern pattern = Pattern.compile("[^a-zA-Z0-9 ]");
		Matcher typeMatcher = pattern.matcher(type);
		type = typeMatcher.replaceAll("").trim();
		type = type.replace(" ", "") ;
		
		Matcher valueMatcher = pattern.matcher(type);
		value = valueMatcher.replaceAll("").trim();
		value = value.replace(" ", "") ;
		
		String query = type + ":" + value;
		QueryContext ctx = new QueryContext(acc,query);
		QueryPlanner qp = parseQuery(ctx);
		QueryTerm queryTerm = qp.mustTerms.get(0);
		assertEquals(type.toLowerCase(), queryTerm.termType);
		assertEquals(value.toLowerCase(), queryTerm.wordOrig);
	}
	
	public void testAndSign() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"+abinash +karan");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("abinash", qp.mustTerms.get(0).wordStemmed);
		assertEquals("karan", qp.mustTerms.get(1).wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, qp.mustTerms.get(0).termType);
	}	
	
	public void testAndOperator() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"jakarta apache\" AND \"Apache Lucene\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache", qp.phrases.get(0).wordOrig);
		assertEquals("apache lucene", qp.phrases.get(1).wordOrig);
		assertEquals("jakarta", qp.mustTerms.get(0).wordOrig);
		assertEquals("apache", qp.mustTerms.get(1).wordOrig);
		assertEquals("lucene", qp.mustTerms.get(2).wordOrig);
	}	
	
	
	public void testPhrasedAnd() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"+\"Sunil Guttula\" +\"Abinasha Karana\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("sunil guttula", qp.phrases.get(0).wordOrig);
		assertEquals("abinasha karana", qp.phrases.get(1).wordOrig);
		assertEquals("sunil", qp.mustTerms.get(0).wordOrig);
		assertEquals("guttula", qp.mustTerms.get(1).wordOrig);
		assertEquals("abinasha", qp.mustTerms.get(2).wordOrig);
		assertEquals("karana", qp.mustTerms.get(3).wordOrig);
	}
	

	public void testOr() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"abinash karan");
		QueryPlanner qp = parseQuery(ctx);
		QueryTerm queryTerm = qp.optionalTerms.get(0);
		assertEquals("abinash", queryTerm.wordStemmed);
		assertEquals("karan", qp.optionalTerms.get(1).wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
	}	

	public void testAndOr() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"+jakarta lucene");
		QueryPlanner qp = parseQuery(ctx);
		QueryTerm queryTerm = qp.mustTerms.get(0);
		assertEquals("jakart", queryTerm.wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);

		queryTerm = qp.optionalTerms.get(0);
		assertEquals("lucene", qp.optionalTerms.get(0).wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, queryTerm.termType);
	}	

	public void testMultiphraseQuotes() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"abinash karan\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("abinash karan", qp.phrases.get(0).wordOrig);
		assertEquals("abinash", qp.optionalTerms.get(0).wordOrig);
		assertEquals("karan", qp.optionalTerms.get(1).wordOrig);
	}
	
	public void testBeginningQuotedMultiphrase() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"jakarta apache\" jakarta");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache", qp.phrases.get(0).wordOrig);
		assertEquals("jakarta", qp.optionalTerms.get(0).wordOrig);
		assertEquals("apache", qp.optionalTerms.get(1).wordOrig);
	}	
	
	
	/**
	public void testTilda() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"roam~0.8");
		QueryPlanner qp = parseQuery(ctx);
		System.out.println(qp.toString());
		assertEquals("roam~0.8", qp.mustTerms.get(0).wordStemmed);
	}

	public void testPhrasedTilda() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"jakarta apache\"~10");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache~10", qp.mustTerms.get(0).wordStemmed);
	}
	
	public void testCarot() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"jakarta^4 apache");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta^4", qp.optionalTerms.get(0).wordOrig);
		assertEquals("apache", qp.optionalTerms.get(1).wordOrig);
	}

	public void testPhrasedCarot() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"jakarta apache\"^4 \"Apache Lucene\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache^4", qp.optionalTerms.get(0).wordOrig);
		assertEquals("apache lucene", qp.optionalTerms.get(1).wordOrig);
	}
	*/
	
	public void testPhrasedExclusionSign() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"jakarta apache\" -\"Apache Lucene\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache", qp.phrases.get(0).wordOrig);
		assertEquals("apache lucene", qp.phrases.get(1).wordOrig);
		assertEquals(false, qp.phrases.get(1).isNegation);
		
		assertEquals("jakarta", qp.optionalTerms.get(0).wordOrig);
		assertEquals("apache", qp.optionalTerms.get(1).wordOrig);
		assertEquals(false, qp.optionalTerms.get(1).isNegation);
		assertEquals("lucene", qp.optionalTerms.get(2).wordOrig);
		assertEquals(false, qp.optionalTerms.get(2).isNegation);
	}

	public void testPhrasedNegationSign() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"jakarta apache\" !\"Apache Lucene\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache", qp.phrases.get(0).wordOrig);
		assertEquals(false, qp.phrases.get(0).isNegation);
		assertEquals("apache lucene", qp.phrases.get(1).wordOrig);
		assertEquals(true, qp.phrases.get(1).isNegation);
	}
	
	public void testPhrasedNegationOperator() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"\"jakarta apache\" NOT \"Apache Lucene\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache", qp.phrases.get(0).wordOrig);
		assertEquals(false, qp.phrases.get(0).isNegation);
		assertEquals("apache lucene", qp.phrases.get(1).wordOrig);
		assertEquals(true, qp.phrases.get(1).isNegation);
	}
	
	public void testNegationOperatorAtBeginning() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"NOT \"jakarta apache\"");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("jakarta apache", qp.phrases.get(0).wordOrig);
		assertEquals(true, qp.phrases.get(0).isNegation);
	}	

	/**
	 * Does not support ^ &*()<>:/{}[]` 
	 * @throws Exception
	 */
	public void testSpecial() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"abinash !@#$%,.?';+-");
		QueryPlanner qp = parseQuery(ctx);
		assertEquals("abinash", qp.phrases.get(0).wordStemmed);
		
		QueryTerm queryTerm = qp.phrases.get(1);
		assertEquals("@#$%,.?';+-", queryTerm.wordStemmed);
	}
	
	public void testStopword() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"abinash but");
		QueryPlanner qp = parseQuery(ctx);
		System.out.println(qp.toString());
		assertEquals(1, qp.mustTerms.size());
		assertEquals(null, qp.optionalTerms);
		
		assertEquals("abinash", qp.mustTerms.get(0).wordStemmed);
		assertEquals(Term.NO_TERM_TYPE, qp.mustTerms.get(0).termType);
	}		

	public void testMultiphraseWithType() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"+title:\"The Right Way\"");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 1, planner.phrases.size());
		assertEquals( "title", planner.phrases.get(0).termType);
		assertEquals( "the right wa", planner.phrases.get(0).wordStemmed);
		
		assertEquals( "the", planner.mustTerms.get(0).wordOrig);
		assertEquals( "right", planner.mustTerms.get(1).wordOrig);
		assertEquals( "way", planner.mustTerms.get(2).wordOrig);
		assertEquals( "title", planner.mustTerms.get(0).termType);
		assertEquals( "title", planner.mustTerms.get(1).termType);
		assertEquals( "title", planner.mustTerms.get(2).termType);
		
	}
	
	public void testType() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"title:Do it right");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 3, planner.optionalTerms.size());
		
		assertEquals( "do", planner.optionalTerms.get(0).wordOrig);
		assertEquals( "it", planner.optionalTerms.get(1).wordOrig);
		assertEquals( "right", planner.optionalTerms.get(2).wordOrig);

		assertEquals( "title", planner.optionalTerms.get(0).termType);
	}
	
	public void testQuotedType() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"title:\"Do it right\" AND right");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 2, planner.phrases.size());
		assertEquals( "title", planner.phrases.get(0).termType);
		assertEquals( "do it right", planner.phrases.get(0).wordStemmed);
		assertEquals( "right", planner.phrases.get(1).wordStemmed);

		assertEquals( "do", planner.mustTerms.get(0).wordStemmed);
		assertEquals( "it", planner.mustTerms.get(1).wordStemmed);
		assertEquals( "right", planner.mustTerms.get(2).wordStemmed);
		
	}

	public void testMultipleQuotedType() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"title:\"The Right Way\" AND text:go");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 2, planner.phrases.size());
		assertEquals( "title", planner.phrases.get(0).termType);
		assertEquals( "the right wa", planner.phrases.get(0).wordStemmed);
		assertEquals( "text", planner.phrases.get(1).termType);
		assertEquals( "go", planner.phrases.get(1).wordStemmed);
	}
	
	public void testTypeAndNonTypeMixed() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"id:560083 abinash");
		QueryPlanner planner = parseQuery(ctx);
		assertEquals( 2, planner.optionalTerms.size());
		assertEquals( null, planner.mustTerms);
		assertEquals( "id", planner.optionalTerms.get(0).termType);
		assertEquals( "560083", planner.optionalTerms.get(0).wordStemmed);
		assertEquals( "abinash", planner.optionalTerms.get(1).wordStemmed);
	}
	
	/**
	public void testBrackets() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"title:(+return +\"pink panther\")");
		parseQuery(ctx);
	}
	*/
	
	public void testDocumentTypeFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"typ:client abinash");
		parseQuery(ctx);
		assertEquals("client", ctx.docType);
	}

	public void testDocumentStateFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"ste:_active abinash");
		parseQuery(ctx); //It is in the stop word.. So just a work around.
		assertEquals("active", Storable.getString(ctx.state.toBytes()) );
	}
	
	public void testOrgunitFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"team:icici abinash");
		parseQuery(ctx); 
		assertEquals("icici", Storable.getString(ctx.team.toBytes()) );
	}

	public void testBornBeforeFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"createdb:1145353454334 abinash");
		parseQuery(ctx); 
		assertEquals(1145353454334L, ctx.createdBefore.longValue());
	}

	public void testBornAfterFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"createda:2165353454334 abinash");
		parseQuery(ctx); 
		assertEquals(2165353454334L, ctx.createdAfter.longValue());
	}

	public void testTouchAfterFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"modifieda:1145353454334 abinash");
		parseQuery(ctx); 
		assertEquals(1145353454334L, ctx.modifiedAfter.longValue());
	}

	public void testTouchBeforeFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"modifiedb:2165353454334 abinash");
		parseQuery(ctx); 
		assertEquals(2165353454334L, ctx.modifiedBefore.longValue());
	}
	
	public void testAreaInKmRadiusFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"aikr:12 abinash");
		parseQuery(ctx); 
		assertEquals(12, ctx.areaInKmRadius);
	}

	public void testMetaFieldFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"mf:_type abinash");
		parseQuery(ctx); 
		assertEquals("type", ctx.metaFields[0]);
	}

	public void testMatchIpFilter() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"matchip:192.168.12.23 abinash");
		parseQuery(ctx); 
		assertEquals("192.168.12.23", ctx.matchIp);
	}

	/**
	public void testLatitudeFilter() throws Exception  {
		GeoId geoId = GeoId.convertLatLng(12.78f, 98.78f);
		QueryContext ctx = new QueryContext(acc,"latlng:12.78,98.78 abinash");
		parseQuery(ctx);
		assertNotNull(ctx.getGeoId().getHouse(), geoId.getHouse());
	}
	*/

	public void testScoreBoosterOnDocWeight() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"docbst:40 abinash");
		parseQuery(ctx);
		assertEquals(40, ctx.boostDocumentWeight);
	}

	public void testScoreBoosterOnIPProximity() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"ipbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostIpProximity);
	}

	public void testScoreBoosterOnAuthorProximity() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"ownerbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostOwner);
	}

	public void testScoreBoosterOnFreshness() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"freshbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostFreshness);
	}

	public void testScoreBoosterOnPreciousness() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"preciousbst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostPrecious);
	}

	public void testScoreBoosterOnSocialRanking() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"choicebst:40 abinash");
		parseQuery(ctx); 
		assertEquals(40, ctx.boostChoices);
	}

	public void testBodyFetchLimit() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"dfl:25 abinash");
		parseQuery(ctx); 
		assertEquals(25, ctx.documentFetchLimit);
	}	
	public void testMetaFetchLimit() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"mfl:200 abinash");
		parseQuery(ctx); 
		assertEquals(200, ctx.metaFetchLimit);
	}

	public void testFacetFetchLimit() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"ffl:3000 abinash");
		parseQuery(ctx); 
		assertEquals(3000, ctx.facetFetchLimit);
	}
	
	public void testTeaserLength() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"tsl:240 abinash");
		parseQuery(ctx); 
		assertEquals(240, ctx.teaserSectionLen);
	}
	
	public void testSortOnMetaSingle() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"som:team=asc abinash");
		parseQuery(ctx); 
		assertEquals(1, ctx.sortOnMeta.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnMeta.get("team"));
	}		

	public void testSortOnMetaMultiple() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"som:team=asc som:type=desc abinash");
		parseQuery(ctx); 
		assertEquals(2, ctx.sortOnMeta.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnMeta.get("team"));
		assertEquals(QueryContext.SORT_DESC, ctx.sortOnMeta.get("type"));
	}		
	
	public void testSortOnXmlFieldSingle() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"sof:age=asc abinash");
		parseQuery(ctx); 
		assertEquals(1, ctx.sortOnFld.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnFld.get("age"));
	}		

	public void testSortOnXmlFieldMultiple() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"sof:age=asc sof:name=desc abinash");
		parseQuery(ctx); 
		assertEquals(2, ctx.sortOnFld.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnFld.get("age"));
		assertEquals(QueryContext.SORT_DESC, ctx.sortOnFld.get("name"));
	}
	
	public void testSortMixedMetaAndXml() throws Exception  {
		QueryContext ctx = new QueryContext(acc,"som:team=asc sof:age=asc abinash");
		parseQuery(ctx); 
		assertEquals(1, ctx.sortOnFld.size());
		assertEquals(1, ctx.sortOnMeta.size());
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnFld.get("age"));
		assertEquals(QueryContext.SORT_ASC, ctx.sortOnMeta.get("team"));
	}	

	/**
	 * TODO: Enable touchstone testing.
	 * @throws Exception
	 */
	public void testTouchStone() throws Exception  {
	}
	

	private QueryPlanner parseQuery(QueryContext ctx) throws Exception {
		QueryPlanner planner = new QueryPlanner();
		HQuery query = new HQuery(ctx, planner);
		
		new HQueryParser().visit(query, isMultiClient);
		//System.out.println( "Query Planner \n" + planner);
		//System.out.println( "Query Context \n" + ctx);
		return planner;
	}
	
	
}
