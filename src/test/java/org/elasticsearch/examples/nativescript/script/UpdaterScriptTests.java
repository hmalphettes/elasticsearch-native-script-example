package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

/**
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =1)
public class UpdaterScriptTests extends AbstractSearchScriptTests {

//	@Override
//	public void setUp() throws Exception {
//		super.setUp();
//		
//	}
	private void prepareTest(XContentBuilder source) throws IOException, InterruptedException, ExecutionException {
		// Create a new index
		String mapping = XContentFactory.jsonBuilder()
		.startObject()
			.startObject("article")
				.startObject("properties")
					.startObject("name").field("type", "string").endObject()
					.startObject("tags").field("type", "string").endObject()
				.endObject()
			.endObject()
		.endObject().string();
		
		assertAcked(prepareCreate("test").addMapping("article", mapping));

		List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
		indexBuilders.add(client().prepareIndex("test", "article", "1").setSource(source));

		indexRandom(true, indexBuilders);
	}

	@Test
	public void testMergeItemsLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		Map<String, Object> setParams = new HashMap<String, Object>();
		List<String> vals = new ArrayList<String>();
		vals.add("elasticsearch");
		vals.add("ok lah");
		setParams.put("tags", vals);
		List<Object> tags = updateAndGetTags("mergeItems", setParams);
		assertTrue(tags.size() == 3);
	}

	@Test
	public void testRemoveItemsLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		Map<String, Object> setParams = new HashMap<String, Object>();
		List<String> vals = new ArrayList<String>();
		vals.add("elasticsearch");
		setParams.put("tags", vals);
		List<Object> tags = updateAndGetTags("removeItems", setParams);
		assertTrue(tags.size() == 2);
	}
	
	/** "script": "updater", "lang": "native",
	*   "params": {
	*	 "doc" : {
	*	   "name" : "First article",
	*	   "my" : {
	*		 "tags" : [ "elasticsearch", "search" ]
	*	   },
	*	   "address" : {
	*		 "city" : "
	*	   }
	*	 },
	*	 "remove": [ "address.city", "name" ],
	*	 "set"   : { "something.tags" : [ "sport", "entertainment", "game" ] }
	*	 "removeItems": { "my.tags" : [ "wow" ], "my.tags": ["foo"] }, 
	*	 "appendItems": { "my.tags" : [ "elasticsearch" ] },
	*	 "mergeItems"   : { "my.tags" : [ "bonsai" ] },
	*	 "orderedActions" : [
	*	   { "remove": [ "address.city", "name" ] },
	*	   { "mergeItems" : { "my.tags" : [ "bonsai" ] } }
	*	 ]
	*   } */

	private ArrayList<Object> updateAndGetTags(String action, Object values) {
		// Update using the script
		
		client().prepareUpdate("test", "article", "1")
				.setScript("updater").setScriptLang("native")
//				.addScriptParam("action", "updater")
//				.addScriptParam("source", "tags")
				.addScriptParam(action, values)
				.get();
		
		// Retrieve record and check
		GetResponse getResponse = client().prepareGet("test", "article", "1").get();
		
		assertTrue(getResponse.isExists());

		return (ArrayList<Object>) getResponse.getSource().get("tags");
	}

}
