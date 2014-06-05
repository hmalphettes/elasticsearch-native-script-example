package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
@SuppressWarnings("unchecked")
@ClusterScope(scope= Scope.SUITE, numDataNodes =1)
public class UpdaterScriptTests extends AbstractSearchScriptTests {

	private Integer idx;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		idx = 0;
	}
	
	private String getIndex() {
		return "test" + idx;
	}
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

		idx++;
		assertAcked(prepareCreate(getIndex()).addMapping("article", mapping));

		List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
		indexBuilders.add(client().prepareIndex(getIndex(), "article", "1").setSource(source));

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
		
		List<Object> tags = ((List<Object>) updateAndGetResponse("mergeItems", setParams).get("tags"));
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
		List<Object> tags = (List<Object>) updateAndGetResponse("removeItems", setParams).get("tags");
		assertTrue(tags.size() == 1);
	}
	
	@Test
	public void testRemoveOneByArrayLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		List<String> vals = new ArrayList<String>();
		vals.add("tags");
		Map<String, Object> result = updateAndGetResponse("remove", vals);
		assertTrue(!result.containsKey("tags"));
		assertTrue(result.containsKey("name"));
	}
	
	@Test
	public void testRemoveOneByStringLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		Map<String, Object> result = updateAndGetResponse("remove", "tags");
		assertTrue(!result.containsKey("tags"));
		assertTrue(result.containsKey("name"));
	}
	
	@Test
	public void testRemoveTwoLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		List<String> vals = new ArrayList<String>();
		vals.add("tags");
		vals.add("name");
		Map<String, Object> result = updateAndGetResponse("remove", vals);
		assertTrue(!result.containsKey("tags"));
		assertTrue(!result.containsKey("name"));
	}

	@Test
	public void testAppendItemsLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		Map<String, Object> params = new HashMap<String, Object>();
		List<String> values = new ArrayList<String>();
		values.add("hello");
		values.add("wow");
		params.put("tags", values);
		List<Object> tags = (List<Object>) updateAndGetResponse("appendItems", params).get("tags");
		assertTrue(tags.size() == 4);
	}

	@Test
	public void testDocLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("name", "First article");

		Map<String, Object> my = new HashMap<String, Object>();
		List<String> values = new ArrayList<String>();
		values.add("hello");
		values.add("wow");
		my.put("tags", values);
		params.put("my", my);

		Map<String, Object> res = updateAndGetResponse("doc", params);
		//{name=First article, my={tags=[hello, wow]}, tags=[elasticsearch, wow]}
		assertEquals("First article", res.get("name"));
		assertEquals(2, ((List<?>)res.get("tags")).size());
		assertEquals(2, ((List<?>)((Map<?, ?>) res.get("my")).get("tags")).size());
	}
	
	@Test
	public void testSetLeafInRoot() throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder()
		.startObject()
			.field("name", "rec1")
			.field("tags", new String[] {"elasticsearch", "wow"})
		.endObject();
		prepareTest(source);

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("name", "First article");

		Map<String, Object> res = updateAndGetResponse("set", params);
		assertEquals("First article", res.get("name"));
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

	private Map<String, Object> updateAndGetResponse(String action, Object values) {
		// Update using the script
		client().prepareUpdate(getIndex(), "article", "1")
				.setScript("updater").setScriptLang("native")
				.addScriptParam(action, values)
				.get();
		
		// Retrieve record and check
		GetResponse getResponse = client().prepareGet(getIndex(), "article", "1").get();
		assertTrue(getResponse.isExists());
		return getResponse.getSource();
	}

}
