package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

/**
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =1)
public class HashHelperScriptTests extends AbstractSearchScriptTests {

    @Test
    public void testSet() throws Exception {
      
        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("article")
                .startObject("properties")
                .startObject("name").field("type", "string").endObject()
                .startObject("tags").field("type", "string").endObject()
                .startObject("address").field("type", "object")
                  .startObject("properties")
                    .startObject("number").field("type", "integer").endObject()
                    .startObject("street").field("type", "string").endObject()
                    .startObject("city").field("type", "string").endObject()
                  .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string();
        
        assertAcked(prepareCreate("test")
                .addMapping("article", mapping));
        
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client().prepareIndex("test", "article", "1")
        		.setSource(XContentFactory.jsonBuilder().startObject()
                    .field("name", "rec1")
                    .field("tags", new String[] {"elasticsearch", "wow"})
                      .startObject("address")
                        .field("number", 42).field("street", "lane 12").field("city", "LA")
                      .endObject()
                    .endObject()));

        indexRandom(true, indexBuilders);

        Map<String,Object> updatedDoc = remove(new String[] { "tags" }, null);
        assertTrue(updatedDoc.get("tags") == null);
        
        updatedDoc = remove(new String[] { "street" }, "address");
        assertTrue(((Map<?,Object>)updatedDoc.get("address")).get("street") == null);        

//        updatedDoc = remove(new String[] { "name", "address.city" }, null);
//        assertTrue(updatedDoc.get("name") == null);
//        assertTrue(((Map<?,Object>)updatedDoc.get("address")).get("city") == null);        
    }
    
    private Map<String,Object> remove(String[] keys, String source) {
        // Update using the script
        UpdateRequestBuilder update = client().prepareUpdate("test", "article", "1")
        		.setScript("hash.remove").setScriptLang("native")
        		.addScriptParam("keys", keys);
        if (source != null) {
        	update.addScriptParam("source", source);
        }
        update.get();
        
        // Retrieve record and check
        GetResponse getResponse = client().prepareGet("test", "article", "1").get();
        
        assertTrue(getResponse.isExists());

        return getResponse.getSource();
    }

}
