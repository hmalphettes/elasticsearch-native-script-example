package org.elasticsearch.examples.nativescript.script;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

/**
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =1)
public class ArrayHelperScriptTests extends AbstractSearchScriptTests {

    @Test
    public void testSet() throws Exception {
      
        // Create a new index
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("article")
                .startObject("properties")
                .startObject("name").field("type", "string").endObject()
                .startObject("tags").field("type", "string").endObject()
                .endObject().endObject().endObject()
                .string();
        
        assertAcked(prepareCreate("test")
                .addMapping("article", mapping));
        
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client().prepareIndex("test", "article", "1")
        		.setSource(XContentFactory.jsonBuilder().startObject()
                    .field("name", "rec1")
                    .field("tags", new String[] {"elasticsearch", "wow"})
//                    .field("tags", "wow")
                    .endObject()));

        indexRandom(true, indexBuilders);

        ArrayList<Object> tags = updateAndGetTags("set", new String[] { "elasticsearch", "ok lah" });
        assertTrue(tags.size() == 3);

        tags = updateAndGetTags("remove", new String[] { "elasticsearch", });
        assertTrue(tags.size() == 2);

    }
    
    private ArrayList<Object> updateAndGetTags(String action, Object[] values) {
        // Update using the script
        client().prepareUpdate("test", "article", "1")
        		.setScript("array").setScriptLang("native")
        		.addScriptParam("action", action)
        		.addScriptParam("source", "tags")
        		.addScriptParam("values", values)
        		.get();
        
        // Retrieve record and check
        GetResponse getResponse = client().prepareGet("test", "article", "1").get();
        
        assertTrue(getResponse.isExists());

        return (ArrayList<Object>) getResponse.getSource().get("tags");
    }

}
