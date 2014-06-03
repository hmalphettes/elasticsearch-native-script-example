package org.elasticsearch.examples.nativescript.script;

import java.util.Map;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

/**
 * Script that updates a hash.
 * Remove one or more items by key.
 * 
 * MVEL version for a single item:
 * "script" : "ctx._source.remove(\"key\")"
 */
public class HashHelperScript extends ArrayHelperScript {

	public static class FactoryRemove extends AbstractComponent implements NativeScriptFactory {
        @SuppressWarnings("unchecked")
        @Inject
        public FactoryRemove(Node node, Settings settings) {
            super(settings);
        }
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new HashHelperScript(params, ACTION_REMOVE);
        }
	}

	public HashHelperScript(Map<String, Object> params, int act) {
		super(params, act, "key");
	}

	@Override
	public Object run() {

		Map<?,Object> items;
		if (fieldName != null) {
			if (fieldName.equals(".")) {
				items = ctx;
			} else {
				items = (Map<?,Object>) ctx.get(fieldName);
			}
		} else if (sourceName != null && !sourceName.equals(".")) {
			items = (Map<?,Object>) ((Map<?,Object>)ctx.get("_source")).get(sourceName);
		} else {
			items = (Map<?,Object>)ctx.get("_source");
		}
		boolean atLeastOneChange = false;
		for (Object val : values) {
			atLeastOneChange = items.remove(val) != null || atLeastOneChange;
		}
		if (!atLeastOneChange) {
			ctx.put("op", "none");
		}
		return atLeastOneChange;
	}

    @Override
    public void setNextVar(String name, Object value) {
        if(name.equals("ctx")) {
        	ctx = (Map<String, Object>) value;
        }
    }

}
