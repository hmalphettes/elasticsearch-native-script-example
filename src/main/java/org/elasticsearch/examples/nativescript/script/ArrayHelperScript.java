package org.elasticsearch.examples.nativescript.script;

import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

/**
 * Script that updates an array.
 * Add/Remove one or more items.
 * Option to add the items only if they are not present inthe Array already.
 * 
 * MVEL version for a single item:
 * ctx._source.list.contains(p) ? (ctx.op = "none") : ctx._source.list += p
 */
public class ArrayHelperScript extends AbstractSearchScript {

	public static class Factory extends AbstractComponent implements NativeScriptFactory {

        /**
         * This constructor will be called by guice during initialization
         *
         * @param node injecting the reference to current node to get access to node's client
         */
        @SuppressWarnings("unchecked")
        @Inject
        public Factory(Node node, Settings settings) {
            super(settings);
        }

        /**
         * This method is called for every search on every shard.
         * 
         * @param params
         *            list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new ArrayHelperScript(params);
        }	
	}
	public static class FactorySet extends AbstractComponent implements NativeScriptFactory {
        @SuppressWarnings("unchecked")
        @Inject
        public FactorySet(Node node, Settings settings) {
            super(settings);
        }
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new ArrayHelperScript(params, ACTION_SET);
        }	
	}
	public static class FactoryAppend extends AbstractComponent implements NativeScriptFactory {
        @SuppressWarnings("unchecked")
        @Inject
        public FactoryAppend(Node node, Settings settings) {
            super(settings);
        }
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new ArrayHelperScript(params, ACTION_APPEND);
        }	
	}
	public static class FactoryRemove extends AbstractComponent implements NativeScriptFactory {
        @SuppressWarnings("unchecked")
        @Inject
        public FactoryRemove(Node node, Settings settings) {
            super(settings);
        }
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new ArrayHelperScript(params, ACTION_REMOVE);
        }	
	}
	
	protected static final int ACTION_SET = 1;
	protected static final int ACTION_APPEND = 2;
	protected static final int ACTION_REMOVE = 3;
	
	protected int action;
	protected String fieldName;
	protected String sourceName;
	protected Iterable<?> values;
	
	protected Map<String, Object> ctx;

	private static final int getAction(String act) {
		if (act == null || act.equals("set")) {
			return ACTION_SET;
		} else if (act.equals("append")) {
			return ACTION_APPEND;
		} else if (act.equals("remove")) {
			return ACTION_REMOVE;
		}
		return ACTION_SET;
	}
	
	public ArrayHelperScript(Map<String, Object> params) {
		this(params, getAction((String)params.get("action")));
	}

	public ArrayHelperScript(Map<String, Object> params, int act) {
		this(params, act, "value");
	}	
	
	public ArrayHelperScript(Map<String, Object> params, int act, String valueParameterName) {
		action = act;
		sourceName = (String) params.get("source");
		fieldName = (String) params.get("field");

		Object value = params.get(valueParameterName);
		if (value != null) {
			values = new ArrayList<Object>(1);
			((ArrayList<Object>)values).add(value);
		} else {
			Object vals = params.get(valueParameterName+"s");
			if (vals instanceof Iterable<?>) {
				values = (Iterable<?>) vals;
			} else {
				Object[] objs = (Object[]) vals;
				values = new ArrayList<Object>(objs.length);
				for (Object o : objs) {
					((ArrayList<Object>)values).add(o);
				}
			}
		}
	}

	@Override
	public Object run() {
		
		ArrayList<Object> items;
		if (fieldName != null) {
			items = (ArrayList<Object>) ctx.get(fieldName);
		} else {
			items = (ArrayList<Object>) ((Map<?,Object>)ctx.get("_source")).get(sourceName);
		}
		//System.err.println(action+ " " + items +" with "+values);
		boolean atLeastOneChange = false;
		if (action == ACTION_SET) {
			for (Object val : values) {
				if (!items.contains(val)) {
					atLeastOneChange = true;
				    //System.err.println("Adding " + val + " to " + items.getClass());
				    items.add(val);
				}
			}
		} else if (action == ACTION_APPEND) {
			for (Object val : values) {
				atLeastOneChange = true;
				items.add(val);
			}
		} else if (action == ACTION_REMOVE) {
			for (Object val : values) {
				atLeastOneChange = items.remove(val) || atLeastOneChange;
			}
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
