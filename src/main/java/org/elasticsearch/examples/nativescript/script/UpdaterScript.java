package org.elasticsearch.examples.nativescript.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

/**
 * Same functionality than the partial document update except that lists can be manipulated
 * and document properties can be deleted.
 * 
 * <ul>
 * <li>Partial update: merge with existing document: <code>{ "doc": {} }</code></li>
 * <li>Remove keys from the document: <code>"remove": { "address.city": [ "name" ] }</code></li>
 * <li>Remove items from lists: <code>{"removeItems": { "my.tags" : [ "wow" ] }</code></li>
 * <li>Append items to lists: <code>{"appendItems": { "my.tags" : [ "wow" ] }</code></li>
 * <li>Append items to lists if they were not in the list: <code>{"setItems": { "my.tags" : [ "wow" ] }</code></li>
 * </ul>
 * 
 * <p>
 * Note that the script first executes doc, then the removals, then the additions.
 * </p>
 * <p>
 * Example:
 * <code>
 * {
 *   "script": "updater", "lang": "native",
 *   "params": {
 *     "doc" : {
 *       "name" : "First article",
 *       "my" : {
 *         "tags" : [ "elasticsearch", "search" ]
 *       },
 *       "address" : {
 *         "city" : "
 *       }
 *     },
 *     "remove": [ "address.city", "name" ],
 *     "set"   : { "something.tags" : [ "sport", "entertainment", "game" ] }
 *     "removeItems": { "my.tags" : [ "wow" ], "my.tags": ["foo"] }, 
 *     "appendItems": { "my.tags" : [ "elasticsearch" ] },
 *     "mergeItems"   : { "my.tags" : [ "bonsai" ] },
 *     "orderedActions" : [
 *       { "remove": [ "address.city", "name" ] },
 *       { "mergeItems" : { "my.tags" : [ "bonsai" ] } }
 *     ]
 *   }
 * }
 * </code>
 * </p>
 */
@SuppressWarnings("unchecked")
public class UpdaterScript  extends AbstractSearchScript {

	public static final String SCRIPT_NAME = "updater";

	public static class Factory extends AbstractComponent implements NativeScriptFactory {

        /**
         * This constructor will be called by guice during initialization
         *
         * @param node injecting the reference to current node to get access to node's client
         */
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
            return new UpdaterScript(params);
        }	
	}
	
	protected Map<String,Object> source;
	protected Map<String, Object> ctx;
	private Map<String, Object> params;
	
	public UpdaterScript(@Nullable Map<String, Object> params) {
		this.params = params;
	}

	@Override
	public Object run() {
		if (!execActions(params)) {
			ctx.put("op", "none");
		}
		return null;
	}
	
    @Override
    public void setNextVar(String name, Object value) {
        if(name.equals("ctx")) {
        	ctx = (Map<String, Object>) value;
        	source = (Map<String, Object>) ctx.get("_source");
        }
    }
    
    protected boolean execActions(Map<String, Object> params) {
    	boolean atLeastOneChange = false;
    	for (Entry<String, Object> param : params.entrySet()) {
    		atLeastOneChange = execAction(param.getKey(), param.getValue()) || atLeastOneChange;
    	}
    	return atLeastOneChange;
    }
    
    protected boolean execAction(String action, Object value) {
    	boolean atLeastOneChange = false;
		if ("ordered".equals(action)) {
			List<Map<String,Object>> actions = (List<Map<String, Object>>)value;
			for (Map<String,Object> subAction : actions) {
				atLeastOneChange = execActions(subAction) || atLeastOneChange;
			}
		} else if (value instanceof Map && "doc".equals(action)) {
			atLeastOneChange = executeDoc( (Map<String,Object>) value) || atLeastOneChange;
		} else if (value instanceof Map) {
			Map<String,Object> pathValues = (Map<String,Object>) value;
			for (Entry<String,Object> entry : pathValues.entrySet()) {
				String path = entry.getKey();
				if ("mergeItems".equals(action)) {
					atLeastOneChange = execMergeItems(path, getEntryValueAsList(entry)) || atLeastOneChange;
				} else if ("appendItems".equals(action)) {
					atLeastOneChange = execAppendItems(path, getEntryValueAsList(entry)) || atLeastOneChange;
				} else if ("removeItems".equals(action)) {
					atLeastOneChange = execRemoveItems(path, getEntryValueAsList(entry)) || atLeastOneChange;
				} else if ("set".equals(action)) {
					atLeastOneChange = execSet(path, entry.getValue()) || atLeastOneChange;
				} else {
					System.err.println("Invalid operation.");
				}
			}
		} else if (value instanceof List) {
			for (Object v : (List<?>) value) {
				if ("remove".equals(action)) {
					atLeastOneChange = execRemove((String)v) || atLeastOneChange;
				}
			}
		} else if (value instanceof String && "remove".equals(action)) {
			atLeastOneChange = execRemove((String)value) || atLeastOneChange;
		}
		return atLeastOneChange;
    }

    private boolean executeDoc(Map<String, Object> doc) {
    	if (doc == null || doc.size() == 0) {
    		return false;
    	}
    	//XContentHelper.update(source, this.doc);
    	return mergeChanges(source, doc);
    }

    private boolean execRemoveItems(String path, List<Object> values) {
    	Map<String,Object> parent = (Map<String,Object>) selectParent(path, false);
    	if (parent == null) {
    		return false;
    	}
    	boolean atLeastOneChange = false;

    	String[] pPath = processPath(path);
    	Object leafNode = parent.get(pPath[1]);
    	if (leafNode == null || !(leafNode instanceof List)) {
    		return false;
    	}
    	List<Object> items = (List<Object>)leafNode;
		for (Object val : values) {
			// this wont work for Object values. We only support 'simple' values
			atLeastOneChange = items.remove(val) || atLeastOneChange;
		}
		if (items.size() == 0) {
			return execRemove(pPath[0]);
		}
		return atLeastOneChange;
    }

    private boolean execAppendItems(String path, List<Object> values) {
    	return execItems(path, values, false);
    }
    private boolean execMergeItems(String path, List<Object> values) {
    	return execItems(path, values, true);
    }

    private boolean execItems(String path, List<Object> values, boolean checkUnique) {
    	boolean atLeastOneChange = false;
    	Map<String,Object> parent = (Map<String,Object>) selectParent(path, true);
    	String leafName = getLeafName(path);
    	Object leafNode = parent.get(leafName);
    	if (leafNode == null || !(leafNode instanceof List)) {
    		leafNode = new ArrayList<Object>();
    		parent.put(leafName, leafNode);
    	}
    	List<Object> items = (List<Object>)leafNode;
		for (Object val : values) {
			// this wont work for Object values. We only support 'simple' values
			if (!checkUnique || !items.contains(val)) {
				atLeastOneChange = true;
			    items.add(val);
			}
		}
		return atLeastOneChange;
    }

    private boolean execRemove(String path) {
    	Map<String,Object> parent = (Map<String,Object>) selectParent(path, false);
    	if (parent == null) {
    		return false;
    	}
    	String[] pPath = processPath(path);
    	if (pPath[0] == null) {
    		return parent.remove(path) != null;
    	}
    	
    	// Remove the leaf of the path
    	if (parent.remove(pPath[1]) == null) {
    		return false;
    	}
    	if (parent.size() != 0) {
    		return true;
    	}
    	// The parent node now has nothing inside: we remove it recursively.
    	return execRemove(pPath[0]);
    }

    private boolean execSet(String path, Object val) {
    	Map<String, Object> parent = selectParent(path, true);
    	parent.put(getLeafName(path), val);
    	return true;
    }

    protected Map<String,Object> selectParent(String path, boolean lazyBuild) {
    	return selectParent(source, path.split("."), 0, lazyBuild);
    }

    /**
     * @param path
     * @return [ parentPath -or- null, leafPathName ]
     */
    private String[] processPath(String path) {
    	int last = path.lastIndexOf(".");
    	if (last == -1) {
    		return new String[] { null, path };
    	}
    	return new String[] { path.substring(0, last), path.substring(last+1) };
    }
    
    private String getLeafName(String path) {
    	int last = path.lastIndexOf(".");
    	if (last == -1) {
    		return path;
    	}
    	return path.substring(last+1);
    }
    
    private static List<Object> getEntryValueAsList(Entry<String,Object> entry) {
    	if (entry.getValue() instanceof List) {
    		return (List<Object>) entry.getValue();
    	} else {
    		List<Object> res = new ArrayList<Object>();
    		res.add(entry.getValue());
    		return res;
    	}
    }
    
    protected static Map<String,Object> selectParent(Map<String,Object> current,
    		String[] path, int segment, boolean lazyBuild) {
    	if (path.length == 0) {
    		return current;
    	}
    	String next = path[segment];
    	segment++;
    	Object property = current.get(next);
    	if (property == null || !(property instanceof Map)) {
    		if (!lazyBuild) {
    			return null;
    		}
	    	// Note that if the node was not null, and is not a map, it is in fact a value.
	    	// We are replacing it by a new property which is most likely going to fail
	    	// as it will conflict with the existing mappings for the node.
    		// We do it anyways as this is what the same behavior than
    		// the partial updates with the doc param
			property = new HashMap<String,Object>();
			current.put(next, property);
    	}
		if (segment == path.length - 1) {
			return (Map<String,Object>)property;
		}
		return selectParent((Map<String,Object>)property, path, segment, lazyBuild);
    }

    /**
     * Updates the provided changes into the source. If the key exists in the changes,
     * it overrides the one in source unless:
     * - both are Maps, in which case it recursively updated it.
     * - both are Arrays, in which case it adds the entries from the changes unless they existed already.
     * <p>
     * Code forked from @see org.elasticsearch.common.xcontent.XContentHelper#update
     * which is the method used by the update API for the <code>doc</code>
     * </br/>
     * The difference of behavior lies with the merge of the lists: combine items from the changes unless they
     * exists already.
     * </p>
     */
    protected static boolean mergeChanges(Map<String, Object> source, Map<String, Object> changes) {
        boolean atLeastOneChange = false;
    	for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            if (!source.containsKey(changesEntry.getKey())) {
                // safe to copy, change does not exist in source
                source.put(changesEntry.getKey(), changesEntry.getValue());
                atLeastOneChange = true;
            } else {
            	Object changesKey = changesEntry.getKey();
            	Object changesValue = changesEntry.getValue();
            	Object sourceValue = source.get(changesKey);
                if (sourceValue instanceof Map && changesValue instanceof Map) {
                    // recursive merge maps
                	atLeastOneChange = mergeChanges((Map<String, Object>) sourceValue,
                			(Map<String, Object>) changesValue) || atLeastOneChange;
                } else if (sourceValue instanceof List && changesValue instanceof List) {
                	// combine and check for uniqueness.
                	// note: this does not handle deep object structures in the list.
                	// only works for items that are 'simple' values.
                	List<Object> sourceList = (List<Object>)sourceValue;
                	for (Object o : (List<Object>)changesValue) {
                		if (!sourceList.contains(o)) {
                			sourceList.add(o);
                			atLeastOneChange = true;
                		}
                	}
                } else {
                    // update the field
                	Object oldValue = source.get(changesEntry.getKey());
                	if (oldValue != changesEntry.getValue()) {
                		atLeastOneChange = true;
                		source.put(changesEntry.getKey(), changesEntry.getValue());
                	}
                }
            }
        }
    	return atLeastOneChange;
    }

    
}
