# You are absolutely right. My previous snippet executed everything immediately and returned a list. To utilize lazy loading properly and populate gen_map with Iterators (generators), you need to wrap the logic in an inner function.

# Here is how you structure this so that gen_map contains unexecuted generators that only run when you iterate over them later.

# The Generator Factory Pattern

from typing import Dict, Iterator, Any
from collections import defaultdict

def get_results(sf):
    gen_map: Dict[str, Iterator[Any]] = {} 
    gen_scaffolds = get_gen_scaffolds(sf)
    junctions = get_junctions(sf)

    # --- The Inner Generator Function ---
    # This won't run until you loop over the result in gen_map
    def generate_matches(child_key, config):
        # 1. BUILD PHASE (Blocking): 
        # We must load the "Left" side (Children) into memory to index them.
        child_map = defaultdict(list)
        fk_field = config["fk"].lower()
        
        # Execute SOQL for children and bucket them by Foreign Key
        for child_rec in lazy_loading(sf, gen_scaffolds[child_key]['soql']):
            # Assuming flatten_record handles the nested field extraction
            fk_value = flatten_record(child_rec).get(fk_field)
            if fk_value:
                child_map[fk_value].append(child_rec)

        # 2. PROBE PHASE (Streaming):
        # Now stream the "Right" side (Parents). We yield as we find matches.
        parent_key = config["source"]
        for parent_rec in lazy_loading(sf, gen_scaffolds[parent_key]['soql']):
            p_id = parent_rec.get('id')
            
            # If this parent has waiting children in our map...
            if p_id in child_map:
                # ...yield each child, now enriched with parent data
                for child in child_map[p_id]:
                    # Merge logic: Nest parent or flatten. 
                    # Example: mutating child to include parent data
                    child['parent_data'] = parent_rec 
                    yield child

    # --- Main Execution ---
    # We simply assign the generator object to the map. 
    # No SOQL runs yet!
    for dev_name, contents in junctions.items():
        gen_map[dev_name] = generate_matches(dev_name, contents)

    return gen_map
# How to use the result
# Now get_results returns instantly. The heavy lifting only happens when you touch the specific key you need.

# Python

# results_map = get_results(sf)

# The SOQL queries for 'AccountContact' start running ONLY here:
my_iterator = results_map['AccountContact']

for match in my_iterator:
    print(match) # Streams one joined record at a time
# Why this structure?
# Memory Safety: You only hold the "Child" table in memory. The "Parent" table is streamed one by one.

# Laziness: If you have 10 junctions but only need to process one right now, the others never run.

# Variable Scope: Passing dev_name and contents as arguments to generate_matches ensures that the closures capture the correct config for each loop iteration.