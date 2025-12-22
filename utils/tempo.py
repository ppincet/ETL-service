from collections import defaultdict
from typing import Dict, Iterator, List, Any

def get_results(sf):
    # Depending on what you return, this might be List[Dict] or Iterator
    gen_map: Dict[str, List[Any]] = {} 
    
    gen_scaffolds = get_gen_scaffolds(sf)
    
    for dev_name, contents in get_junctions(sf).items():
        # 1. Prepare the Hash Map (Bucketing)
        # Use defaultdict(list) to handle multiple children pointing to one parent
        master = defaultdict(list) 
        id_field = contents["fk"].lower()
        
        # --- LEFT SIDE (Junctions) ---
        for rec in lazy_loading(sf, gen_scaffolds[dev_name]['soql']):
            # Key the map by the Foreign Key so we can look it up later
            fk_value = flatten_record(rec).get(id_field)
            if fk_value:
                master[fk_value].append(rec)
        
        # Container for matched results for this specific dev_name
        joined_results = []

        # --- RIGHT SIDE (Source) ---
        for parent_rec in lazy_loading(sf, gen_scaffolds[contents["source"]]['soql']):
            parent_id = parent_rec.get('id')
            
            # 2. The Join (Probe Phase)
            if parent_id in master:
                children = master[parent_id]
                
                # We found a match! Now merge the data.
                for child in children:
                    # OPTION A: Merge fields (Parent overwrites child collisions)
                    # combined = {**child, **parent_rec} 
                    
                    # OPTION B: Nest Parent inside Child (Cleanest)
                    child['parent_details'] = parent_rec
                    joined_results.append(child)

        # 3. Save to the map
        gen_map[dev_name] = joined_results

    return gen_map

# You are implementing an In-Memory Hash Join. You are loading the "Left" side (Junctions) into a dictionary (master) and then streaming the "Right" side (Source) to match them up.

# Here is the problem with your current code and the fix to populate gen_map.

# The Problems in the Current Logic
# Overwriting Data: In your first loop, master[fk] = rec will overwrite data if multiple records share the same foreign key (which is common). master needs to hold a list of records, not a single record.

# Type Error: In the second loop, master[id].append(rec) will crash because master[id] is currently a dictionary (the record itself), and dictionaries don't have an .append method.

# The Solution
# You need to bucket the records in the first loop (create a list of children for every parent ID), and then flatten/merge them in the second loop.

# Here is the corrected implementation:

# Python

from collections import defaultdict
from typing import Dict, Iterator, List, Any

def get_results(sf):
    # Depending on what you return, this might be List[Dict] or Iterator
    gen_map: Dict[str, List[Any]] = {} 
    
    gen_scaffolds = get_gen_scaffolds(sf)
    
    for dev_name, contents in get_junctions(sf).items():
        # 1. Prepare the Hash Map (Bucketing)
        # Use defaultdict(list) to handle multiple children pointing to one parent
        master = defaultdict(list) 
        id_field = contents["fk"].lower()
        
        # --- LEFT SIDE (Junctions) ---
        for rec in lazy_loading(sf, gen_scaffolds[dev_name]['soql']):
            # Key the map by the Foreign Key so we can look it up later
            fk_value = flatten_record(rec).get(id_field)
            if fk_value:
                master[fk_value].append(rec)
        
        # Container for matched results for this specific dev_name
        joined_results = []

        # --- RIGHT SIDE (Source) ---
        for parent_rec in lazy_loading(sf, gen_scaffolds[contents["source"]]['soql']):
            parent_id = parent_rec.get('id')
            
            # 2. The Join (Probe Phase)
            if parent_id in master:
                children = master[parent_id]
                
                # We found a match! Now merge the data.
                for child in children:
                    # OPTION A: Merge fields (Parent overwrites child collisions)
                    # combined = {**child, **parent_rec} 
                    
                    # OPTION B: Nest Parent inside Child (Cleanest)
                    child['parent_details'] = parent_rec
                    joined_results.append(child)

        # 3. Save to the map
        gen_map[dev_name] = joined_results

    return gen_map
# Key Changes Made:
# defaultdict(list): Used this for master. Now, if 5 records point to the same Foreign Key, all 5 are saved. Your previous code would have only saved the last one.

# Logic Flow:

# Loop 1: Build a lookup table where Key = FK and Value = [List of Child Records].

# Loop 2: Iterate the parents. If Parent.Id exists in the lookup table, grab the list of children and attach the parent data to them.

# Population: Created joined_results to collect the successes and assigned it to gen_map[dev_name].

# If you strictly need an Iterator (Generator)
# If your datasets are massive and you don't want gen_map holding everything in RAM at once, you need to turn the logic inside out or wrap the join logic in a separate generator function.

# However, since master (the first loop) must be fully loaded into memory to perform the join, the list approach above is usually sufficient unless the resulting joined_results is too massive.