from typing import Dict, Iterator, Any

def get_results(sf):
    gen_map: Dict[str, Iterator[Any]] = {} 
    gen_scaffolds = get_gen_scaffolds(sf)
    
    # Factory function to create a unique generator for each junction
    def generate_delta_matches(child_key, config):
        # 1. BUILD PHASE (In-Memory): 
        # Load the DELTA (Source/User) completely into a dictionary.
        # This acts as our "allowlist" filter.
        master_delta = {}
        source_key = config["source"] # The User/Parent
        
        for parent_rec in lazy_loading(sf, gen_scaffolds[source_key]['soql']):
            p_id = parent_rec.get('id')
            if p_id:
                master_delta[p_id] = parent_rec
        
        # 2. STREAM PHASE (Yielding):
        # Iterate through the Children (Accounts). 
        # We do NOT load all accounts into memory; we check them one by one.
        fk_field = config["fk"].lower()
        
        for child_rec in lazy_loading(sf, gen_scaffolds[child_key]['soql']):
            # Get the Foreign Key (e.g., OwnerId) from the Account
            fk_value = flatten_record(child_rec).get(fk_field)
            
            # 3. MATCH CHECK
            # If the Account's owner is in our Delta User map, yield it.
            if fk_value in master_delta:
                # Optional: Attach parent data if needed
                # child_rec['parent_details'] = master_delta[fk_value]
                yield child_rec

    # Assign the unexecuted generators to the map
    for dev_name, contents in get_junctions(sf).items():
        gen_map[dev_name] = generate_delta_matches(dev_name, contents)

    return gen_map