def separate(collection, predicate):
    hits = []
    misses = []
    for item in collection:
        if predicate(item) is True:
            hits.append(item)
        else:
            misses.append(item)
    return hits, misses
