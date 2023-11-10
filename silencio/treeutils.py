from collections import defaultdict
from functools import partial
from random import choice
from typing import Mapping, Any

import pandas as pd
from cytoolz import valfilter, keyfilter


def make_drive_adjacency_list(directories: pd.DataFrame):
    adjacencies = {}
    for _, directory in directories.iterrows():
        siblings = directories.loc[directories["parents"] == directory["id"]]
        adjacencies[directory["id"]] = {
            "name": directory["name"],
            "children": set(siblings["id"].tolist()),
            "parent": directory["parents"],
            "id": directory["id"],
        }
    return adjacencies


# TODO, maybe: replace this stuff with graphlib
def find_root(adjacencies: Mapping, node_id: Any):
    while node_id in adjacencies.keys():
        node_id = adjacencies[node_id]["parent"]
    return node_id


def paths_from_root(adjacencies: Mapping, root_id: Any):
    top_nodes = valfilter(lambda x: x["parent"] == root_id, adjacencies)
    tree = {}
    unfinished = list(top_nodes.items())
    while len(unfinished) > 0:
        node_id, node = unfinished.pop()
        path = f"{node.get('path', '')}{node['name']}"
        if path in tree.values():
            print(f"warning: name collision on {path} under {root_id}")
        tree[node_id] = path
        children = {
            node_id: node | {"path": path + "/"}
            for node_id, node in keyfilter(
                lambda x: x in node["children"], adjacencies
            ).items()
        }
        unfinished += list(children.items())
    return tree


def segment_trees(adjacencies: Mapping):
    unrooted_nodes = list(adjacencies.keys())
    segments = {}
    cycles = 0
    while len(unrooted_nodes) > 0:
        root = find_root(adjacencies, choice(unrooted_nodes))
        tree = paths_from_root(adjacencies, root)
        unrooted_nodes = [
            node for node in unrooted_nodes if node not in tree.keys()
        ]
        segments[root] = tree
    return segments


def get_segment_ids(segments):
    return {
        root: set(segment.keys()) | {root}
        for root, segment in segments.items()
    }


def pick_disjoint_set(entry, value_set_mapping):
    for characteristic, value_set in value_set_mapping.items():
        if entry in value_set:
            return characteristic
    return None


def add_files_to_directory_tree(tree, files, root):
    for parent_id, contents in files.groupby("parents"):
        if parent_id == root:
            parent_path = ""
        elif parent_id not in tree.keys():
            continue
        else:
            parent_path = f"{tree[parent_id]}/"
        for _, row in contents.iterrows():
            path = f"{parent_path}{row['name']}"
            tree[row["id"]] = path
    return tree


def add_files_to_segmented_trees(segments, files):
    segmenter = partial(
        pick_disjoint_set, value_set_mapping=get_segment_ids(segments)
    )
    segment_membership = files["parents"].map(segmenter)
    for root, tree_files in files.groupby(segment_membership):
        add_files_to_directory_tree(segments[root], tree_files, root)
    return segments


def flip_path_tree(tree):
    paths = {}
    collisions = defaultdict(set)
    for drive_id in tree.keys():
        path = tree[drive_id]
        if path in paths.keys():
            collisions[path].update({drive_id, paths[path]})
        paths[path] = drive_id
    return (
        {path: paths[path] for path in sorted(paths.keys())},
        dict(collisions),
    )
