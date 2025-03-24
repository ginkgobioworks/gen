import gen
import networkx as nx
import matplotlib.pyplot as plt
from mod import plot_graph, add_dummy_nodes

repo = gen.Repository()
bg = repo.get_block_groups_by_collection('default')[0]
G = repo.block_group_to_networkx(bg)

# The base layout defines the hierarchical layers
base_layout = repo.create_base_layout(bg)
#nx.set_node_attributes(G, base_layout.get_node_layers(), 'rank')

# The scaled layout is the outcome of the Sugiyama layout,
# for this example we clamp node length to 5, and set the scale to 1x.
scaled_layout = base_layout.create_scaled_layout(5, 1)
node_positions = scaled_layout.get_node_positions()
# node_positions has a tuple of (left, right) positions for each node (think about refactoring this)
for key, (left_pos, right_pos) in node_positions.items():
    try:
        G.nodes[key]['pos'] = left_pos # (x, y)
        G.nodes[key]['width'] = right_pos[0] - left_pos[0]
    except KeyError:
        print(f"KeyError: {key} -> ")
    except:
        raise

G = nx.convert_node_labels_to_integers(G, label_attribute='block_multi_id') 

plot_graph(G)

# Add dummy nodes to the graph, confirm by drawing the graph
H = add_dummy_nodes(G)
plot_graph(H)
for n in H.nodes():
    if H.nodes[n].get('is_dummy', False):
        print(n, H.nodes[n])


