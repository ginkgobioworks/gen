#!/usr/bin/env python

import networkx as nx
import sqlite3
import json
from copy import deepcopy
import subprocess

import pygraphviz
# Note: to install pygraphviz on macOS, you need to first install graphviz using homebrew,
# and then tell pip to look for the graphviz headers and libraries in the homebrew directory:
#  brew install graphviz
#  python3 -m pip install \
#                --config-settings="--global-option=build_ext" \
#                --config-settings="--global-option=-I$(brew --prefix graphviz)/include/" \
#                --config-settings="--global-option=-L$(brew --prefix graphviz)/lib/" \
#                pygraphviz

# TODO:
# - use pydot instead of pygraphviz to render the graph, as pydot is a pure python library and is easier to install

class Graph:
    def __init__(self, db=None, collection_name=None, sample_name=None, block_group_name=None):
        # There are two representations of the same graph: one with ported nodes (the additive model), 
        # and one with blocks (the conventional model with blocks and links).
        self.graph = nx.MultiDiGraph() # Allow multiple edges between the same nodes
        self.block_graph = None
        
        # Either import from a gen sqlite database or set up a minimal structure
        if db:
            self.import_from_db(db, collection_name, sample_name, block_group_name)
        else:
            # The main source and sink nodes of the DAG, named following the Rust code
            self.graph.add_node('start', label='start')
            self.graph.add_node('end', label='end')

    def _repr_png_(self):
        """
        Returns the PNG representation of the graph for IPython display.
        """
        return self.render_block_graph(minimize=True, format='png')

    def add_node(self, sequence, node_id=None):
        """
        Adds a node to the graph.

        Parameters:
        sequence (str): The sequence associated with a node. Note that in the actual gen code the sequence is hashed to save space.
        """
        if node_id is None:
            # Start node IDs at 1, not counting source and sink
            node_id = self.graph.number_of_nodes() - 1 

        node_id = f'{node_id}'

        self.graph.add_node(node_id, sequence=sequence)
        return node_id
    
    def connect_to_source(self, node_id, to_pos=0):
        """
        Connects a node to the main source node.

        Parameters:
        node_id (str): The ID of the node to connect to the source.
        to_pos (int): The position at which the edge arrives at node.
        """
        self.graph.add_edge('start', f'{node_id}', to_pos=to_pos)

        return self

    def connect_to_sink(self, node_id, from_pos=-1):
        """
        Connects a node to the sink node.

        Parameters:
        node_id (int or str): The ID of the node to connect to the sink.
        from_pos (int): The position from which the edge departs from the node.
        """
        # Make sure the node_id is used as a string
        node_id = f'{node_id}'

        if from_pos == -1:
            from_pos = len(self.graph.nodes[node_id]['sequence'])-1
            
        self.graph.add_edge(node_id, 'end', from_pos=from_pos)

        return self

    def add_edge(self, source, target, metadata={}):
        """
        Adds an edge to the graph with port information stored as edge attributes.

        Parameters:
        source (tuple): A tuple containing the source node and the position from which the edge departs.
        target (tuple): A tuple containing the target node and the position at which the edge arrives.
        """
        (source, from_pos), (target, to_pos) = source, target

        self.graph.add_edge(f'{source}', f'{target}', from_pos=from_pos, to_pos=to_pos, **metadata)

        return self

    def get_edges(self):
        """
        Returns a list of edges as tuples that can be hashed and compared
        """
        edges = set()
        for n1, n2, d in self.graph.edges(data=True):
            edges.add(((n1,d.get('from_pos',-1)),
                          (n2,d.get('to_pos',0))))
        return edges
    
    def get_nodes(self):
        """
        Returns a list of nodes as tuples that can be hashed and compared
        """
        nodes = set()
        for n, d in self.graph.nodes(data=True):
            nodes.add((n, d.get('sequence')))
        return nodes
    
    def remove_edge(self, source, target, prune=True):
        (source, from_pos), (target, to_pos) = source, target

        # Find the key corresponding to the given edge and coordinates
        for key, data in self.graph[source][target].items():
            if data.get('from_pos', -1) == from_pos and data.get('to_pos', 0) == to_pos:
                self.graph.remove_edge(source, target, key=key)
                break
        
        # If there are nodes that are completely disconnected, remove those too
        if prune:
            self.prune()

        return self
    
    def remove_node(self, node_id):
        self.graph.remove_node(f'{node_id}')
        return self

    def prune(self):
        '''Remove any nodes that are completely disconnected.'''
        for node in list(self.graph.nodes):
            if self.graph.out_degree()[node] == 0 and self.graph.in_degree()[node] == 0:
                self.graph.remove_node(node)

    def highlight_edge(self, source, target):
        # Unpack the source and target (node,position) tuples
        (source, from_pos), (target, to_pos) = source, target

        # Find the key corresponding to the given edge and coordinates
        for key, data in self.graph[source][target].items():
            if data.get('from_pos', -1) == from_pos and data.get('to_pos', 0) == to_pos:
                self.graph.edges[(source,target,key)]['highlight'] = True
                break

        return self
                
    def import_from_db(self, db, collection_name=None, sample_name=None, block_group_name=None):
        """
        Imports a graph from a gen database.

        Parameters:
        db (str): The gen sqlite3 .db file to import the graph from.
        collection_name (str): Filter by collection.
        sample_name (str): Filter by sample.
        graph_name (str): Filter by name.
        """
        con = sqlite3.connect(db)
        cur = con.cursor()

        # Build a query to get the edges
        query = '''SELECT source_node_id, source_coordinate, target_node_id, target_coordinate,
                          block_group_edges.chromosome_index as chrom_index,
                          block_group_edges.phased,
                          block_group_edges.source_phase_layer_id,
                          block_group_edges.target_phase_layer_id,
                          source_phase_layers.is_reference as source_is_reference,
                          target_phase_layers.is_reference as target_is_reference
                 FROM edges
                 JOIN block_group_edges
                 ON block_group_edges.edge_id = edges.id
                 JOIN block_groups
                 ON block_groups.id = block_group_edges.block_group_id
                 LEFT JOIN phase_layers source_phase_layers
                 ON source_phase_layers.id = block_group_edges.source_phase_layer_id
                 LEFT JOIN phase_layers target_phase_layers
                 ON target_phase_layers.id = block_group_edges.target_phase_layer_id'''

        filters = []
        if collection_name:
            filters.append(f'block_groups.collection_name = "{collection_name}"')
        if sample_name:
            filters.append(f'block_groups.sample_name = "{sample_name}"')
        if block_group_name:
            filters.append(f'block_groups.name = "{block_group_name}"')

        if filters:
            query += ' WHERE ' + ' AND '.join(filters)

        cur.execute(query)
        edges = cur.fetchall()
        
        # Get the nodes
        node_ids = set([e[0] for e in edges] + [e[2] for e in edges])
        query = f'''SELECT id, sequence
        FROM nodes
        JOIN sequences
        ON nodes.sequence_hash = sequences.hash 
        WHERE nodes.id IN ({','.join(str(i) for i in node_ids)});'''
        cur.execute(query)
        nodes = cur.fetchall()

        # Look for the source and sink nodes
        source_node, sink_node = None, None
        for id, sequence in nodes:
            if sequence.startswith('start-'):
                source_node = id
                continue
            if sequence.startswith('end-'):
                sink_node = id
                continue

            self.add_node(sequence, node_id=id)

        # In the Rust code, from_pos is considered to be exclusive to the range of the block, 
        # so we need to subtract 1 to get the conventional position in the sequence.
        for e in edges:
            source, from_pos, target, to_pos = e[0:4]
            metadata_keys = ['chrom_index', 'phased', 'src_phase_layer', 'tgt_phase_layer', 'src_is_ref', 'tgt_is_ref']
            metadata = {k: v for k, v in zip(metadata_keys, e[4:])}

            if source == source_node or from_pos == 0:
                self.connect_to_source(target, to_pos)
            elif target == sink_node:
                self.connect_to_sink(source, from_pos-1)
            else:
                self.add_edge((source, from_pos-1), (target, to_pos), metadata)
    
    def make_block_graph(self, prune=True):
        # Split up all nodes into blocks (i.e. contiguous subsequences)
        self.block_graph = nx.DiGraph()

        # Add source and sink nodes
        self.block_graph.add_node('start', label='start', original_node='start', end=-1)
        self.block_graph.add_node('end', label='end', original_node='end', start=0)

        # Iterate over the nodes in the graph and split them into blocks
        for node in self.graph.nodes:
            if node in ['start', 'end']:
                continue
            # Get ports from incoming and outgoing edges
            in_ports = [data.get('to_pos',0) for source, target, data in self.graph.in_edges(node, data=True)]
            out_ports = [data.get('from_pos',-1) for source, target, data in self.graph.out_edges(node, data=True)]

            sequence = self.graph.nodes[node].get('sequence', None)
            highlights = self.graph.nodes[node].get('highlights', [False] * len(sequence))

            # New blocks are defined by the port node identifier, and a start and end position on the sequence the port node references.
            # Each "in port" needs to be present as a segment start, and each "out port" as a segment end.
            # All in ports are preceded by a segment end, an all out ports are followed by a segment start.
            # Position 0 and the last position are always segment starts and ends, respectively.
            block_starts = sorted(set([0] + in_ports + [x+1 for x in out_ports if x < (len(sequence)-1)]))
            block_ends = sorted(set(out_ports + [len(sequence)-1] + [x-1 for x in in_ports if x > 0])) 

            # Create the blocks
            blocks = []
            for i, j in zip(block_starts, block_ends):
                segment_id = f"{node}.{i}"
                self.block_graph.add_node(segment_id, 
                                            sequence = sequence[i:j+1], 
                                            highlights = highlights[i:j+1],
                                            original_node = node, 
                                            start=i, end=j)
                blocks.append(segment_id)

            # Create new edges between the blocks to represent the reference sequence
            for i, j in zip(blocks[:-1], blocks[1:]):
                self.block_graph.add_edge(i, j, reference=True)

        # Translate the original edges to new edges in the segment graph
        for source, target, data in self.graph.edges(data=True):
            source_end = data.get('from_pos', -1)
            target_start = data.get('to_pos', 0)
            
            # Find the blocks that correspond to the source and target nodes, there should be only one of each
            source_blocks = [node for node, segment_data in self.block_graph.nodes(data=True) if segment_data['original_node'] == source and segment_data['end'] == source_end]
            target_blocks = [node for node, segment_data in self.block_graph.nodes(data=True) if segment_data['original_node'] == target and segment_data['start'] == target_start]
            assert len(source_blocks) == 1
            assert len(target_blocks) == 1

            # Other relevant data from the original edge
            metadata_keys = ['chrom_index', 'phased', 'src_phase_layer', 'tgt_phase_layer', 'src_is_ref', 'tgt_is_ref']
            metadata = {k: data.get(k, None) for k in metadata_keys}

            self.block_graph.add_edge(source_blocks[0], target_blocks[0], 
                                      original_edge=((source,data.get('from_pos',-1)),(target,data.get('to_pos',0))),
                                      reference=False, 
                                      highlight=data.get('highlight', False),
                                      **metadata
                                      )
            
        # Prune the block graph to remove nodes that are not connected to the main start and end node
        if prune:
           while True:
                trim_blocks = [k for k,v in dict(self.block_graph.in_degree()).items() if v == 0 and k!='start']
                trim_blocks += [k for k,v in dict(self.block_graph.out_degree()).items() if v == 0 and k!='end']
                if trim_blocks:
                    self.block_graph.remove_nodes_from(trim_blocks)
                else:
                    break


    def highlight_ranges(self, node_id, highlight_ranges):
        """
        Highlights specific ranges in a node's sequence.

        Parameters:
        node_id (int): The ID of the node to apply highlights to.
        highlight_ranges (list): A list of tuples containing the start and end positions of the ranges to highlight.
        """
        sequence = self.graph.nodes[node_id]['sequence']
        highlights = [False] * len(sequence)
        for start, end in highlight_ranges:
            for i in range(start, end):
                highlights[i] = True
        self.graph.nodes[node_id]['highlights'] = highlights

        return self

    def remove_highlights(self):
        """
        Removes highlights from nodes and edges.
        """
        for node in self.graph.nodes:
            self.graph.nodes[node].pop('highlights', None)
        for edge in self.graph.edges:
            self.graph.edges[edge].pop('highlight', None)

        return self

    def render_graph(self, filename=None, format='svg', minimize=False, splines=True, hide_nodes=[], 
                     graph_attributes={}, node_attributes={}, edge_attributes={}):
        # Create an AGraph to hold Graphviz attributes, based on the topology of the original graph
        agraph = pygraphviz.AGraph(directed=True, strict=False)
        # Add the source and sink nodes first and last, respectively
        agraph.add_node('start', label='start')
        agraph.add_nodes_from([n for n in self.graph.nodes if n not in ['start', 'end']])
        agraph.add_node('end', label='end')
        # Add the edges (the key allows us to have multiple edges between the same nodes)
        for source, target, key, data in self.graph.edges(data=True, keys=True):
            agraph.add_edge(source, target, key=key, **data)

        # Remove nodes that are marked as hidden
        for node in hide_nodes:
            agraph.delete_node(node)

        for node in agraph.iternodes():
            if node in ['start', 'end']:
                node.attr['margin'] = 0
                node.attr['fontsize'] = 12
                node.attr['width'] = 0.45
                node.attr['height'] = 0.3
                node.attr['label'] = 'Start' if node == 'start' else 'End'
                continue
            # Get the sequence and highlight information from the original graph
            node_data = self.graph.nodes[node]
            sequence = node_data['sequence']
            highlights = node_data.get('highlights', [False] * len(sequence))
            formatted_sequence = [f'<B>{c}</B>' if highlights[i] else c for i, c in enumerate(sequence)]

            if minimize and len(sequence) > 7:
                label = f"<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0' CELLPADDING='5'><TR>"
                label += f"<TD BORDER='0' PORT='caption'>{node}: </TD>"
                # Only show the first and last 3 elements, and ports that are connected to other nodes
                visible_ports = set(list(range(0,len(sequence))[:3]) + list(range(0,len(sequence))[-3:]))
                for u, v, edge_data in self.graph.in_edges(node, data=True):
                    visible_ports.add(edge_data['to_pos'])
                for u, v, edge_data in self.graph.out_edges(node, data=True):
                    visible_ports.add(edge_data['from_pos'])
                visible_ports = sorted(visible_ports)

                # Start of a list that contains each port of the node, with ellipses between ports that are not sequential
                labeled_sequence = [f"<TD PORT='{visible_ports[0]}'><FONT FACE='Monospace'>{formatted_sequence[visible_ports[0]]}<SUB>{visible_ports[0]}</SUB></FONT></TD>"]
                if len(visible_ports) > 1 and (visible_ports[1]-visible_ports[0]) > 1:
                    labeled_sequence.append("<TD>…</TD>")
                    
                # Loop over the connected ports in pairs of two, so that we can take into account the gaps between them.
                for a, b in zip(visible_ports, visible_ports[1:]):
                    if b - a > 1 and b != visible_ports[-1]:
                        labeled_sequence.append("<TD>..</TD>")
                        # A proper elipsis … or ... isn't rendered properly on github 

                    labeled_sequence.append(f"<TD PORT='{b}'><FONT FACE='Monospace'>{formatted_sequence[b]}<SUB>{b}</SUB></FONT></TD>")

                label += '\n'.join(labeled_sequence)
                label += "</TR></TABLE>>"
                
            else:
                label = f"<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0' CELLPADDING='2'><TR>"
                label += f"<TD BORDER='0' PORT='caption' ALIGN='right'>{node}:</TD>"
                labeled_sequence = [f"<TD PORT='{i}'><FONT FACE='Monospace'>{c}</FONT></TD>" 
                                      for i, c in enumerate(formatted_sequence)]
                label += '\n'.join(labeled_sequence)
                label += "</TR></TABLE>>"

            node.attr['shape'] = 'none'
            node.attr['margin'] = 0
            node.attr['label'] = label

            # Update the node attributes with any custom attributes
            node.attr.update(node_attributes.get(node, {}))

        for edge in agraph.iteredges():
            # The attributes should have been transferred over from the original graph

            # Set the constraint attribute to false for edges connected to the source or sink node
            if edge[0] == 'start' or edge[1] == 'end':
                edge.attr['constraint'] = False

            # Connect the head and tail of each edge to the correct port
            # Note: you can also force the head and tail to be on one side of the port 
            # by appending ":s" or ":n" to the port name
            edge.attr['headport'] = f"{edge.attr.get('to_pos','w')}" 
            # The above doesn't always work as expected, it seems like sometimes it gets set to None
            edge.attr['tailport'] = f"{edge.attr.get('from_pos','e')}"

            # Cleaner look for the first edge coming out of the source node, or for left-right layouts in general):
            if (edge[0] == 'start' or graph_attributes.get('rankdir', 'TD') == 'LR') and edge.attr['headport'] == '0':
                edge.attr['headport'] = 'caption'
            if (edge[1] == 'end'):
                edge.attr['headport'] = 'w'

         # Set the graph-level attributes that will be used by the dot rendering engine
        agraph.graph_attr.update(splines = 'true' if splines else 'polyline',
                                 fontnames = 'svg',
                                 rankdir = graph_attributes.get('rankdir', 'TD')
                                 )
        # Other useful arguments for dot (with defaults): ranksep (0.5) searchsize(100) mclimit(10) newrank(false)

        # Custom attributes will override what we just set above
        for node in agraph.iternodes():
            node.attr.update(node_attributes.get(node, {}))
        for edge in agraph.iteredges():
            source = edge[0]
            source_coordinate = edge.attr.get('from_pos', -1)
            target = edge[1]
            target_coordinate = edge.attr.get('to_pos', 0)
            port_edge = (f"{source}:{source_coordinate}", f"{target}:{target_coordinate}")
            edge.attr.update(edge_attributes.get(port_edge, {})) 

        agraph.graph_attr.update(graph_attributes)

        return self.render_dot(agraph, filename, format)


    def render_block_graph(self, filename=None, format='svg', minimize=False, splines=True, align_blocks=True, 
                            ranksep=0.5, hide_nodes=[], prune=True, edge_label=None,
                            node_attributes={}, edge_attributes={}, graph_attributes={}):
        # Ensure that the file format is one of the supported formats
        if format not in ['svg', 'png', 'dot']:
            raise ValueError(f'Unsupported file format: {format}')
        
        # Ensure edge_label is one of the allowed values
        if edge_label not in [None, 'index', 'phase_layer']:
            raise ValueError(f'Unsupported edge_label: {edge_label}')

        # Todo: refactor to break out a node -> segment function instead of make_block_graph
        self.make_block_graph(prune=prune)

        # Create an AGraph to hold Graphviz attributes, based on the topology of the original graph
        agraph = pygraphviz.AGraph(directed=True)
        # Add the source and sink nodes first and last, respectively
        agraph.add_node('start', label='start')
        agraph.add_nodes_from([n for n in self.block_graph.nodes if n not in ['start', 'end']])
        agraph.add_node('end', label='end')
        agraph.add_edges_from(self.block_graph.edges)

        # Remove nodes that are marked as hidden
        for node in hide_nodes:
            agraph.delete_node(node)

        for node in agraph.iternodes():
            if node in ['start', 'end']:
                node.attr['margin'] = 0
                node.attr['fontsize'] = 12
                node.attr['width'] = 0.45
                node.attr['height'] = 0.3
                node.attr['label'] = 'Start' if node == 'start' else 'End'
                continue

            # Get the sequence and highlight information from the networkx graph
            node_data = self.block_graph.nodes[node]
            # Update the node attributes with any custom attributes
            node_data.update(node_attributes.get(node, {}))

            sequence = node_data['sequence']
            highlights = node_data.get('highlights', [False] * len(sequence))
            formatted_sequence = [f'<B>{c}</B>' if highlights[i] else c for i, c in enumerate(sequence)]
            bgcolor = node_data.get('bgcolor', None)
            origin = node_data['original_node']
            start = node_data.get('start', 0)
            end = node_data.get('end', len(sequence)-1)

            label = f"<<TABLE BORDER='0'>"
            if bgcolor:
                label += f"<TR><TD BORDER='1' ALIGN='CENTER' PORT='seq' BGCOLOR='{bgcolor}'>"
            else:
                label += f"<TR><TD BORDER='1' ALIGN='CENTER' PORT='seq'>"
            label += f"<FONT POINT-SIZE='12' FACE='Monospace'>"
            if minimize and len(sequence) > 7:
                # Only show the first and last 3 elements
                label += f"{''.join(formatted_sequence[0:3])}..{''.join(formatted_sequence[-3:])}"
                # The Unicode elipsis … isn't rendered properly on github, and it tries to replace ... automatically,
                # so we use two dots instead
            else:
                label += ''.join(formatted_sequence)

            label += "</FONT></TD></TR>"  
            label += "<TR><TD ALIGN='CENTER'><FONT POINT-SIZE='10'>"
            if end-start > 0:
                label += f"{origin}:{start}-{end}"
            elif start > 0:
                label += f"{origin}:{start}"
            else:
                label += f"{origin}"
            label += "</FONT></TD></TR></TABLE>>"
            
            node.attr['shape'] = 'none'
            node.attr['margin'] = 0
            # Trim superfluous formatting tags from the label
            node.attr['label'] = label.replace('</B><B>', '')

        for edge in agraph.iteredges():
            # Get the attributes from the corresponding edge in the segment graph
            edge_data = self.block_graph.edges[edge[0], edge[1]]
            # If the edge is a reference edge, make it dashed
            if edge_data.get('reference', False):
                edge.attr['style'] = 'dashed'
                edge.attr['arrowhead'] = 'none'
                # If we want to align blocks on a row, add a weight
                if align_blocks:
                    edge.attr['weight'] = 10

            # Highlight the edge if it is marked as such
            if edge_data.get('highlight', False):
                edge.attr['penwidth'] = 2
                
            # Configure where the edges will connect to the nodes
            if edge[1] == 'end':
                edge.attr['headport'] = 'w'
            else:
                edge.attr['headport'] = 'seq:w'

            # Configure where the edges will depart the nodes
            if edge[0] == 'start':
                edge.attr['tailport'] = 'e'
            else:
                edge.attr['tailport'] = 'seq:e'

            # Generate an edge label based on the edge_label parameter
            if edge_label == 'index' and edge_data.get('chrom_index', False):
                edge.attr['label'] = f"{edge_data.get('chrom_index', '')}"
            elif edge_label == 'phase_layer' and (edge_data.get('src_phase_layer', False) or edge_data.get('tgt_phase_layer', False)):
                edge.attr['label'] = f"{edge_data.get('src_phase_layer', '')} → {edge_data.get('tgt_phase_layer', '')}"

        # Set the graph-level attributes that will be used by the dot rendering engine
        agraph.graph_attr.update(rankdir=graph_attributes.get('rankdir', 'LR'), 
                                 splines='true' if splines else 'polyline',
                                 ranksep=ranksep,
                                 fontnames='svg')
                
        # Custom attributes will override what we just set above
        for node in agraph.iternodes():
            node.attr.update(node_attributes.get(node, {}))
        for edge in agraph.iteredges():
            edge.attr.update(edge_attributes.get(edge, {})) 
        agraph.graph_attr.update(graph_attributes)

        # Other useful arguments for dot (with defaults): ranksep (0.5) searchsize(100) mclimit(10) newrank(false)
        return self.render_dot(agraph, filename, format)
    
    def render_dot(self, agraph, filename=None, format=None):
        # Create the node layout
        agraph.layout(prog='dot')

        # Handle the dot output format as text
        if format == 'dot':
            output = agraph.to_string()
            if filename:
                with open(filename, 'w') as file:
                    file.write(output)
            return output
        
        # For the actual rendering the cairo renderer sometimes produces better results for longer nodes
        # It does not support fonts in SVG however, so we make the PNG instead
        if format == 'png':
            img = agraph.draw(prog='dot', format='png', args='-Gdpi=300')
        elif format == 'svg':
            img = agraph.draw(prog='dot', format='svg')
        else:
            try:
                img = agraph.draw(prog='dot', format=format)
            except ValueError:
                raise ValueError(f'Unsupported file format: {format}')
        
        if filename:  
            with open(filename, 'wb') as file:
                file.write(img)
        
        return img

    def extract_subgraph(self, start, end):
        """
        Extracts a subgraph from the graph that starts at the given start (node, position) pair and ends at 
        the given end (node, position) pair.

        Parameters:
        start (tuple): A tuple containing the node and position to start the subgraph from.
        end (tuple): A tuple containing the node and position to end the subgraph at.
        """
        subgraph_start_node, start_coordinate = start
        subgraph_end_node, end_coordinate = end

        # Ensure we're working with strings
        subgraph_start_node = f'{subgraph_start_node}'
        subgraph_end_node = f'{subgraph_end_node}'

        # Create a copy of the current object
        Gx = deepcopy(self)

        # 1) remove the existing start and end edges, but don't prune any nodes
        for edge in Gx.get_edges():
            (source, source_coordinate), (target, target_coordinate) = edge
            if source == 'start' or target == 'end':
                Gx.remove_edge(*edge, prune=False)

        # 2) create the new edges:
        Gx.connect_to_source(subgraph_start_node, start_coordinate)
        Gx.connect_to_sink(subgraph_end_node, end_coordinate)

        # 3) find all blocks and edges between the start and end nodes
        Gx.make_block_graph()

        paths = nx.all_simple_paths(Gx.block_graph, source='start', target='end')

        traversed_blocks = set()
        traversed_block_edges = set()
        for path in paths:
            traversed_blocks.update(path)
            for b1, b2 in zip(path[:-1], path[1:]):
                traversed_block_edges.add((b1, b2))

        # 4) remove all nodes that do not have a corresponding block in the traversed_nodes set
        traversed_nodes = set()

        for block in traversed_blocks:
            corresponding_node = Gx.block_graph.nodes[block]['original_node']
            traversed_nodes.add(corresponding_node)
        remove_nodes = set(Gx.graph.nodes).difference(traversed_nodes)
        Gx.graph.remove_nodes_from(remove_nodes)

        # 5) remove all edges that do not have a corresponding edge in the traversed_edges set
        traversed_edges = set()

        for block_edge in traversed_block_edges:
            corresponding_edge = Gx.block_graph.edges[*block_edge].get('original_edge', None)
            if corresponding_edge:
                traversed_edges.add(corresponding_edge)

        cut_edges = set(Gx.get_edges()).difference(traversed_edges)
        for source, target in cut_edges:
            Gx.remove_edge(source, target)
            
        return Gx


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("db", help="The path to the gen database file")
    parser.add_argument("--output", help="The path to save the output file to", default=None)
    parser.add_argument("--format", help="The output file format (svg scales well but can be unpredictable for longer nodes, png is slow but tends to give better fitting text)", choices=['svg','png','dot'], default='png')
    parser.add_argument("--collection", help="Filter by collection name", default=None)
    parser.add_argument("--sample", help="Filter by sample name", default=None)
    parser.add_argument("--graph", help="Filter by graph name", default=None)
    parser.add_argument("--ports", help="Visualize as a ported graph without splitting nodes", action="store_true")
    parser.add_argument("--maximize", help="Do not truncate sequences", action="store_true")
    parser.add_argument("--lines", help="Use straight lines instead of splines for edges", action="store_true")
    parser.add_argument("--flex", help="Do not attempt to align blocks from the same segment as a row", action="store_true")
    parser.add_argument("--hide_nodes", help="Comma-separated list of nodes to hide", default="")
    parser.add_argument("--edge_label", help="Label the edges with the given attribute", choices=[None, 'index', 'phase_layer'], default=None)
    parser.add_argument("--graph_attributes", help="JSON string of a dictionary of additional graph attributes", default="{}")
    parser.add_argument("--node_attributes", help="JSON string of a nested dictionary of additional node attributes", default="{}")
    parser.add_argument("--edge_attributes", help="JSON string of a nested dictionary of additional edge attributes", default="{}")

    args = parser.parse_args()

    try:
        graph_attributes = json.loads(args.graph_attributes)
        node_attributes = json.loads(args.node_attributes)
        edge_attributes = json.loads(args.edge_attributes)
    except json.JSONDecodeError:
        print("Error: could not decode JSON string")
        print(graph_attributes)

        return
    
    g = Graph(db=args.db, 
              collection_name=args.collection, 
              sample_name=args.sample,
              block_group_name=args.graph)

    if args.ports:
        img = g.render_graph(filename=args.output,
                             format=args.format,
                             minimize=not(args.maximize),
                             splines= not(args.lines),
                             hide_nodes=args.hide_nodes.split(',') if args.hide_nodes else [],
                             graph_attributes=graph_attributes,
                             node_attributes=node_attributes,
                             edge_attributes=edge_attributes)
    else:
        img = g.render_block_graph(filename=args.output,
                               format=args.format,
                               minimize=not(args.maximize),
                               splines= not(args.lines),
                               align_blocks= not(args.flex),
                               hide_nodes=args.hide_nodes.split(',') if args.hide_nodes else [],
                               edge_label=args.edge_label,
                               graph_attributes=graph_attributes,
                               node_attributes=node_attributes,
                               edge_attributes=edge_attributes)
    
    if not args.output:
        # Try to display in the terminal using kitty, otherwise just print the raw image data
        try:
            subprocess.run(["kitty", "icat"], input=img)
        except FileNotFoundError:
            print(img)
    
if __name__ == "__main__":
    main()