# Graph Operators

_Make_ and _derive_ operations work purely with graphs - they take graphs from the database as input and produce new graphs as output, unlike update operations which incorporate external data. The fundamental difference between _make_ and _derive_ lies in how they handle sequence relationships across their inputs and outputs: _make_ operations create graph objects by making new connections between distinct sequence graphs, whereas _derive_ operations take place within a common reference backbone. For example: when you derive the intersection of sequence variants of chromosome I across experimental samples to study the impact of evolved mutations, you still end up with an object that represents “chromosome I”. In contrast, stitching together a set of engineered enzyme variants and a library of secretion tags is an operation in which you have distinct input objects (“enzyme¨ and “secretion tag”) and end up with a single object that represents a diversity of secreted enzymes.
This is not intended to be strict rule, and there are examples where the line blurs: for example, splitting a computationally designed chromosome into chunks for synthesis, and then stitching them together is a workflow that arguably takes place entirely within the sequence context of the chromosome, yet those are still considered to be _make_ operations. Likewise, while there is a certain directionality to the intent of _derive_ and _make_ operations (respectively analysis and desig of experiments), users should feel empowered to combine operations as needed.
## Derive
### Subset
Pangenome graphs can get rather big and unwieldy, but by deriving a subset we can extract a specific region to work on in a sequence editor, and later on merge it back into the complete sequence. To illustrate, the first panel in Figure 1 represents a sequence in which an insertion and substitution have taken place. We want to take a closer look at the region indicated in bold, and use the subset command as follows:
```console
gen derive subset —sample S1 —region chr1:7-12 —new-sample my_locus
```
In this case we have a sample S1 that contains a sequence graph with the name ‘chr1’. The bounds of the subgraph we want to extract can be specified either as a region with linear coordinates, a named accession, or a pair of sequence segments. Here we use a region string “chr1:7-12’, which can be read as “the sequence from position 7 through 12 (but not including 12) on the main linear path of the graph with the name chr1”. Gen graphs generally have a designated path that serves as a linear backbone to which variants are referenced. An error will be raised if this path is absent. If we wanted to use a different path on chr1, we would also specify the name of that path as ‘´—backbone’ argument along with the sample and region.
It must be noted that the derived subset does not have be a linear sequence. These two points in the graph can be seen as a source and sink node that induce a subgraph consisting only of the blocks and edges that can be accessed by walks from the source to the sink node. Any edges to or from blocks that are outside of the subset will be rewritten to edges to the end and end start dummy nodes, respectively.
Alternatively, users can specify the boundaries directly by entering a pair of block identifiers using the `—start-block` and `—end-block` options. Block identifiers can be obtained from visualizations or exported GFA files, where they take the role of segment names. They currently follow the format `m.n`, where m and n refer to the node ID in the sequence graph, and starting coordinate of the block on that node, respectively. This ensures consistency between multiple versions of the sequence graph, since node IDs do not change when new blocks are created. The end coordinate of the block can be derived from its length, but this may be made explicit in the future if advantageous (for example m.n.l where l is equal to the block length). Blocks are not stored in the database directly, but are instead inferred from nodes and edges as needed. This also means that a block does not have to exist _a priori_ when specifying a start or end block to create the subset.¨
Lastly, users can define a subset through a named accession instead. An accession also defines a part of the graph through coordinates on a linear path, but they are stored by name in the gen database. and referenced through the ‘—accession’ option of the subset command. The accession does not have to be defined in the sample from which you derive a subset, it is automatically translated from the coordinate frame of the sample in which it was originally defined. This is only possible if the graphs are related trough the sample lineage, and an error will be raised if they are not.
In the example above we use the option `—new-sample my_locus` to create a new sample called my_locus, otherwise gen will edit the original sample instead. While we could always roll back to the previous sample, it can still be useful to create a new sample named after the region that was extracted.
Because a subset does not include edges and nodes that are no longer relevant to the user, it can be exported to a file that is much smaller in size and is generally easier to visualize or edit. But as long as the bases (or residues) at the very ends stay intact, edited subsets can be easily imported and re-attached to a larger backbone graph using the ‘superset’ command.
### Superset
Subset sequence graphs and their descendants can be (re)attached to their parent graph or any graph that shares at least the terminal nodes of the subset. This is done using the superset operation, which incorporates any new nodes, , edges, annotations or other labels
moves the start and end pointers of a subgraph to given backbone, or more precisely, it cuts the start and end edges and computes the union of both graphs. Both the subgraph and backbone are passed to gen in the form of samples. If the backbone sample contains multiple block groups, you can specify the correct target by passing the `—blockgroup-name` option along with the `—backbone` sample. The graph passed as the `—sample` option is modified in place unless the `—new-sample ` is used to define a new sample.

```
gen derive —attach —sample S2 —backbone S1 —new-sample S3
```






Figure 1: a) DNA sequence graph representing the insertion of the trinucleotide CCC, and a substitution of AGG by TGA; the section we wish to detach is indicated in bold. Numbers below the blocks indicate the gen node id and corresponding coordinate range. b) Subgraph that was detached, note the single-nucleotide terminal blocks A and A. c) Sequence graph with the same node 1 backbone; the terminal blocks from panel b are highlighted in bold. d) Result of attaching the subgraph from panel b to the graph in panel c.







### Union
A graph merge operation is performed by taking the union of their edges, and any resulting new blocks are calculated on the fly. This is useful to combine distinct sequencing datasets or engineered libraries in a single object that can be more easily handeled and tracked.







Figure x:









Figure x:




**_Figure 3_**: _..._





### Intersect
The complementary operation to a union, is to retain only the edges that are present in both, i.e. the intersection.

### Difference

## Build

### Stitch
`gen build —stitch —samples S1,S2,... —new-sample Sx —overlap 20`





Figure x:





### Split
`gen build —split —sample S1 —name X —position 1,2,3,... —overlap 20 —new-sample S2`






Figure x:








Figure x:





### Splice

### Circularize
