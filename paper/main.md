# Introduction

In synthetic biology, a common workflow is iterative engineering. This is where a strain is subject to multiple,
sequential rounds of engineering to arrive at desired states. Pangenomes are an underutilized but highly useful tool for
modeling iterative engineering. The graph simultaneously represents the edits to the genome as well as enabling
sophisticated analyses. However, while representing changes as a graph enables analyses of this workflow, the existing
data models are not optimal for representing this type of iterative engineering. Thus, we set out on a data model which
could be a common, growable database of both reference and sample-specific sequences.

While not directly related to GFAs, we saw another opportunity to couple graph models with another common need in
synthetic biology -- tracking engineering. The current state of sharing the evolution of a strain to its final state is
ad-hoc. We saw that the addition of variants to a graph can be captured as a set of changes. And if we can annotate
these set of changes, we are able to devise a git-like system for tracking changes to samples. This enables workflows
common in software engineering, such as continuous integration and code review. Additionally, this naturally links the
process of genetic engineering with tracking the work that was done. This provides an easier path for follow-up work
such as patent applications and regulatory compliance.

For these two purposes, Gen was created. Gen offers a growable database of tracking changes to a sequence. Changes are
recorded and can be gathered into a patch, allowing for sharing of work, code review, and testing. Other git-like
functions are also included, such as the ability to create new branches and merge work across branches. Thus, teams can
work in parallel and incorporate changes across branches when necessary. Gen is written in rust and binaries are
available for Linux and OSX platforms.

# Methods

## Graph Data Model

The core data model of Gen is an append-only ledger of edges. An edge is represented as a (source_node,
source_coordinate, source_strand) to a (target_node, target_coordinate, target_strand). This allows us to reference a
position within a node without having to create a new node. A common alternative is to use a segment graph, where a long
sequence is broken up into new chunks as changes are incorporated into it. An update to a segment graph requires
identifying where the change will be inserted, splitting the prior chunk in half, and updating the edges from the
previous segment to the new segment. The edge model requires no updates, only the addition of new edges. This has
significant speed advantages, as no queries and updates are required of the database.

## Graph Representation

Fundamentally, gen captures the pan-genome graph model by storing sequences and edges between the sequences, but the
model is a little more complicated, as we'll see.  Each edge has a coordinate of one sequence as a source, and a
coordinate of a sequence (possibly the same one) as a target.  Gen creates new sequences and edges whenever importing or
updating from a file and adds them to the appropriate graph structure.  One important additional data model is a node.
Each node corresponds to a sequence, but a sequence may have multiple nodes.  The reason for this is that if a file
specifies multiple copies of the same sequence in a row, say two "T" segments, we need to represent the T with two
copies.  Otherwise, if gen used the same sequence to represent them, gen would create an edge from the T sequence to
itself, creating an unintentional loop.  Instead, gen creates two nodes, each referencing the T sequence, and creates an
edge between the two nodes.  In this way gen can faithfully represent duplicate segments.  So each edge links from a
source coordinate on a node to a target coordinate on a node.  Source and target node coordinates implicitly refer to
the sequence the node is associated with.

Coordinates in gen are 0-based, and ranges are closed/open.  For instance, (0, 5) represents the first five base pairs of
a sequence, and (10, 11) represents the eleventh base pair.

Gen represents an insertion or replacement by creating a new sequence, a corresponding node, and two edges.  The first
edge has its source as the start of the region being replaced (or the coordinate of the insertion start) on the source
node, and its target as the start of the created node.  The second edge has its source as the end of the created node,
and its target as the end of the region being replaced (or the coordinate of the insertion start plus one).

Gen represents a deletion as just one new edge, from the start coordinate of the region being deleted to the end
coordinate of that region.

Gen represents one contig, say a chromosome, using one directed graph of nodes and edges.  The internal gen data model
for a graph is called a block group.  Gen uses a join table between one block group and multiple edges to record all the
edges (and infer all the nodes and sequences) in a block group.  Because of the additive nature of insertions,
replacements, and deletions, block groups can only grow over time.  The block group represents all possible sequences
that can be generated from the graph.

Gen defines a virtual start node and a virtual end node which each represent an empty sequence, and which every block
group uses to record the first and last nodes in the group.  Gen creates an edge from the virtual start node to each
node that actually occurs at the beginning of the graph, and similarly creates an edge from each real node that occurs
at the end of a graph to the virtual end node.

A path specifies a unique way to traverse edges from the virtual start node to the virtual end of a block group.  The
path specification is a list of edges.  Each pair of contiguous edges in the path must share the same node as the target
of the previous edge and the source of the next edge.  The stretch of sequence between the two node coordinates is taken
to be a subsequence of the path.  Each edge also has a source strand and a target strand.  Each pair of contiguous edges
in a path must have the same strand.  If the strand is forward, the stretch of sequence between the two edges is on the
forward strand, and if the shared strand of the edges is reverse, the sequence between the edges is on the reverse
strand.

A block group may have multiple paths.  If so, the most recent one is treated as the "current" path, and represents the
current canonical sequence for the block group.

An accession is a way of specifying a subgraph.

## Data Organization

Data is stored in on-disk sqlite database files that act as compartmentalized repositories. Within each database file,
Gen has 3 mechanisms for facetting data -- a graph name, a sample, and a collection. The name of a graph refers to its
underlying biological identity as polynucleotide or polypeptide; the sample identifies a specific variation or instance
of the graph; and a collection groups a set of graphs together. Practically this means that "chromosome I in sample S1"
is a different object than "chromosome I in sample S2", and one graph can be modified without forcing the change to
propagate to the other. But because both graphs share a common universe of node and edge identifiers, variants and
annotations can still be easily compared or propagated between samples if desired. 

The interpretation of the sample facet is flexible by design. Depending on the application domain or needs of the user a
sample could refer to a physical specimen or individual, for example, but also an environmental population or pool of
experimental samples. Samples do not have to have a counterpart in the physical world, they can be purely virtual and
refer to a digitally designed sequence or model-derived screening library. Multiple graphs can be associated to the same
sample (e.g. all chromosomes in a genome), but not every graph has to be associated with a sample. If it is not, that
graph is said to be part of the _null_ sample.

Each graph is comprised of nodes and edges. An example of importing a fasta and applying a vcf is used to show how the
model are added. First, for each record such as chr1 and chr2, a node is created of the entire sequence. Two new edges
are added to the new node, connecting the start of the sequence to a source node and the end to a sink node. These nodes
are used to simplify the process of finding the starts and end of a graph. When a vcf is applied, for non-deletions, a
new node is created with the alternative sequence and two new edges are created -- an edge from the reference sequence's
source node (such as chr1) to the new node, and an edge from the end of the new node back to the reference sequence. For
deletions, only a new single edge is required to represent the new path.

## Updating the Graph

Updating a graph has varying levels of difficulty. Gen's data model supports polyploidy and as such it may not be
possible to have a consistent index into a sequence. For example, if a heterozygous change is present at base 15 of
chromosome 1, all positions after that position may not be unambiguously indexable. For this, various operations are
possible to address the increasing levels of complexity.

A vcf file may be provided to the `update` command to incorporate any changes identified. 

Accessions can be created as part of updates to provide areas of a graph that can be referenced unambiguously.

Alignments may be used to insert new sequences into a graph.

A GFA may be provided with updates to a path.
 
A fasta file may be inserted at a given position.

A custom library format may be provided. 

A GenBank file can be provided with changes annotated. Currently, we only support changes encoded in the Geneious
format.

Changes can be made with respect to the initially imported sequence (often the reference sequence), or to a derived
sample graph.

## Sample Lineage

Gen represents lineage across updates and other operations using samples.  For instance, importing a fasta file into a
new collection creates the null sample with block groups for each contig.  If we then update that collection with a vcf
file, gen creates new samples specified in the vcf, and any new sample in the vcf is created as a child of the null
sample.  Gen copies any graph data created by the fasta import in the null sample over to each child sample before
applying the updates specified for each sample.

Using samples seems to be the best fit for tracking lineage, instead of say updating block groups with new information
within an existing sample.  A sample can be used to capture the two main use cases for gen operations: Intended edits,
and observed variations.  An intended edit can be represented by updating an existing sample using say a fasta file plus
a region and coordinates.  That operation would capture the integration of a cassette, and create a new sample with a
path that represents the updated sequence.  That new sample would be "virtual" in the sense that it doesn't exist in the
real world, but is treated by gen as separate from any real world samples, which seems appropriate.  Observed variations
can be represented by updating an existing sample using say a vcf file.  That operation will create new samples, each of
which represents variations observed during sequencing.  In this way, gen spans both intended and unintended genetic
changes, and can be used to track changes across repeated cycles of development and testing.

## Pooling

Add details about how gen represents pooling.

## Phasing

While we continually add new edges, we need to indicate which edges belong together. Phasing is supported and changes on
the same chromatid will be exported together. Phasing is stored as the chromosome_index field, xx, and yyy.

A similar need for phasing is within combinatorial assembly. This is where a set of parts are engineered in series,
leading to a massive diversity of end products. However, in many cases users want a desired set of paths through these
parts. This is another form of phasing, where a set of parts are linked together much like a haplotype.

To support both of these cases, the concept of a phase layer is utilized. A phase layer groups together variants
representing both haplotypes and linked parts.

## Operations

There are various methods to change a graph, which we term an operation. Each operation is analogous to a commit in git.
It is recorded under the operations command, and can be exported to a patch for sharing.

## Translating coordinate schemes

Annotations can be propagated through the graph structure. Thus, annotations on the reference genome can be translated
into the coordinates of new samples. Coordinates are translated with the following rules:

* If the start and end of the annotation range on the reference map to valid coordinates on the sample sequence, gen
  translates the entire annotation to the later sample.  Gen does this even if there is a gap in the middle of the later
  sequence that doesn't correspond to anything in the reference sequence, say if a subsequence were replaced.
* If say the start of the range translates to a valid coordinate on the later sample, but the end does not (say because
  a replacement wiped out the stretch of the reference sequence that contained the end), gen truncates the translated
  annotation range to only include the stretch of sequence that the reference shares with the later sample.

## Distribution and Collaboration

Gen is designed to facilitate collaboration across teams in a distributed manner. Changes can be shared by creating
patches from operations and treated like a git patch. A patch contains models to add as well as upstream dependencies of
changes.

For visualizing patches, the `patch-view` command can be used to generate a DOT formated graph (fig.
[dot_example](dot_example/final.svg)). 

## Database

Gen uses a sqlite database. Sqlite was chosen because it is extensively used across all major platforms. It is
additionally a local database, enabling users to use gen without having to set up more complicated databases or require
an internet connection to a cloud service. Sqlite also allows extensibility of the data model via schema migrations and
users can extend the Gen model with new tables to add custom functionality if desired. Updates to gen will be possible
with schema migrations, which help mitigate the version issues plaguing many bioinformatic data formats.

Another consideration for sqlite is it can be accessed via web assembly (WASM), meaning databases stored in places such
an Amazon s3 bucket can be accessed without any dedicated servers. Thus, applications can be developed and ran entirely
on the client side.


# Discussion

* The lack of a growable database. Minor changes required regenerating the entire graph and re-exporting it. Graph
  genomes grow at a rate faster than a linear alignment. For example, a single copy of hg38 sufficies for analyses of
  many samples. Whereas with sample specific graphs, each sample requires its own graph as an additional tracked asset.
  This also made it difficult to compare across analyses, as nodes which may be in common between two graph samples
  would have different labels. This could be mitigated by creating a graph encompassing all samples, but this approach
  simply delays the inevitable addition of a new sample, a derivative lineage, or an updated set of variant calls.

*  
