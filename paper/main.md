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

Explanation on nodes, edges, paths and accessions.

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

xx yy zz

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