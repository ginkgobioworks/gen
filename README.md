# Gen
Gen is a version control system for genetic sequences. It efficiently stores genome-length sequences and sequence
variations, with native support for polyploid genomes and pooled genotypes. Each project is organized into a repository,
where collections of sequences and associated data are stored and tracked over time. Within a repository, branches can
be created to explore different modifications or variations without affecting the main project. These branches can later
be merged to integrate results from different experiments or collaborators.

The gen client can import standard sequence file formats from sources like NCBI and genetic design tools. The sequence
model takes the form of a graph structure as shown in the figure below. Each molecule is made up out of a network of
_nodes_ that represent sequence fragments, and _edges_ that define how sequence fragments are connected. Multiple
molecules are organized into _collections_ that could represent the different chromosomes in a cell or DNA in a reaction
mixture. Molecules generally start out as a single node that holds a reference sequence, and new edges and nodes are
added for every sequence variant that is designed or observed. To reconstitute a linear sequence, the client walks from
node to node along a defined _path_. This data model allows the representation of a wide range of biological complexity
-- from a single molecule to complex cultivars and cell lines, including any naturally occurring variation in addition
to intended engineering.

`<figure 1>`

Imported feature annotations can be propagated from path to path in a sequence-agnostic way that relies on coordinate
translation. Paths can also be compared to one another to detect features that are common or different between sets of
paths, which can be used to analyze experimental data. A _sample_ object represents the subset of the possible paths and
edges that is actually present in an experimental sample. A value between from 0 and 1 is assigned to each edge and path
to represent the probability that and edge or path is observed. These numbers can be derived from sequencing results, or
set by the user to represent an isolate or cloning reaction. This allows a user to focus on distinguishing features of a
molecule by masking out irrelevant edges. The figure below demonstrates how this can be used to represent a polyploid
genome obtained through cross-breeding. Like paths, samples can be compared to one another to detect differences and
common features.

`<figure 2>`

## Installing from Source
Make sure you have a Rust compiler installed on your system. You can install the Rust toolset using the [rustup installer](https://rustup.rs/).


1. Clone the [source](https://github.com/ginkgobioworks/gen) with `git`:

   ```sh
   git clone https://github.com/ginkgobioworks/gen.git
   cd rust
   ```

2. Compile the gen package and its dependencies:

    ```
    cargo build
    ```

## Usage

### Starting a new repository
`gen --db <file> init`

<!-- Importing sequence files into a collection -->

`gen --db <file> import --fasta <file> --name <string>`

### Cloning an existing repository
<!-- -Exporting sequence files and slices -->
<!-- -Shallow checkout -->
<!-- -Exploring a repository: listing collections, samples, paths (molecules) -->
<!-- -Translating coordinates between paths -->

### Recording sequence changes
<!-- From a VCF file -->
Sequence variants observed through NGS can be imported into a gen repository via standard VCF file obtained from variant callers like Freebayes, GATK, or DeepVariant. [...]
<!-- -From a sequence file that was edited externally -->

<!-- -From the gen command line -->

<!--

Inserting one or more genetic parts at a specific locus can be done using the `gen update <location> <sequence>` command. The location parameter is given as the combination of a path (molecule) identifier and a numerical coordinate referenced to that path, separated by a colon. If the option `--propagate` is used, all other paths that include the location will be updated as well. By default the molecule itself is changed, but by using the option `--new` new molecules are created and their path IDs are returned to the user. Multiple inserts can be specified using the `--fasta <file>` argument. In this case new path IDs are always returned to the user, but unless the option `--new` is given the original path is removed.

To insert multiple parts successively in the same location, use the option `--combinatorial <file>` with a CSV file where each column represents a slot within the target position, and the rows list which parts can be introduced in that slot, as specified by their identifiers from the fasta file. By default a full factorial design will be generated, but the option `--associations <file>` allows you to restrict the experimental design to the combinations listed as individual rows, with columns still corresponding to the same slots. Columns that are left empty are treated as being full factorial. The option `--anti-associations <file>` provides the inverse functionality: a similar csv file is provided, but each row represents genetic that should not occur together as combinations.

-->

### Associating numerical data with paths and edges
<!-- -From CSV referencing named paths -->

<!-- -From mapped sequencing reads -->

### Commits and merges
<!-- -Local only for now -->







