# Gen
`Gen` is a version control system for genetic sequences. It efficiently stores genome-length sequences and sequence variations, with native support for polyploid genomes and pooled genotypes. Each project is organized into a repository, where collections of sequences and associated data are stored and tracked over time. Within a repository, branches can be created to explore different modifications or variations without affecting the main project. These branches can later be merged to integrate results from different experiments or collaborators.

Sequences could be genomes from NCBI for example, or engineered plasmid constructs from your favorite genetic design tool. In either case, the `gen` client imports standard file formats and stores sequences as _pangenomic molecules_ that represent not just a single strain, cultivar, or cell line, but also thousands of engineered or natural genotype variants in large scale laboratory experiments. Gen molecules take the form of a graph structure as you can see in the figure below. Each molecule is made up out of a network of nodes _blocks_), that represent sequence fragments, and links (_edges_) that define a connection. Molecules generally start out as a single block that holds a reference sequence, and new edges and blocks are added for every sequence variant you design or observe. To reconstitute a linear sequence, the client walks from block to block along a _path_ like shown in the figure below.

`<figure 1>`

Advantages are:
(1) very large collections of whole genomes can be stored as paths with modest hardware requirements. 
(2) solves the problem of coordinate reference frames changing between genetic lineages during the life cycle of a project. Even as your sequences acquire deletions and insertions, coordinates can be easily translated from path to path.
(3) version control

Next to paths, `gen` provides another way to describe individual strains or experiments: _samples_.

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
<!-- -From a sequence file that was edited externally -->

<!-- -From the gen command line -->

Inserting one or more genetic parts at a specific locus can be done using the `gen update <location> <sequence>` command. The location parameter is given as the combination of a path (molecule) identifier and a numerical coordinate referenced to that path, separated by a colon. If the option `--propagate` is used, all other paths that include the location will be updated as well. By default the molecule itself is changed, but by using the option `--new` new molecules are created and their path IDs are returned to the user. Multiple inserts can be specified using the `--fasta <file>` argument. In this case new path IDs are always returned to the user, but unless the option `--new` is given the original path is removed.


To insert multiple parts successively in the same location, use the option `--combinatorial <file>` with a CSV file where each column represents a slot within the target position, and the rows list which parts can be introduced in that slot, as specified by their identifiers from the fasta file. By default a full factorial design will be generated, but the option `--associations <file>` allows you to restrict the experimental design to the combinations listed as individual rows, with columns still corresponding to the same slots. Columns that are left empty are treated as being full factorial. The option `--anti-associations <file>` provides the inverse functionality: a similar csv file is provided, but each row represents genetic that should not occur together as combinations.

### Associating numerical data with paths and edges
<!-- -From CSV referencing named paths -->

<!-- -From mapped sequencing reads -->

### Commits and merges
<!-- -Local only for now -->







