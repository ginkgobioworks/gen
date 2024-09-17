# Gen
`Gen` is a version control system for genetic sequences. It efficiently stores genome-length sequences and sequence variations, with native support for polyploid genomes and pooled genotypes. Each project is organized into a repository, where collections of sequences and associated data are stored and tracked over time. Within a repository, branches can be created to explore different modifications or variations without affecting the main project. These branches can later be merged to integrate results from different experiments or collaborators.

Sequences could be natural genomes from NCBI for example, or engineered constructs from your favorite genetic design tool. In either case, the `gen` client imports standard file formats and stores sequences as _pangenome molecules_ that represent not just the sequence itself, but also every engineered variant or natural mutation observed via DNA sequencing. This is made possible by implementing a graph model with nodes that represent sequence fragments and edges that represent how these fragments can be connected in a path to reconstitute a linear sequence. The figure below illustrates what this looks like for different types of variants.

`<figure 1>`

In this way, pangenome molecules (internally referred to as block groups) can represent not just a single strain, cultivar, or cell line, but the entire populations or experimental library associated with that collection. By storing sequences as paths in a common graph `gen` solves the problem of coordinate reference frames changing between genetic lineages during the life cycle of a project. Even as your sequences acquire deletions and insertions, coordinates can be easily translated from path to path.

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







