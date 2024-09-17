# Gen
`Gen` is a version control system for genetic sequences. It efficiently stores genome-length sequences and sequence variations, with native support for polyploid genomes and pooled genotypes. Each project is organized into a repository, where collections of sequences and associated data are stored and tracked over time. Within a repository, branches can be created to explore different modifications or variations without affecting the main project. These branches can later be merged to integrate results from different experiments or collaborators.

We do this by storing sequences in a graph model. For example, when you add a FASTA file to a repository, a 'block group' is created for each sequence—typically corresponding to a chromosome or contig. Initially, each group contains a single block that acts as the coordinate reference. As genome edits are made, or variations are observed through sequencing, gen automatically tracks these changes by adding new blocks and edges to the block group. The figure below illustrates how this process works for different types of variants.

Each block group can represent not just a single strain, cultivar, or cell line, but an entire population or library. Individual linear sequences are stored as paths, which represent a walk over the blocks and edges within the group. This design allows you to refer to specific loci across different sequences without worrying about changes in the coordinate reference frame, ensuring that your analyses stay consistent as sequences evolve."

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







