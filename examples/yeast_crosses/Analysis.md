# Yeast breeding experiment
In this example, we want to model a cross of two diploid yeast strains. Depending on which sequence data you have 
available, you can take multiple approaches depending on which genome data you have available. In this document we will
demonstrate a workflow that makes us of variant call files, and a workflow that uses whole genome alignment. In both
cases we start from two beer yeasts from the [1000 Yeast Genome collection](http://1002genomes.u-strasbg.fr/): Orval 
trappist yeast strain DBVPG6695 from Belgium and American ale strain 1.3_Safale_US05 (codenames BRQ and CFD).

## Starting from variant calls



bcftools query -l 1011Matrix.gvcf.gz | head > samples.txt
bcftools view -S samples.txt -r chromosome1,chromosome2 filtered.vcf.gz 1011Matrix.gvcf.gz
bcftools annotate --rename-chrs mapping.txt 1011Matrix.gvcf.gz -o 1011Matrix_renamed.gvcf.gz



https://www.nature.com/articles/s41586-018-0030-5

Saccharomyces_cerevisiae.R64-1-1.dna.toplevel.fa.gz
http://sgd-archive.yeastgenome.org/sequence/S288C_reference/genome_releases/S288C_reference_genome_R64-1-1_20110203.tgz

## Starting from assembled genomes
First we download the genome assembly archive and extract the genome sequence for the strains we want to mate.
```
wget http://1002genomes.u-strasbg.fr/files/1011Assemblies.tar.gz
tar -xzf 1011Assemblies.tar.gz GENOMES_ASSEMBLED/BRQ_4.re.fa GENOMES_ASSEMBLED/CFD_4.re.fa
```

Then we use [Cactus](https://doi.org/10.1038/s41586-020-2871-y) to create a graph via whole genome alignment.
The Comparative Genomics Toolkit provides a Docker container that includes all the infrastructure needed to do so.
We launch the container and bind our working directory to the /data directory in the container:

```
 docker run --mount type=bind,source=$PWD,target=/data -it quay.io/comparative-genomics-toolkit/cactus:v2.9.0 bash
```

We tell Cactus which genomes to align by providing a text file containing names and file paths. You can set this up
manually, or generate it for all fasta files in our current directory and subdirectories: 
 
```
find . -type f -name "*.fa" -printf "%f %p\n" | awk '{ sub(/\..*$/, "", $1); print $1, $2 }' > file_list.txt
```

Then we can start the Cactus pipeline using the command list below. `./js` specifies where intermediate files are stored,
outName and outDir influence the pipeline, and `--gfa` tells Cactus to output a GFA file. While this is a graph based
alignment, we must still designate one genome as reference, in this case we pick BRQ.
```
cactus-pangenome ./js file_list.txt --outDir ./cactus_output --outName cross --reference BRQ_4 --gfa
```

After this completes (approximately 10 minutes), we can load it back into gen by running:

```gen --db main import --gfa cactus_output/cross.gfa --name my_collection```