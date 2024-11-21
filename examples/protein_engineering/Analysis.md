# Tracking protein variant libraries
## Site-directed Homologous Recombination

This example recreates a protein engineering library made through site-directed, homologous recombination guided by structure-based computation (SCHEMA) ([Otey 2006](doi.org/10.1371/journal.pbio.0040112)). Starting from three existing cytochrome P450 proteins, approximately 3,000 artifical (chimeric) proteins were constructed and tested.

"We generated an artificial family of cytochromes P450 by recombining fragments of the genes encoding the heme-binding domains of three bacterial P450s, CYP102A1 (also known as P450BM3), CYP102A2, and CYP102A3 (abbreviated A1, A2, and A3), which share ̃65% amino acid identity [...] The final design has crossovers located after residues Glu64, Ile122, Tyr166, Val216, Thr268, Ala328, and Gln404, based on the numbering of the A1 sequence"

First we download the sequences of the parent proteins and combine them into one fasta file:

```console
wget https://rest.uniprot.org/uniprotkb/P14779.fasta https://rest.uniprot.org/uniprotkb/O08394.fasta https://rest.uniprot.org/uniprotkb/O08336.fasta
cat O08336.fasta O08394.fasta P14779.fasta > parents.fa
```

Next we create multiple sequence alignment using the Muscle application, for example through Docker:

```console
 docker run --rm --volume $PWD:/data --workdir /data pegi3s/muscle -in parents.fa -out parents_aligned.fa
```

With this alignment, we can then translate the crossover points from the A1 reference frame to all other proteins. The msa_crossover.py Python script performs those calculations, creates the protein segments, and saves them to disk in a format readable by the gen update command.

```console
python msa_crossover.py parents_aligned.fa 64 122 166 216 268 328 404
```

The default output of this script is a directory called 'output' that contains the files 'layout.csv' and 'segments.fa'. We now set up our gen repository, import one of the parents to have a starting point, and then perform an update operation.

```console
gen init
gen defaults --database test.db --collection protein
gen import --fasta P14779.fasta
gen update --parts output/segments.fa --library output/layout.csv --path-name sp\|P14779\|CPXB_PRIM2 --start 0 --end 657
```

![Recombination library](docs/figures/protein_figure_1.svg)

VG can also be used to generate graphical representations, but overwrites the node identifiers. The identifiers can be restored by operating as follows:
```console
vg convert --gfa-in library.gfa --gfa-trans translation_table.txt --vg-out | vg view --vg-in - --dot --color --simple-dot | dot -Tsvg -o library.svg

IN='P450_chimera.svg'; cp $IN ${IN%.*}_fixed.${IN##*.} && while IFS=$'\t' read _ new old; do sed "s#font-size=\"14.00\">$old</text>#font-size=\"14.00\">$new</text>#g" ${IN%.*}_fixed.${IN##*.} > temp_file.html && mv temp_file.html ${IN%.*}_fixed.${IN##*.}; done < translation_table.txt
```

## Site Saturation Mutagenesis
GB1 dataset: Adaptation in protein fitness landscapes is facilitated by indirect paths
 https://doi.org/10.7554/eLife.16965

"In this study, we investigated the fitness landscape of all variants (204 = 160,000) at four amino acid sites (V39, D40, G41 and V54) in an epistatic region of protein G domain B1 (GB1, 56 amino acids in total)"

There's also the 2014 publication _A Comprehensive Biophysical Description of Pairwise Epistasis throughout an Entire Protein Domain_ by Olson et al. (doi: [10.1016/j.cub.2014.09.072](https://doi.org/10.1016/j.cub.2014.09.072)) but that's more DMS

## Deep Diversification with Printed Oligonucleotides 
AAV Dataset

Bryant et al. Nature Methods 2021
https://www.ncbi.nlm.nih.gov/bioproject/PRJNA673640/
https://github.com/churchlab/Deep_diversification_AAV 
https://github.com/google-research/google-research/tree/master/aav 


## Natural Variants

The Protein Gym datasets are a good way to demonstrate graph generation starting from MSA. 

```console
git clone https://github.com/fawaz-dabbaghieh/PanPA.git
cd PanPA
pip install setuptools Cython
python setup.py install
cd ..
```

```console
wget https://marks.hms.harvard.edu/proteingym/ProteinGym_v1.1/DMS_msa_files.zip
```

Converting a2m to fasta:

```console
grep -v "^#" input.a2m | sed '/^>/! s/[a-z.]/-/g' > output.fa
```

## Fitness Landscape Inference for Proteins (FLIP) 
Another commonly referenced open source dataset

## Inscripta example
Dewachter et al. 

Includes NGS counts

TODO: demonstrate DNA <-> protein graph conversion (same topology)



# Bibliography
Otey, C. R., Landwehr, M., Endelman, J. B., Hiraga, K., Bloom, J. D., & Arnold, F. H. (2006). Structure-guided recombination creates an artificial family of cytochromes P450. PLoS biology, 4(5), e112. https://doi.org/10.1371/journal.pbio.0040112 

Dewachter, L., Brooks, A.N., Noon, K. et al. Deep mutational scanning of essential bacterial proteins can guide antibiotic development. Nat Commun 14, 241 (2023). https://doi.org/10.1038/s41467-023-35940-3