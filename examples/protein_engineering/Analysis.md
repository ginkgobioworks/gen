# Tracking protein variant libraries
## Site-directed Homologous Recombination

This example recreates a protein engineering library made through site-directed, homologous recombination guided by structure-based computation (SCHEMA) ([Otey 2006](doi.org/10.1371/journal.pbio.0040112)). Starting from three existing cytochrome P450 proteins, approximately 3,000 artifical (chimeric) proteins were constructed and tested.

"We generated an artificial family of cytochromes P450 by recombining fragments of the genes encoding the heme-binding domains of three bacterial P450s, CYP102A1 (also known as P450BM3), CYP102A2, and CYP102A3 (abbreviated A1, A2, and A3), which share ̃65% amino acid identity [21, 22] (Figure 1). The parent proteins are 463–466 amino acids long and contain the single substitution F87A (A1) or F88A (A2 and A3), which increases the peroxygenase activities of these heme domains. [...] The final design has crossovers located after residues Glu64, Ile122, Tyr166, Val216, Thr268, Ala328, and Gln404, based on the numbering of the A1 sequence"

```console
wget https://rest.uniprot.org/uniprotkb/P14779.fasta https://rest.uniprot.org/uniprotkb/O08394.fasta https://rest.uniprot.org/uniprotkb/O08336.fasta
cat O08336.fasta O08394.fasta P14779.fasta > parents.fa
```

```console
 docker run --rm --volume $PWD:/data --workdir /data pegi3s/muscle -in parents.fa -out parents_aligned.fa
```

WIP: translate_breakpoints.py to generate the parts.fa and layout CSV

-> followed by combinatorial update into gen

## Site Saturation Mutagenesis
GB1 dataset: Adaptation in protein fitness landscapes is facilitated by indirect paths
 https://doi.org/10.7554/eLife.16965

"In this study, we investigated the fitness landscape of all variants (204 = 160,000) at four amino acid sites (V39, D40, G41 and V54) in an epistatic region of protein G domain B1 (GB1, 56 amino acids in total)"

There's also the 2014 publication _A Comprehensive Biophysical Description of Pairwise Epistasis throughout an Entire Protein Domain_ by Olson et al. (doi: [10.1016/j.cub.2014.09.072](https://doi.org/10.1016/j.cub.2014.09.072)) but that's more DMS

## Deep Diversification with Printed Oligonucleotids 
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