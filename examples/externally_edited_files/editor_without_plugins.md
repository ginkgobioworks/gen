# Tracking sequence edits across applications - without plugins

In this example a user makes changes in a third party genetic sequence editing suite like the freely available [ApE](https://jorgensen.biology.utah.edu/wayned/ape/) or commercial software like [Geneious](https://www.geneious.com/). The task at hand is: "change the selection marker of the Bacterial Artificial Chromosome (BAC) pBeloBAC11 ([file](./pBeloBAC11.gb) provided by NEB) from chloramphenicol resistance to kanamycin resistance." We will use the kanamycin resistance marker from the Tn5 transposon (GenBank: U00004.1), which we can download from NCBI:

```console
wget "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&rettype=gb&id=U00004.1" -O Tn5.gb

```

Gen does not support feature annotations yet, so we will first convert these genbank files to fasta using BioPython:
```console
python -c "from Bio import SeqIO; SeqIO.convert('pBeloBAC11.gb', 'genbank', 'pBeloBAC11.fa', 'fasta')"
python -c "from Bio import SeqIO; SeqIO.convert('Tn5.gb', 'genbank', 'Tn5.fa', 'fasta')"

We then initialize a new gen repository and import the BAC sequence.
```console
gen init
gen defaults --database my_project.db --collection plasmids
gen import --fasta pBeloBAC11.fa
```

Gen does not yet keep track of files, so let's copy the imported files to a snapshot directory:
```console
mkdir snapshot_1
cp pBeloBAC11.* snapshot_1
```

Then we move over to our sequence editor, where we edit the BAC sequence to replace the chloramphenicol acetyl transferase (cat) coding sequence by the coding sequence of aminoglycoside-3'-O-phosphotransferase from Tn5 (don't forget to reverse complement it before pasting). We then save the resulting sequence, overwriting the original file.

We now have two files called pBeloBAC11.gb, one in the working directory, and one in the snapshot_1 directory. In order to update the sequence in the gen database, we must represent the changes that were made in a format gen understands. We convert the genbank file to fasta as shown above, and then perform a pairwise alignment using wfmash from the PanGenome Graph Builder (pggb). First we concatenate the old and new sequence, renaming them to ref and alt, respectively. The resulting file is then indexed using samtools and used as input to wfmash. The obtained alignment (aln.paf) is then used to build a graph via seqwish. By specifying a --min-match-len of 12, we control the complexity of the graph topology built from the alignment.

```console
(sed 's/^>.*/>ref/' snapshot_1/pBeloBAC11.fa; sed 's/^>.*/>alt/' pBeloBAC11.fa) > combined.fa
samtools faidx combined.fa
wfmash combined.fa > aln.paf
seqwish --seqs combined.fa --paf-alns aln.paf --gfa out.gfa --min-match-len=12
```

In this specific example ommitting the min-match-len filter results in a graph that contains a node of only 1 bp long because of a local alignment. This is born out by the CIGAR string in the alignment file: `cg:Z:766=790I1=655D6085=`

The path lines in the GFA file indicate that this is a replacement, which we then translate to a gen update command.
```
P       ref     1+,2+,3+        *
P       alt     1+,4+,3+        *
```

TODO: test this out with minimap2 as aligner, and with >2 protein engineering sequences as a way to get the multiple sequence alignment import working too.


```console
sed 's/^>.*/>alt/' pBeloBAC11.fa > ref.fa
sed 's/^>.*/>alt/' pBeloBAC11.fa > alt.fa
minimap2 -x asm5 -c -o aln.paf alt.fa ref.fa
cat ref.fa alt.fa > combined.fa
seqwish --seqs combined.fa --paf-alns aln.paf --gfa out.gfa --min-match-len=12
```