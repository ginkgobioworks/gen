# Simple editing workflow with Geneious

This shows a simple back-and-forth editing example using Geneious. Here, sequences are managed in Geneious and exported
as GenBank files to faciliate analyses.

```console
wget "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&rettype=gb&id=U00004.1" -O Tn5.gb
```

We then initialize a new gen repository and import the BAC sequence.
```console
gen init
gen defaults --database my_project.db --collection plasmids
gen import --gb Tn5.gb
```

Then we move over to our sequence editor, where we edit the BAC sequence to replace the chloramphenicol acetyl transferase (cat) coding sequence by the coding sequence of aminoglycoside-3'-O-phosphotransferase from Tn5 (don't forget to reverse complement it before pasting). We then save the resulting sequence, overwriting the original file.

We now can export our new GenBank file from Geneious, Tn5_edited.gb. In order to update the sequence in the gen database, we can use the `update` command.
```console
gen update --gb Tn5_edited.gb
```

This will incorporate changes made in Geneious to the database. The LOCUS tag is used to identify the sequence to use.

Now we are free to export to GFA, etc. for further analysis.

## Appendix

### Mixing sequence creation and updates

To reduce mistakes, if an update is requested and a sequence is in the GenBank file but not in the database, an error will be raised. However, sometimes you may have a mixture of updates and new sequences to add. To facilitate this, use the `--create-missing` (or `--cm`), flag in update:
```console
gen update --gb multiple_locus.gb --create-missing
```