# Simple editing workflow with Geneious

This shows a simple back-and-forth editing example using Geneious. Here, sequences are managed in Geneious and exported
as GenBank files to facilitate analyses.

Our task here is to take a bacterial transposon (Tn5)

```console
wget "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&rettype=gb&id=U00004.1" -O Tn5.gb
```

We then initialize a new gen repository and import the BAC sequence.
```console
gen init
gen defaults --database my_project.db --collection plasmids
gen import --gb Tn5.gb
```

Then we move over to our sequence editor, where we can carry out any edits and save our genbank file again (overwriting or creating a new file).
This updated file can be passed to the `update` command to incorporate the new changes.

```console
gen update --gb Tn5_edited.gb
```

This will incorporate changes made in Geneious to the database. The LOCUS tag is used to identify the sequence to update.

Now we are free to export to GFA, etc. for further analysis.

## Appendix

### Mixing sequence creation and updates

To reduce mistakes, if an update is requested and a sequence is in the GenBank file but not in the database, an error will be raised. However, sometimes you may have a mixture of updates and new sequences to add. To facilitate this, use the `--create-missing` (or `--cm`), flag in update:
```console
gen update --gb multiple_locus.gb --create-missing
```