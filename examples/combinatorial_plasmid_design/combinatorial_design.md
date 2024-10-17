# Combinatorial plasmid design

In this example we will design a library of expression plasmids for _E. coli_. We are looking for the combination of promoter and ribosome binding site that result in the highest expression of an insulin precursor peptide.

We start by setting up a new gen repository. To save on typing down the line we will also set a default database and collection.

```console
bvh@mbp:~$ gen init

Gen repository initialized.
```
``` console
bvh@mbp:~$ gen defaults --database insulin.db --collection plasmids

Default database set to insulin.db
Default collection set to plasmids
```

Next, we import our base vector from a fasta file.
``` console
bvh@mbp:~$ gen import --fasta puc19.fa

Created it
```

By importing the sequence we have created one new block group, and one new path. We can verify this by querying the database _(todo: implement this functionality in gen)_.
```console
bvh@mbp:~$ sqlite3 --header insulin.db

SQLite version 3.39.5 2022-10-14 20:58:05
Enter ".help" for usage hints.

sqlite> select * from block_group;
id|collection_name|sample_name|name
1|plasmids||M77789.2
sqlite> select * from path;
id|block_group_id|name
1|1|M77789.2
sqlite> .quit
```

We see that both the path and the block group are called M77789.2, is accession ID for the pUC19 plasmid, which was extracted from the fasta header. Next, we will prepare a _gen update_ operation to insert the insulin operon variants into the vector. We need two files to specify the design: a _parts_ file that contains the sequences of all of the genetic parts that go into the design, and a _library_ file that describes how the parts should be arranged. For the parts file we use

```console
gen update --path-name M77789.2 --start 2 --end 4 --library design.csv ```

docker run -v $PWD:/data -it quay.io/vgteam/vg:v1.60.0


vg convert --gfa-in --gfa-trans FILE   
vg view --gfa-in combo.gfa --dot  --color --simple-dot | dot -Tsvg -o x.svg

