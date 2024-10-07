# Samples

A sample represents a set of changes to a reference genome. Take a simple vcf file with the following line:

```
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	unknown	G1	foo
m123	3	.	CGA	CA	1611.92	.	.	GT:DP:AD	1/1:56:0,53	.	1/1:56:0,53
```

This creates 3 new samples, `unknown`, `G1`, and `foo`. Each one of these samples will have its own copy of the
reference genome, and any changes specific to the sample. In this case, a sample can be thought of as mapping to
3 individuals (unknown, G1, and foo), but can be leveraged to model any set of changes. For example, a sample could
be a pooled sample with many changes, a clade, etc.