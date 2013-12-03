mahout-sequence-file-groups
===========================

Takes a bunch of sequence files that are in the mahout classification format (groupId/unique-id) and creates another bunch of sequence files with each file having items from only 1 group id

This code will primarily be used by the classification utilities in mahout. After doing a seq2sparse, we get the vectors in a sequence file format.
However, the keys in these sequence files are random i.e the special meaning that mahout assigns the keys i.e (category-id/unique-item-id) is not considered.

We would like to use the --testSplitPct option of the mahout `split` utility to split the data into training and test data

But the problem is that to use this option, the input directory needs to have 1 sequence file per category.


So, in this code we're simply going to pass through all the sequence files in the input directory and create output sequence files that have only 1 group in them
