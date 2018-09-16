
## Read more details on [this page](https://gurumulay.github.io/datascience/datascience-4/).

**authorAttrVect**: Offline algorithm to find the Author Attribute Vectors for all the authors in the gutenberg corpus.

**cosineSimilarity**: Algorithms to fins cosine similarity metric between a document with unknown authorship and authors from gutenberg corpus (from earlier step). We use **combiners** for this step.

Multiple Mappers and Reducers are chained together (called *job* *chaining*) that produce intermediate results and then final results based on those intermediate results.


## Toy Example
**authorAttrVect_toy_dataset_illustration**: Contains a very small toy dataset fed as input to the system and the outputs at every map reduce step of **authorAttrVect** program. The final output is *mr4aav12.txt* that contains TF-IDF scores in the final column.

**cosineSimilarity_illustration**: Contains a list of top authors that might be the potential authors of a document with unknown authorship provided as a input to the program.
