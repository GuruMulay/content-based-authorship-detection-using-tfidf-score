
Read more details on [this page](https://gurumulay.github.io/datascience/datascience-4/).

**authorAttrVect**: Offline algorithm to find the Author Attribute Vectors for all the authors in the gutenberg corpus.

**cosineSimilarity**: Algorithms to fins cosine similarity metric between a document with unknown authorship and authors from gutenberg corpus (from earlier step). We use **combiners** for this step.

Multiple Mappers and Reducers are chained together (called *job* *chaining*) that produce intermediate results and then final results based on those intermediate results.
