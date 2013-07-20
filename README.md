Mathematical Language Processing
================================

# Run
* compile the maven project
* adapt the paths to your stratosphere environment in the file `cluster-run.sh`
* setup the right values for the parameters of the ranking algorithm also in `cluster-run.sh`
* execute the script


## Notice
To start the processor, an additional model file is needed. Download the Stanford POS tagger from http://nlp.stanford.edu/software/tagger.shtml. Within this archive is a directory called `pos-tagger-models/`, containing a variaty of model files for a couple of languages.

If uncertain, the `english-left3words-distsim.tagger` model is a good starting point.

Tested with http://nlp.stanford.edu/software/stanford-postagger-2012-11-11.zip ... the most recent version seems not to work.
