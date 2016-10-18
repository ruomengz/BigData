#!/bin/bash
#
# Downloads the wiki dataset and prepares it for clustering
# Put that script in the examples folder of mahout diretory
# To run:  change into the mahout directory and type:
#  examples/bin/cluster-wiki.sh

if [ "$1" = "--help" ] || [ "$1" = "--?" ]; then
  echo "This script clusters the Wikipedia data set using a variety of algorithms.  The data set is downloaded automatically."
  exit
fi

SCRIPT_PATH=${0%/*}
if [ "$0" != "$SCRIPT_PATH" ] && [ "$SCRIPT_PATH" != "" ]; then
  cd $SCRIPT_PATH
fi
START_PATH=`pwd`

# Set commands for dfs
source ${START_PATH}/set-dfs-commands.sh

MAHOUT="../../bin/mahout"

if [ ! -e $MAHOUT ]; then
  echo "Can't find mahout driver in $MAHOUT, cwd `pwd`, exiting.."
  exit 1
fi

if [[ -z "$MAHOUT_WORK_DIR" ]]; then
  WORK_DIR=/tmp/mahout-work-wiki
else
  WORK_DIR=$MAHOUT_WORK_DIR
fi

algorithm=( kmeans fuzzykmeans lda streamingkmeans clean)
if [ -n "$1" ]; then
  choice=$1
else
  echo "Please select a number to choose the corresponding clustering algorithm"
  echo "1. ${algorithm[0]} clustering (runs from this example script in cluster mode only)"
  echo "2. ${algorithm[1]} clustering (may require increased heap space on yarn)"
  echo "3. ${algorithm[2]} clustering"
  echo "4. ${algorithm[3]} clustering"
  echo "5. ${algorithm[4]} -- cleans up the work area in $WORK_DIR"
  read -p "Enter your choice : " choice
fi

echo "ok. You chose $choice and we'll use ${algorithm[$choice-1]} Clustering"
clustertype=${algorithm[$choice-1]}

if [ "x$clustertype" == "xclean" ]; then
  rm -rf $WORK_DIR
  $DFSRM $WORK_DIR
  exit 1
else
  $DFS -mkdir -p $WORK_DIR
  mkdir -p $WORK_DIR
  echo "Creating work directory at ${WORK_DIR}"
fi
echo "creating work directory at ${WORK_DIR}"
mkdir -p ${WORK_DIR}
  if [ ! -e ${WORK_DIR}/wikixml ]; then
    mkdir -p ${WORK_DIR}/wikixml
  fi
  if [ ! -e ${WORK_DIR}/wikixml/enwiki-latest-pages-articles.xml.bz2 ]; then
      echo "Downloading wikipedia XML dump"
        ########################################################
        #  Datasets: uncomment and run "clean" to change dataset
        ########################################################
        ########## partial small 42.5M zipped
      curl https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles1.xml-p000000010p000030302.bz2 -o ${WORK_DIR}/wikixml/enwiki-latest-pages-articles.xml.bz2
        ########## partial larger 256M zipped
        #curl https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles10.xml-p002336425p003046511.bz2 -o ${WORK_DIR}/wikixml/enwiki-latest-pages-articles.xml.bz2
        ######### full wikipedia dump: 10G zipped
        # curl https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2 -o ${WORK_DIR}/wikixml/enwiki-latest-pages-articles.xml.bz2
        ########################################################
  fi
  if [ ! -e ${WORK_DIR}/wikixml/enwiki-latest-pages-articles.xml ]; then
      echo "Extracting..."
      cd ${WORK_DIR}/wikixml && bunzip2 enwiki-latest-pages-articles.xml.bz2 && cd .. && cd ..
  fi
echo $START_PATH

set -e

  set -x
  echo "Preparing wikipedia data"
  rm -rf ${WORK_DIR}/wiki
  mkdir ${WORK_DIR}/wiki

  echo "using United States"
  echo "United States" > ${WORK_DIR}/categories.txt
#============================================
#============================================
if [ "$HADOOP_HOME" != "" ] && [ "$MAHOUT_LOCAL" == "" ] ; then
  echo "Copying wikipedia data to HDFS"
  set +e
  $DFSRM ${WORK_DIR}/wikixml
  $DFS -mkdir -p ${WORK_DIR}
  set -e
  $DFS -put ${WORK_DIR}/wikixml ${WORK_DIR}/wikixml
fi
echo "Creating sequence files from wikiXML"
$MAHOUT_HOME/bin/mahout seqwiki -c ${WORK_DIR}/categories.txt\
                                -i ${WORK_DIR}/wikixml/enwiki-latest-pages-articles.xml \
                                -o ${WORK_DIR}/wikipediainput

if [ "x$clustertype" == "xkmeans" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/wikipediainput/ \
    -o ${WORK_DIR}/wiki-out-seqdir-sparse-kmeans --maxDFPercent 85 --namedVector \
  && \
  $MAHOUT kmeans \
    -i ${WORK_DIR}/wiki-out-seqdir-sparse-kmeans/tfidf-vectors/ \
    -c ${WORK_DIR}/wiki-kmeans-clusters \
    -o ${WORK_DIR}/wiki-kmeans \
    -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure \
    -x 10 -k 20 -ow --clustering \
  && \
  $MAHOUT clusterdump \
    -i `$DFS -ls -d ${WORK_DIR}/wiki-kmeans/clusters-*-final | awk '{print $8}'` \
    -o ${WORK_DIR}/wiki-kmeans/clusterdump \
    -d ${WORK_DIR}/wiki-out-seqdir-sparse-kmeans/dictionary.file-0 \
    -dt sequencefile -b 100 -n 20 --evaluate -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure -sp 0 \
    --pointsDir ${WORK_DIR}/wiki-kmeans/clusteredPoints \
    && \
  cat ${WORK_DIR}/wiki-kmeans/clusterdump
elif [ "x$clustertype" == "xfuzzykmeans" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/wiki-out-seqdir/ \
    -o ${WORK_DIR}/wiki-out-seqdir-sparse-fkmeans --maxDFPercent 85 --namedVector \
  && \
  $MAHOUT fkmeans \
    -i ${WORK_DIR}/wiki-out-seqdir-sparse-fkmeans/tfidf-vectors/ \
    -c ${WORK_DIR}/wiki-fkmeans-clusters \
    -o ${WORK_DIR}/wiki-fkmeans \
    -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure \
    -x 10 -k 20 -ow -m 1.1 \
  && \
  $MAHOUT clusterdump \
    -i ${WORK_DIR}/wiki-fkmeans/clusters-*-final \
    -o ${WORK_DIR}/wiki-fkmeans/clusterdump \
    -d ${WORK_DIR}/wiki-out-seqdir-sparse-fkmeans/dictionary.file-0 \
    -dt sequencefile -b 100 -n 20 -sp 0 \
    && \
  cat ${WORK_DIR}/wiki-fkmeans/clusterdump
elif [ "x$clustertype" == "xlda" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/wiki-out-seqdir/ \
    -o ${WORK_DIR}/wiki-out-seqdir-sparse-lda -ow --maxDFPercent 85 --namedVector \
  && \
  $MAHOUT rowid \
    -i ${WORK_DIR}/wiki-out-seqdir-sparse-lda/tfidf-vectors \
    -o ${WORK_DIR}/wiki-out-matrix \
  && \
  rm -rf ${WORK_DIR}/wiki-lda ${WORK_DIR}/wiki-lda-topics ${WORK_DIR}/wiki-lda-model \
  && \
  $MAHOUT cvb \
    -i ${WORK_DIR}/wiki-out-matrix/matrix \
    -o ${WORK_DIR}/wiki-lda -k 20 -ow -x 20 \
    -dict ${WORK_DIR}/wiki-out-seqdir-sparse-lda/dictionary.file-* \
    -dt ${WORK_DIR}/wiki-lda-topics \
    -mt ${WORK_DIR}/wiki-lda-model \
  && \
  $MAHOUT vectordump \
    -i ${WORK_DIR}/wiki-lda-topics/part-m-00000 \
    -o ${WORK_DIR}/wiki-lda/vectordump \
    -vs 10 -p true \
    -d ${WORK_DIR}/wiki-out-seqdir-sparse-lda/dictionary.file-* \
    -dt sequencefile -sort ${WORK_DIR}/wiki-lda-topics/part-m-00000 \
    && \
  cat ${WORK_DIR}/wiki-lda/vectordump
elif [ "x$clustertype" == "xstreamingkmeans" ]; then
  $MAHOUT seq2sparse \
    -i ${WORK_DIR}/wiki-out-seqdir/ \
    -o ${WORK_DIR}/wiki-out-seqdir-sparse-streamingkmeans -ow --maxDFPercent 85 --namedVector \
  && \
  rm -rf ${WORK_DIR}/wiki-streamingkmeans \
  && \
  $MAHOUT streamingkmeans \
    -i ${WORK_DIR}/wiki-out-seqdir-sparse-streamingkmeans/tfidf-vectors/ \
    --tempDir ${WORK_DIR}/tmp \
    -o ${WORK_DIR}/wiki-streamingkmeans \
    -sc org.apache.mahout.math.neighborhood.FastProjectionSearch \
    -dm org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure \
    -k 10 -km 100 -ow \
  && \
  $MAHOUT qualcluster \
    -i ${WORK_DIR}/wiki-out-seqdir-sparse-streamingkmeans/tfidf-vectors/part-r-00000 \
    -c ${WORK_DIR}/wiki-streamingkmeans/part-r-00000   \
    -o ${WORK_DIR}/wiki-cluster-distance.csv \
    && \
  cat ${WORK_DIR}/wiki-cluster-distance.csv
fi
