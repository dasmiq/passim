(ns passim.galago
  (:import (org.lemurproject.galago.core.index IndexPartReader KeyIterator)
           (org.lemurproject.galago.core.index.corpus CorpusReader
                                                      DocumentReader$DocumentIterator)
           (org.lemurproject.galago.core.index.disk DiskIndex)
           (org.lemurproject.galago.core.parse Document Document$DocumentComponents TagTokenizer)
           (org.lemurproject.galago.core.retrieval Retrieval RetrievalFactory)
           (org.lemurproject.galago.core.retrieval.iterator ExtentIterator CountIterator)
           (org.lemurproject.galago.core.retrieval.processing ScoringContext)
           (org.lemurproject.galago.tupleflow Parameters Utility)))

(defprotocol LocalValueIterator
  (value-iterator-seq [this]))

(extend-type ExtentIterator
  LocalValueIterator
  (value-iterator-seq [this]
    (lazy-seq
     (when-not (.isDone this)
       (let [k (.currentCandidate this)]
         (let [ext (.extents this (ScoringContext. k))
               v (.size ext)
               ;; Realize pos now to capture iterator side effects
               pos (mapv #(.begin ext %) (range v))]
           (.movePast this k)
           (cons [k v pos] (value-iterator-seq this))))))))

(extend-type CountIterator
  LocalValueIterator
  (value-iterator-seq [this]
    (lazy-seq
     (when-not (.isDone this)
       (let [k (.currentCandidate this)
             v (.count this (ScoringContext. k))]
         (.movePast this k)
         (cons [k v] (value-iterator-seq this)))))))

(defn dump-kl-index
  [^KeyIterator iter]
  (lazy-seq
   (when-not (.isDone iter)
     (cons
      (let [key (.getKeyString iter)
            vi (.getValueIterator iter)
            vcount (.totalEntries vi)
            val (value-iterator-seq vi)]
        (.nextKey iter)
        [key vcount val])
      (dump-kl-index iter)))))

(defn- dump-kv-index
  [^KeyIterator iter]
  (lazy-seq
   (when-not (.isDone iter)
     (cons
      (let [key (.getKeyString iter)
            val (.getValueString iter)]
        (.nextKey iter)
        [key val])
      (dump-kv-index iter)))))

(defn dump-index
  [index-file]
  (let [ireader ^IndexPartReader (DiskIndex/openIndexPart index-file)
        ki ^KeyIterator (.getIterator ireader)]
    (condp #(isa? %2 %1) (class ireader)
      org.lemurproject.galago.core.index.KeyListReader (dump-kl-index ki)
      org.lemurproject.galago.core.index.KeyValueReader (dump-kv-index ki))))

(defn kv-dump
  [^IndexPartReader ireader]
  (let [ki ^KeyIterator (.getIterator ireader)]
    (while (not (.isDone ki))
      (println (.getKeyString ki) (.getValueString ki))
      (.nextKey ki))))

(defn dump-corpus
  [^String corpus-file]
  (let [di (.getIterator (CorpusReader. corpus-file))
        params (Parameters.)]
    (letfn [(doc-stream [^DocumentReader$DocumentIterator iter]
              (lazy-seq
               (when-not (.isDone iter)
                 (cons
                  [(Utility/toInt (.getKey iter))
                   (vec (.terms (.getDocument iter params)))]
                  (do
                    (.nextKey iter)
                    (doc-stream iter))))))]
      (doc-stream di))))

(defn galago-tokenize
  [^String s]
  (let [d (Document. "foo" s)]
    (.tokenize (TagTokenizer.) d)
    (.terms d)))

(defn ^Document get-index-doc
  [^Retrieval ri ^String dname]
  (.getDocument ri dname (Document$DocumentComponents. true true true)))

(defn doc-words
  [^Retrieval ri ^String dname]
  (vec (.terms (get-index-doc ri dname))))

(defn doc-text
  [ri dname start end]
  (let [d (get-index-doc ri dname)]
    (subs (.text d) (.get (.termCharBegin d) start) (.get (.termCharEnd d) (dec end)))))

(defn doc-meta
  [ri dname]
  (into {} (.metadata (get-index-doc ri dname))))
