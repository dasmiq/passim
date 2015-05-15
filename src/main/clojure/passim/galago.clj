(ns passim.galago
  (:require [passim.utils :refer [largest-binary-search]])
  (:import (org.lemurproject.galago.core.index IndexPartReader KeyIterator)
           (org.lemurproject.galago.core.index.corpus CorpusReader
                                                      DocumentReader$DocumentIterator)
           (org.lemurproject.galago.core.index.disk DiskIndex)
           (org.lemurproject.galago.core.parse Document Document$DocumentComponents TagTokenizer)
           (org.lemurproject.galago.core.retrieval Retrieval RetrievalFactory)
           (org.lemurproject.galago.core.retrieval.iterator ExtentIterator CountIterator)
           (org.lemurproject.galago.core.retrieval.processing ScoringContext)
           (org.lemurproject.galago.tupleflow Parameters Utility FakeParameters)))

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

(defn ^Document galago-tokenize
  [^String s]
  (let [d (Document. "foo" s)]
    (.tokenize (TagTokenizer.) d)
    d))

(defn ^Document get-index-doc
  [^Retrieval ri ^String dname]
  (let [d
        (.getDocument ri dname (Document$DocumentComponents. true true false))
        tok (TagTokenizer. (FakeParameters. (doto (Parameters.) (.set "fields" ["pb" "w"]))))]
    (.tokenize tok d)
    d))

(defn doc-words
  [^Retrieval ri ^String dname]
  (vec (.terms (get-index-doc ri dname))))

(defn doc-text
  ([^Document d start end]
   (subs (.text d) (.get (.termCharBegin d) start) (.get (.termCharEnd d) (dec end))))
  ([^Retrieval ri dname start end]
   (doc-text (get-index-doc ri dname) start end)))

(defn doc-meta
  ([^Document d]
   (into {} (.metadata d)))
  ([^Retrieval ri dname]
   (doc-meta (get-index-doc ri dname))))

(defn- loc-scale
  [in]
  (int (/ in 4)))

(defn doc-url
  [^Document d start end]
  (let [m (doc-meta d)]
    (when-let [base (m "url")]
      (str
       base
       (when-let [purl (m "pageurl")]
         (let [startChar (.get (.termCharBegin d) start)
               endChar (.get (.termCharEnd d) (dec end))
               tags (.tags d)
               pages (vec (filter #(= "pb" (.name %)) tags))
               startPage (largest-binary-search #(< (.charBegin (pages %)) startChar) 0 (dec (count pages)) 0)]
           ;; (println startChar endChar pages)
           ;; (println (get (.attributes (pages startPage)) "n"))
           (str
            (format purl (get (.attributes (pages startPage)) "n"))
            (when-let [iurl (m "imgurl")]
              (let [span (->> tags
                              (drop-while #(< (.charEnd %) (dec startChar)))
                              (take-while #(< (.charBegin %) endChar))
                              (mapcat
                               (fn [t]
                                 (when (and (= "w" (.name t))
                                            (re-matches #"^\d+,\d+,\d+,\d+"
                                                        (get (.attributes t) "coords")))
                                   [(mapv #(Integer/parseInt %)
                                         (clojure.string/split
                                          (get (.attributes t) "coords") #","))]))))
                    qwe (when (<= (count span) 0) (println "#" start end base purl iurl))
                    x1 (->> span (map first) (reduce min Integer/MAX_VALUE) loc-scale)
                    y1 (->> span (map second) (reduce min Integer/MAX_VALUE) loc-scale)
                    x2 (->> span (map (fn [[x y w h]] (+ x w))) (reduce max 0) loc-scale)
                    y2 (->> span (map (fn [[x y w h]] (+ y h))) (reduce max 0) loc-scale)]

                (format (clojure.string/replace iurl #"print/image%" "print/image_%") 600 600 x1 y1 x2 y2)
                )))))))))
