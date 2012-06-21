(ns ciir.doxim
  (:require [clojure.string :as s]
            [clojure.java.shell :as sh]
            [clojure.java.io :as jio])
  (:use [clojure.math.combinatorics])
  (:import (org.lemurproject.galago.core.index IndexPartReader KeyIterator ValueIterator)
           (org.lemurproject.galago.core.index.disk DiskIndex CountIndexReader$TermCountIterator)
           (org.lemurproject.galago.tupleflow Utility))
  (:gen-class))

(set! *warn-on-reflection* true)

(defn doc-series
  [docid]
  (first (s/split docid #"_" 2)))

(defn doc-series-pair
  [[no id]]
  [(Integer/parseInt no) (doc-series id)])

(defn read-series-map
  [fname]
  (let [asize (-> (sh/sh "tail" "-1" fname) :out (s/split #"\t") first Integer/parseInt inc)
        res (make-array Integer/TYPE asize)]
    (with-open [in (jio/reader fname)]
      (doseq [line (line-seq in)]
        (let [[id series] (s/split line #"\t")]
          (aset-int res (Integer/parseInt id) (Integer/parseInt series)))))
    res))

;; Sort by series, then multiply group sizes.
(defn cross-counts
  [bins rec]
  (->> (nth rec 2)
       (map first)
       (map #(get bins %))
       frequencies
       vals
       (#(combinations % 2))
       (map (partial apply *))
       (reduce +)))

(defn cross-pairs
  [bins upper rec]
  (let [total-freq (second rec)]
    (when (<= total-freq upper)
      (let [k (first rec)
            npairs (cross-counts bins rec)]
        (when (<= npairs upper)
          (let [docs (map first (nth rec 2))]
            (for [b docs a docs :while (< a b) :when (not= (get bins a) (get bins b))]
              {[a b] [k total-freq]})))))))

(defn value-iterator-seq
  [^CountIndexReader$TermCountIterator vi]
  (lazy-seq
   (when-not (.isDone vi)
     (let [k (.currentCandidate vi)
           v (.count vi)]
       (.next vi)
       (cons [k v] (value-iterator-seq vi))))))

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
  [^IndexPartReader ireader]
  (let [ki ^KeyIterator (.getIterator ireader)]
    (condp #(isa? %2 %1) (class ireader)
      org.lemurproject.galago.core.index.KeyListReader (dump-kl-index ki)
      org.lemurproject.galago.core.index.KeyValueReader (dump-kv-index ki))))

(defn kv-dump
  [^IndexPartReader ireader]
  (let [ki ^KeyIterator (.getIterator ireader)]
    (while (not (.isDone ki))
      (println (.getKeyString ki) (.getValueString ki))
      (.nextKey ki))))

(defn rand-blat
  [f prop coll]
  (filter
   #(do
      (when (<= (rand) prop)
        (binding [*out* *err*]
          (println (f %))))
      true)
   coll))

(defn match-pairs
  [bins upper recs]
  (->> recs
       (filter #(re-find #"^[a-z~]+$" (first %)))
       (mapcat (partial cross-pairs bins upper))))

;; (reduce (partial merge-with #(conj %1 (first %2))) {})))

(defn batch-blat
  [f pref size coll]
  (count
   (map-indexed
    #(with-open [out (jio/writer (str pref %1))]
       (binding [*out* out]
         (doseq [item (f %2)]
           (prn item))))
    (partition size coll))))

(defn doc-name
  [^IndexPartReader ireader id]
  (let [iter (.getIterator ireader)]
    (.skipToKey iter (Utility/fromInt id))
    (.getValueString iter)))

(defn dump-pairs
  [index-file series-map-file file-prefix max-series step stride]
  (let [ireader (DiskIndex/openIndexPart index-file)
        ki (.getIterator ireader)
        series (read-series-map series-map-file)
        upper (/ (* max-series (dec max-series)) 2)]
    (dorun (repeatedly (* step stride) (fn [] (.nextKey ki))))
    (println "#" step stride (.getKeyString ki))
    (with-open [out (jio/writer (str file-prefix step))]
      (binding [*out* out]
        (doseq [item (->> ki dump-kl-index (take stride) (match-pairs series upper))]
          (prn item))))))

(defn -main
  "I don't do a whole lot."
  [& args]
  (condp = (first args)
    "pairs" (dump-pairs (second args) (nth args 2) (nth args 3)
                        (Integer/parseInt (nth args 4)) (Integer/parseInt (nth args 5))
                        (Integer/parseInt (nth args 6)))
    "counts" (->> (DiskIndex/openIndexPart (second args)) dump-index (map second) frequencies prn)
    "entries"  (->> (DiskIndex/openIndexPart (second args)) dump-index count prn)
    "total"  (->> (DiskIndex/openIndexPart (second args)) dump-index (rand-blat first 0.001) (map second) (reduce +) prn)
    "dump" (doseq
               [s (->> (DiskIndex/openIndexPart (second args)) dump-index)]
             (println s))
    "easy-dump" (kv-dump (DiskIndex/openIndexPart (second args)))
    (println "Unexpected command:" (first args))))
