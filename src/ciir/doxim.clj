(ns ciir.doxim
  (:require [clojure.string :as s]
            [clojure.java.shell :as sh]
            [clojure.java.io :as jio])
  (:use [clojure.math.combinatorics])
  (:import (org.lemurproject.galago.core.index IndexPartReader KeyIterator ValueIterator)
           (org.lemurproject.galago.core.index.disk
            DiskIndex CountIndexReader$TermCountIterator WindowIndexReader$TermExtentIterator)
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
          (let [docs (nth rec 2)]
            (for [b docs a docs
                  :while (< (a 0) (b 0))
                  :when (not= (get bins (a 0)) (get bins (b 0)))]
              {[(a 0) (b 0)] [k total-freq (rest a) (rest b)]})))))))

(defprotocol LocalValueIterator
  (value-iterator-seq [this]))

(extend-type WindowIndexReader$TermExtentIterator
  LocalValueIterator
  (value-iterator-seq [this]
    (lazy-seq
     (when-not (.isDone this)
       (let [k (.currentCandidate this)
             v (.count this)
             ext (.extents this)
             ;; Realize pos now to capture iterator side effects
             pos (vec (map #(.begin ext %) (range (.size ext))))]
         (.next this)
         (cons [k v pos] (value-iterator-seq this)))))))

(extend-type CountIndexReader$TermCountIterator
  LocalValueIterator
  (value-iterator-seq [this]
    (lazy-seq
     (when-not (.isDone this)
       (let [k (.currentCandidate this)
             v (.count this)]
         (.next this)
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
  [index-file series-map-file file-prefix file-suffix max-series step stride]
  (let [ireader (DiskIndex/openIndexPart index-file)
        ki (.getIterator ireader)
        series (read-series-map series-map-file)
        upper (/ (* max-series (dec max-series)) 2)]
    (dorun (repeatedly (* step stride) (fn [] (.nextKey ki))))
    (println "#" step stride (.getKeyString ki))
    (with-open [out (jio/writer (str file-prefix step file-suffix))]
      (binding [*out* out]
        (doseq [item (->> ki dump-kl-index (take stride) (match-pairs series upper))]
          (prn item))))))

(defn- read-match-data
  [s]
  (let [[ids data] (read-string (str "[" s "]"))
        dmap
        (->> data
             (partition 4)
             (map #(vector (first %) (rest %)))
             (into {}))]
    [ids dmap]))

(defn- sort-matches
  [matches side]
  (sort
   (mapcat #(for [position (second (nth (second %) (inc side)))]
              (vector position (first %)))
           matches)))

(defn- bridge-gap
  [max-gap [prev cur]]
  (let [[prev-pos prev-string] prev
        [cur-pos cur-string] cur
        w (s/split cur-string #"~")
        diff (- cur-pos prev-pos)]
    (cond
     (< diff 5) (drop (- 5 diff) w)
     (< diff (+ max-gap 5)) (concat (repeat (- diff 5) ".") w)
     :else (concat ["###"] w))))

(defn- merge-matches
  [matches max-gap]
  (concat
   (s/split (second (first matches)) #"~")
   (->> matches
        (partition 2 1)
        (mapcat (partial bridge-gap max-gap)))))

;; (partition-by (partial = "###"))
;;      (map (partial s/join " "))))

(defn- trailing-date
  [s]
  (second (re-find #"/([0-9]{8})[0-9][0-9]$" s)))

(defn score-pair
  [s namei smeta]
  (let [[[id1 id2] matches] (read-match-data s)
        [s1 u1] (s/split (doc-name namei id1) #"_" 2)
        [s2 u2] (s/split (doc-name namei id2) #"_" 2)
        text1 (merge-matches (sort-matches matches 0) 10)
        text2 (merge-matches (sort-matches matches 1) 10)
        nseries (count smeta)
        score (reduce +
                      (map #(Math/log %)
                           (map (partial / nseries) (map first (vals matches)))))]
    (s/join "\t" [score
                  (trailing-date u1)
                  (smeta s1)
                  (str "http://" u1)
                  (trailing-date u2)
                  (smeta s2)
                  (str "http://" u2)
                  id1 id2 s1 s2
                  (s/join " " text1)
                  (s/join " " text2)])))

(defn load-series-meta
  [fname]
  (->> fname jio/reader line-seq
       (map #(let [fields (s/split % #"\t")]
               [(nth fields 3) (nth fields 2)]))
       (into {})))

(defn dump-scores
  [namef seriesf]
  (let [namei (DiskIndex/openIndexPart namef)
        smeta (load-series-meta seriesf)]
    (doseq [line (line-seq (-> System/in java.io.InputStreamReader. java.io.BufferedReader.))]
      (println (score-pair line namei smeta)))))

(defn -main
  "I don't do a whole lot."
  [& args]
  (condp = (first args)
    "scores" (dump-scores (second args) (nth args 2))
    "pairs" (dump-pairs (second args) (nth args 2) (nth args 3) (nth args 4)
                        (Integer/parseInt (nth args 5)) (Integer/parseInt (nth args 6))
                        (Integer/parseInt (nth args 7)))
    "counts" (->> (DiskIndex/openIndexPart (second args)) dump-index (map second) frequencies prn)
    "entries"  (->> (DiskIndex/openIndexPart (second args)) dump-index count prn)
    "total"  (->> (DiskIndex/openIndexPart (second args)) dump-index (rand-blat first 0.001) (map second) (reduce +) prn)
    "dump" (doseq
               [s (->> (DiskIndex/openIndexPart (second args)) dump-index)]
             (println s))
    "easy-dump" (kv-dump (DiskIndex/openIndexPart (second args)))
    (println "Unexpected command:" (first args))))
