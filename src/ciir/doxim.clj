(ns ciir.doxim
  (:require [clojure.string :as s]
            [clojure.set :as set]
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
    (doseq [line (-> System/in java.io.InputStreamReader. java.io.BufferedReader. line-seq)]
      (println (score-pair line namei smeta)))))

(defn- vocab-set
  [s]
  (-> s (s/split #" ") set (disj "." "###")))

(defn jaccard
  [set1 set2]
  (/ (count (set/intersection set1 set2)) (count (set/union set1 set2))))

(defn cluster-matches
  [m thresh voc id]
  (let [members (get-in m [:members id])]
    (= (count members)
       (count
        (filter
         #(> (jaccard voc (:vocabulary %)) thresh)
         members)))))

(defn format-cluster
  [cluster]
  (s/join
   "\n"
   (sort 
    (loop [seen {}
           res []
           members cluster]
      (if-let [cur (first members)]
        (let [id (:id cur)]
          (recur (assoc seen id true)
                 (if (seen id)
                   res
                   (conj res
                         (s/join "\t" ((juxt :date :title :id :url :text) cur))))
                 (next members)))
        res)))))

(defn complete-link-reducer
  [m line]
  (let [[score date1 title1 url1 date2 title2 url2 sid1 sid2 series1 series2 text1 text2]
        (s/split line #"\t")
        id1 (Integer/parseInt sid1)
        id2 (Integer/parseInt sid2)
        v1 (vocab-set text1)
        v2 (vocab-set text2)
        rec1 {:id id1 :text text1 :vocabulary v1 :date date1 :title title1 :url url1 :score score}
        rec2 {:id id2 :text text2 :vocabulary v2 :date date2 :title title2 :url url2 :score score}
        nextid (inc (get m :top 0))
        clusters1 (get-in m [:clusters id1] #{})
        clusters2 (get-in m [:clusters id2] #{})
        clusters (set/union clusters1 clusters2)
        matches (when clusters
                  (set/select
                   (partial cluster-matches m 0.8 v1)
                   (set/select (partial cluster-matches m 0.8 v2) clusters)))
        match (or (first matches) nextid)]
    ;; (println nextid)
    ;; Actually, we should merge clusters if there is more than one match
    (assoc
        (if (> (count matches) 1)
          (let [others (rest matches)
                orecs (map (partial get (:members m)) others)
                newrec (set/union (reduce set/union (get-in m [:members match]) orecs)
                                  #{rec1 rec2})
                docs (->> newrec (map :id) set seq)
                newidx
                (into
                 {} (map
                     vector docs
                     (map #(conj (apply disj (get (:clusters m) % #{}) others) match) docs)))]
            ;; (println match "\t" others)
            ;; (println id1 "clusters1:" clusters1)
            ;; (println id2 "clusters2:" clusters2)
            ;; (println "docs:" docs)
            ;; (println "newidx:" newidx)
            ;; Need to dissociate the old cluster numbers from
            ;; *all* documents, not just id1 and id2
            ;; To test, take the first 877
            (-> m
                (assoc-in [:members match] newrec)
                (assoc :clusters (merge (:clusters m) newidx))))
          (-> m
              (assoc-in [:members match] (set/union (get-in m [:members match])
                                                    #{rec1 rec2}))
              (assoc-in [:clusters id1] (conj clusters1 match))
              (assoc-in [:clusters id2] (conj clusters2 match))))
      :top nextid)))

(defn single-link-reducer
  [m line]
  (let [[score date1 title1 url1 date2 title2 url2 sid1 sid2 series1 series2 text1 text2]
        (s/split line #"\t")
        id1 (Integer/parseInt sid1)
        id2 (Integer/parseInt sid2)
        cluster1 (get-in m [:cluster id1])
        cluster2 (get-in m [:cluster id2])]
    (cond
     (and cluster1 cluster2) (reduce
                              #(assoc-in %1 [:cluster %2] cluster1)
                              (assoc-in m [:members cluster1] (set/union (get-in m [:members cluster1])
                                                                         (get-in m [:members cluster2])))
                              (get-in m [:members cluster2]))
     cluster1 (-> m
                  (assoc-in [:members cluster1] (conj (get-in m [:members cluster1]) id2))
                  (assoc-in [:cluster id2] cluster1)
                  (assoc-in [:text id2] text2))
     cluster2 (-> m
                  (assoc-in [:members cluster2] (conj (get-in m [:members cluster2]) id1))
                  (assoc-in [:cluster id1] cluster2)
                  (assoc-in [:text id1] text1))
     :else (-> m
               (assoc-in [:members id1] #{id1 id2})
               (assoc-in [:cluster id1] id1)
               (assoc-in [:cluster id2] id1)
               (assoc-in [:text id1] text1)
               (assoc-in [:text id2] text2)))))

(defn cluster-scores
  [lines]
  (doseq [cluster
          (->> lines
               (reduce complete-link-reducer {})
               :members
               vals)]
    (println (format-cluster cluster))))

     ;; (s/join
     ;;  "\n"
     ;;  (sort (map #(s/join "\t" ((juxt :date :title :score :id :url :text) %)) cluster)))
     ;; "\n")))

(defn -main
  "I don't do a whole lot."
  [& args]
  (condp = (first args)
    "cluster" (cluster-scores
               (-> System/in java.io.InputStreamReader. java.io.BufferedReader. line-seq))
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
