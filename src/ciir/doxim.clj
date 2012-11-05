(ns ciir.doxim
  (:require [clojure.string :as s]
            [clojure.set :as set]
            [clojure.java.shell :as sh]
            [clojure.java.io :as jio])
  (:use [clojure.math.combinatorics])
  (:import (org.lemurproject.galago.core.index IndexPartReader KeyIterator ValueIterator)
           (org.lemurproject.galago.core.index.disk
            DiskIndex CountIndexReader$TermCountIterator WindowIndexReader$TermExtentIterator)
           (org.lemurproject.galago.core.parse Document)
           (org.lemurproject.galago.core.retrieval Retrieval RetrievalFactory)
           (org.lemurproject.galago.tupleflow Parameters Utility))
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

(defn- unique-matches
  "Remove repeated ngrams"
  [matches]
  (into {} (filter (fn [[k v]] (= 1 (first (second v)) (first (nth v 2)))) matches)))

(defn- find-match-anchors
  [matches]
  (let [res (->> matches
                 unique-matches
                 vals
                 (map rest)
                 (map #(mapv (comp first second) %))
                 sort
                 vec)]
    (when (not-empty res) res)))

(defn largest-binary-search
  [pred low high default]
  (if (< high low)
    default
    (let [mid (+ low (int (/ (- high low) 2)))]
      (cond (= mid low) (cond (pred high) high
                              (pred low) low
                              :else default)
            (pred mid) (recur pred mid high default)
            :else (recur pred low (dec mid) default)))))

(defn longest-increasing-subsequence
  "Output indices of the LIS"
  [coll]
  (when-let [s (vec coll)]
    (let [n (count s)]
      (loop [L 0
             i 1
             M (vec (take (inc n) (repeat 0)))
             P (vec (take (inc n) (repeat 0)))]
        (if (<= i n)
          (let [cur (s (dec i))
                j (largest-binary-search (fn [x] (< (s (dec (M (dec x)))) cur)) 1 L 0)
                newP (if (> j 0) (assoc P (dec i) (M (dec j))) P)]
            (if (or (= j L) (< cur (s (dec (M j)))))
              (recur (long (max L (inc j)))
                     (inc i)
                     (assoc M j i)
                     newP)
              (recur L
                     (inc i)
                     M
                     newP)))
          ;; Traceback
          (loop [res (list (dec (M (dec L))))]
            (let [prev (P (first res))]
              (if (> prev 0)
                (recur (cons (dec prev) res))
                (seq res)))))))))

(def match-matrix (jaligner.matrix.MatrixGenerator/generate 2 -1))

(defn- doc-words
  [^Retrieval ri ^String dname ]
  (vec (.terms (.getDocument ri dname (Parameters.)))))

(defn- align-words
  [start end w1 w2 gap-words]
  (let [[s1 s2] (if (not-empty start)
                  start
                  [(max 0 (- (first end) gap-words))
                   (max 0 (- (second end) gap-words))])
        [e1 e2] (if (not-empty end)
                  end
                  [(min (dec (count w1)) (+ (first start) gap-words))
                   (min (dec (count w2)) (+ (second start) gap-words))])
        s1 (s/replace (s/join " " (subvec w1 s1 (inc e1))) #"[^a-zA-Z0-9 ]" "#")
        s2 (s/replace (s/join " " (subvec w2 s2 (inc e2))) #"[^a-zA-Z0-9 ]" "#")
        alg (jaligner.SmithWatermanGotoh/align
             (jaligner.Sequence. s1) (jaligner.Sequence. s2) match-matrix 5 0.5)
        out1 (String. (.getSequence1 alg))
        out2 (String. (.getSequence2 alg))
        os1 (.getStart1 alg)
        os2 (.getStart2 alg)]
    ;; (prn [os1 os2 out1 out2])
    (when (and (or (empty? start) (= 0 os1 os2))
               ;; Probably should count alignment without hyphen gaps
               (or (empty? end) (and (<= (count s1) (+ os1 (count out1)))
                                     (<= (count s2) (+ os2 (count out2))))))
      [out1 out2])))

(defn best-passages
  [id1 id2 matches ^Retrieval ri]
  (let [name1 (.getDocumentName ri id1)
        name2 (.getDocumentName ri id2)
        w1 (doc-words ri name1)
        w2 (doc-words ri name2)
        anch (or (find-match-anchors matches)
                 (->> matches vals first rest (map second) (map first) vec vector))
        lis (longest-increasing-subsequence (map second anch))
        inc-anch (mapv #(get anch %) lis)
        gap-words 100
        gram 5
        add-gram (partial + (dec gram))
        trim-gram (fn [x] (s/replace x #"( [^ ]+){5}$" ""))
        middles (mapcat
                 (fn [[[s1 s2] [e1 e2]]]
                   (if (> (max (- e1 s1) (- e2 s2)) 1)
                     (if (and (> (- e1 s1) gap-words) (> (- e2 s2) gap-words))
                       (list (align-words [s1 s2] [] w1 w2 gap-words)
                             nil
                             ;; shorten gap-words?
                             (align-words [] [e1 e2] w1 w2 gap-words))
                       (if-let [gap
                                (align-words [s1 s2] [(add-gram e1) (add-gram e2)]
                                             w1 w2 gap-words)]
                         ;; Remove tacked-on trailing words
                         (list (mapv trim-gram gap))
                         (list [(s/join " " (subvec w1 s1 (+ s1 gram)))
                                (s/join " " (subvec w2 s2 (+ s2 gram)))]
                               nil)))
                     (list [(w1 s1) (w2 s2)])))
                 (partition 2 1 inc-anch))
        ;; removing trailing tacked-on words
        leading (when-let
                    [res (align-words [] (mapv add-gram (first inc-anch)) w1 w2 gap-words)]
                  (list (mapv trim-gram res)))
        trailing (when-let
                     [res (align-words (nth inc-anch (dec (count inc-anch))) [] w1 w2 gap-words)]
                   (list res))]
    ;; (prn inc-anch)
    ;; (prn (concat leading middles trailing))
    (remove nil?
            (for [span (partition-by nil? (concat leading middles trailing))]
              (when (first span)
                [(s/join " " (map first span))
                 (s/join " " (map second span))])))))

(defn maxer
  [f]
  (fn [a b] (< (f a) (f b)) b a))

(defn score-pair
  [^String s ^Retrieval ri smeta]
  (let [[[id1 id2] matches] (read-match-data s)
        name1 (.getDocumentName ri id1)
        name2 (.getDocumentName ri id2)
        [s1 u1] (s/split name1 #"_" 2)
        [s2 u2] (s/split name2 #"_" 2)
        passages (best-passages id1 id2 matches ri)
        pass (if (empty? passages) ["" ""] (reduce (maxer (comp count #(s/replace % "-" "") s/trim first)) passages))
        nseries (count smeta)
        idf (reduce +
                    (map #(Math/log %)
                         (map (partial / nseries) (map first (vals matches)))))]
    (s/join "\t" [(-> pass first s/trim (s/split #" ") count)
                  (trailing-date u1)
                  (smeta s1)
                  (str "http://" u1)
                  (trailing-date u2)
                  (smeta s2)
                  (str "http://" u2)
                  id1 id2 s1 s2
                  (-> pass first s/trim)
                  (-> pass second s/trim)])))

(defn old-score-pair
  [s namei smeta]
  (let [[[id1 id2] matches] (read-match-data s)
        [s1 u1] (s/split (doc-name namei id1) #"_" 2)
        [s2 u2] (s/split (doc-name namei id2) #"_" 2)
        text1 (merge-matches (sort-matches matches 0) 10)
        text2 (merge-matches (sort-matches matches 1) 10)
        spanCounts (map count (partition-by (partial = "###") text1))
        longSpan (/ (reduce max spanCounts) (reduce + spanCounts))
        nseries (count smeta)
        score (*
               longSpan
               (reduce +
                       (map #(Math/log %)
                            (map (partial / nseries) (map first (vals matches))))))]
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
  [^String idx seriesf]
  (let [ri (RetrievalFactory/instance idx (Parameters.))
        smeta (load-series-meta seriesf)]
    (doseq [line (-> System/in java.io.InputStreamReader. java.io.BufferedReader. line-seq)]
      (println (score-pair line ri smeta)))))

(defn- vocab-set
  [s]
  (-> s (s/split #" ") set (disj "." "###")))

(defn jaccard
  [set1 set2]
  (/ (count (set/intersection set1 set2)) (count (set/union set1 set2))))

(defn complete-cluster-matches
  [m thresh voc id]
  (let [members (get-in m [:members id])]
    (= (count members)
       (count
        (filter
         #(> (jaccard voc (:vocabulary %)) thresh)
         members)))))

(defn single-cluster-matches
  [m thresh voc id]
  (let [members (get-in m [:members id])]
    (<= 1
        (count
         (filter
          #(> (jaccard voc (:vocabulary %)) thresh)
          members)))))

(defn format-cluster
  [cluster]
  (let [scores (->> cluster (map :score) set seq)]
    [(->> cluster (map :id) set count)
     ;;(/ (reduce + scores) (count scores))
     (->> cluster (map :id) set (s/join ":"))
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
                           (s/join
                            "\t"
                            [(:date cur) (:url cur) (:title cur)
                             (:id cur) (:text cur)])))
                   (next members)))
          res)))]))

(defn greedy-cluster-reducer
  [match-fn m line]
  (let [[sscore date1 title1 url1 date2 title2 url2 sid1 sid2 series1 series2 raw1 raw2]
        (s/split line #"\t" 13)
        id1 (Integer/parseInt sid1)
        id2 (Integer/parseInt sid2)
        score (Double/parseDouble sscore)
        text1 (s/replace raw1 "-" "")
        text2 (s/replace raw2 "-" "")
        v1 (vocab-set text1)
        v2 (vocab-set text2)
        rec1 {:id id1 :text text1 :vocabulary v1 :date date1 :title title1 :url url1 :score score}
        rec2 {:id id2 :text text2 :vocabulary v2 :date date2 :title title2 :url url2 :score score}
        nextid (inc (get m :top 0))
        clusters1 (get-in m [:clusters id1] #{})
        clusters2 (get-in m [:clusters id2] #{})
        matches (match-fn m clusters1 clusters2 rec1 rec2)
        match (or (first matches) nextid)]
    (assoc
        (if (> (count matches) 1)
          (let [others (rest matches)
                orecs (map (partial get (:members m)) others)
                newrec (merge {id1 rec1 id2 rec2} (reduce merge orecs) (get-in m [:members match]))
                docs (keys newrec)
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
              (assoc-in [:members match] (merge {id1 rec1 id2 rec2} (get-in m [:members match]))
              (assoc-in [:clusters id1] (conj clusters1 match))
              (assoc-in [:clusters id2] (conj clusters2 match)))))
      :top nextid)))

(defn single-link-reducer
  [m line]
  (let [[sscore date1 title1 url1 date2 title2 url2 sid1 sid2 series1 series2 raw1 raw2]
        (s/split line #"\t" 13)
        id1 (Integer/parseInt sid1)
        id2 (Integer/parseInt sid2)
        score (Double/parseDouble sscore)
        text1 (s/replace raw1 "-" "")
        text2 (s/replace raw2 "-" "")
        v1 (vocab-set text1)
        v2 (vocab-set text2)
        rec1 {:id id1 :text text1 :vocabulary v1 :date date1 :title title1 :url url1 :score score}
        rec2 {:id id2 :text text2 :vocabulary v2 :date date2 :title title2 :url url2 :score score}
        nextid (inc (get m :top 0))
        clusters1 (get-in m [:clusters id1] #{})
        clusters2 (get-in m [:clusters id2] #{})
        clusters (set/union clusters1 clusters2)
        matches (when clusters
                  (set/union
                   (set/select (partial single-cluster-matches m 0.6 v1) clusters1)
                   (set/select (partial single-cluster-matches m 0.6 v2) clusters2)))
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

(defn norep-cluster
  [cluster]
  (let [max-rep (->> cluster (map (juxt :id :title)) (into {}) vals frequencies vals (reduce max))]
    (<= max-rep 1)))

(defn cluster-scores
  [lines]
  (doseq
      [cluster
       (->> lines
            (reduce single-link-reducer {})
            :members
            vals
            (filter norep-cluster)
            (map format-cluster))]
    (let [prefix
          (str (second cluster) "\t"
               (first cluster) "\t")]
      (print prefix)
      (println (s/join (str "\n" prefix) (nth cluster 2))))))

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
